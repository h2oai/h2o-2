#/bin/sh

# takes a -n argument to disable the s3 download for faster testing

# Ensure that all your children are truly dead when you yourself are killed.
# http://www.davidpashley.com/articles/writing-robust-shell-scripts/#id2382181
# trap "kill -- -$BASHPID" INT TERM EXIT
# leave out EXIT for now
trap "kill -- -$BASHPID" INT TERM
echo "BASHPID: $BASHPID"
echo "current PID: $$"

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions
set -o nounset   ## set -u : exit the script if you try to use an uninitialised variable
set -o errexit   ## set -e : exit the script if any statement returns a non-true return value

# remove any test*xml or TEST*xml in the current dir
rm -f test.*xml

# This gets the h2o.jar
#source ./runner_setup.sh "$@"

rm -f h2o-nodes.json
if [[ $USER == "jenkins" ]]
then 
    # clean out old ice roots from 0xcust.** (assuming we're going to run as 0xcust..
    # only do this if you're jenksin
    echo "If we use more machines, expand this cleaning list."
    echo "The possibilities should be relatively static over time"
    echo "Could be problems if other threads also using that user on these machines at same time"
    echo "Could make the rm pattern match a "sourcing job", not just 0xcustomer"
    echo "Also: Touch all the 0xcustomer-datasets mnt points, to get autofs to mount them."
    echo "Permission rights extend to the top level now, so only 0xcustomer can automount them"
    echo "okay to ls the top level here...no secret info..do all the machines we might be using"

    for mr in 161 164
    do
        ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.$mr  \
            'echo rm -f -r /home/0xcustomer/ice*; cd /mnt/0xcustomer-datasets'
    done

    # HACK this is really 161 plus 164. this allows us to talk to localhost:54377 accidently (R)
    # python ../four_hour_cloud.py -cj pytest_config-jenkins-161.json &
    # CLOUD_IP=192.168.1.161
    python ../four_hour_cloud.py -cj pytest_config-jenkins.json &
    # make sure this matches what's in the json!
    CLOUD_IP=192.168.1.164
    CLOUD_PORT=54355
else
    if [[ $USER == "kevin" ]]
    then
        python ../four_hour_cloud.py -cj pytest_config-kevin.json &
        # make sure this matches what's in the json!
        CLOUD_IP=127.0.0.1
        CLOUD_PORT=54355
    else
        python ../four_hour_cloud.py &
        # make sure this matches what the four_hour_cloud.py does!
        CLOUD_IP=127.0.0.1
        CLOUD_PORT=54321
    fi
fi 

CLOUD_PID=$!
jobs -l

echo ""
echo "Have to wait until h2o-nodes.json is available from the cloud build. Deleted it above."
echo "spin loop here waiting for it. Since the h2o.jar copy slows each node creation"
echo "it might be 12 secs per node"

while [ ! -f ./h2o-nodes.json ]
do
  sleep 5
done
ls -lt ./h2o-nodes.json

# We now have the h2o-nodes.json, that means we started the jvms
# Shouldn't need to wait for h2o cloud here..
# the test should do the normal cloud-stabilize before it does anything.
# n0.doit uses nosetests so the xml gets created on completion. (n0.doit is a single test thing)

# A little '|| true' hack to make sure we don't fail out if this subtest fails
# test_c1_rel has 1 subtest

# This could be a runner, that loops thru a list of tests.

echo "If it exists, pytest_config-<username>.json in this dir will be used"
echo "i.e. pytest_config-jenkins.json"
echo "Used to run as 0xcust.., with multi-node targets (possibly)"

#******************************************************
mySetup() {
    # we setup .Renviron and delete the old local library if it exists
    # then make the R_LIB_USERS dir
    which R
    R --version
    # don't always remove..other users may have stuff he doesn't want to re-install
    if [[ $USER == "jenkins" ]]
    then 
        echo "Rebuilding ~/.Renviron and ~/.Rprofile for $USER"
        # Set CRAN mirror to a default location
        rm -f ~/.Renviron
        rm -f ~/.Rprofile
        echo "options(repos = \"http://cran.stat.ucla.edu\")" > ~/.Rprofile
        echo "R_LIBS_USER=\"~/.Rlibrary\"" > ~/.Renviron
        rm -f -r ~/.Rlibrary
        mkdir -p ~/.Rlibrary
    fi

    # removing .Rlibrary should have removed h2oWrapper
    # but maybe it was installed in another library (site library)
    # make sure it's removed, so the install installs the new (latest) one
    cat <<!  > /tmp/libPaths.cmd
    .libPaths()
    myPackages = rownames(installed.packages())
    if("h2o" %in% myPackages) {
      remove.packages("h2o")
    }
!

    cmd="R -f /tmp/libPaths.cmd --args $CLOUD_IP:$CLOUD_PORT"
    echo "Running this cmd:"
    echo $cmd
    # everything after -- is positional. grabbed by argparse.REMAINDER
    ./sh2junit.py -name $1 -timeout 30 -- $cmd
}

myR() {
    # these are hardwired in the config json used above for the cloud
    # CLOUD_IP=
    # CLOUD_PORT=
    # get_s3_jar.sh now downloads it. We need to tell anqi's wrapper where to find it.
    # with an environment variable
    if [[ -z $2 ]];
    then
        timeout=30 # default to 30
    else
        timeout=$2
    fi

    which R
    R --version
    H2O_R_HOME=../../R
    H2O_PYTHON_HOME=../../py

    # first test will cause an install
    # this is where we downloaded to. 
    # notice no version number
    # ../../h2o-1.6.0.1/R/h2oWrapper_1.0.tar.gz
    export H2OWrapperDir=../../h2o-downloaded/R
    echo "H2OWrapperDir should be $H2OWrapperDir"
    ls $H2OWrapperDir/h2o*.tar.gz

    # we want $1 used for -name below, to not have .R suffix
    rScript=$H2O_R_HOME/tests/$1.R
    echo $rScript
    echo "Running this cmd:"
    cmd="R -f $rScript --args $CLOUD_IP:$CLOUD_PORT"
    echo $cmd

    # don't fail on errors, since we want to check the logs in case that has more info!
    set +e
    # everything after -- is positional. grabbed by argparse.REMAINDER
    ./sh2junit.py -name $1 -timeout $timeout -- $cmd || true

    # try moving all the logs created by this test in sandbox to a subdir to isolate test failures
    # think of h2o.check_sandbox_for_errors()
    rm -f -r sandbox/$1
    mkdir -p sandbox/$1
    cp -f sandbox/*log sandbox/$1
    # rm -f sandbox/*log
    set -e
}

H2O_R_HOME=../../R
echo "Okay to run h2oWrapper.R every time for now"

#***********************************************************************
# This is the list of tests
#***********************************************************************
mySetup libPaths

# can be slow if it had to reinstall all packages?
export H2OWrapperDir=../../h2o-downloaded/R
echo "Showing the H2OWrapperDir env. variable. Is it .../../h2o-downloaded/R?"
printenv | grep H2OWrapperDir

#autoGen RUnits
myR testdir_autoGen/runit_complexFilterTest_2_100kx7_logreg_140.R 45
myR testdir_autoGen/runit_complexFilterTest_30k_categoricals_178.R 45
myR testdir_autoGen/runit_complexFilterTest_Goalies_167.R 45
myR testdir_autoGen/runit_complexFilterTest_HTWO_87_one_line_dataset_1dos_142.R 45
myR testdir_autoGen/runit_complexFilterTest_HTWO_87_one_line_dataset_1unix_148.R 45
myR testdir_autoGen/runit_complexFilterTest_TwoBedrooms_Rent_Neighborhoods_190.R 45
myR testdir_autoGen/runit_complexFilterTest_USArrests_163.R 45
myR testdir_autoGen/runit_complexFilterTest_arit_153.R 45
myR testdir_autoGen/runit_complexFilterTest_cars_170.R 45
myR testdir_autoGen/runit_complexFilterTest_cgd_183.R 45
myR testdir_autoGen/runit_complexFilterTest_cgd_204.R 45
myR testdir_autoGen/runit_complexFilterTest_chdage_136.R 45
myR testdir_autoGen/runit_complexFilterTest_chess_2x2_500_int_181.R 45
myR testdir_autoGen/runit_complexFilterTest_chess_test_172.R 45
myR testdir_autoGen/runit_complexFilterTest_constantColumn_161.R 45
myR testdir_autoGen/runit_complexFilterTest_copen_179.R 45
myR testdir_autoGen/runit_complexFilterTest_cuse_149.R 45
myR testdir_autoGen/runit_complexFilterTest_datagen1_184.R 45
myR testdir_autoGen/runit_complexFilterTest_dd_mon_yr_143.R 45
myR testdir_autoGen/runit_complexFilterTest_dd_mon_yr_177.R 45
myR testdir_autoGen/runit_complexFilterTest_iris22_198.R 45
myR testdir_autoGen/runit_complexFilterTest_iris2_139.R 45
myR testdir_autoGen/runit_complexFilterTest_iris_test_extra_199.R 45
myR testdir_autoGen/runit_complexFilterTest_iris_test_extra_with_na_160.R 45
myR testdir_autoGen/runit_complexFilterTest_iris_test_missing_extra_197.R 45
myR testdir_autoGen/runit_complexFilterTest_iris_test_numeric_188.R 45
myR testdir_autoGen/runit_complexFilterTest_iris_test_numeric_missing_171.R 45
myR testdir_autoGen/runit_complexFilterTest_iris_test_numeric_missing_extra_182.R 45
myR testdir_autoGen/runit_complexFilterTest_iris_train_165.R 45
myR testdir_autoGen/runit_complexFilterTest_iris_wheader_135.R 45
myR testdir_autoGen/runit_complexFilterTest_leads_169.R 45
myR testdir_autoGen/runit_complexFilterTest_lowbwtm11_138.R 45
myR testdir_autoGen/runit_complexFilterTest_make_me_converge_10000x5_185.R 45
myR testdir_autoGen/runit_complexFilterTest_meexp_168.R 45
myR testdir_autoGen/runit_complexFilterTest_nhanes3_174.R 45
myR testdir_autoGen/runit_complexFilterTest_pbc_210.R 45
myR testdir_autoGen/runit_complexFilterTest_pharynx_192.R 45
myR testdir_autoGen/runit_complexFilterTest_poisson_tst1_166.R 45
myR testdir_autoGen/runit_complexFilterTest_poker_hand_testing_175.R 45
myR testdir_autoGen/runit_complexFilterTest_prostate_1_176.R 45
myR testdir_autoGen/runit_complexFilterTest_prostate_4_147.R 45
myR testdir_autoGen/runit_complexFilterTest_prostate_7_209.R 45
myR testdir_autoGen/runit_complexFilterTest_prostate_8_145.R 45
myR testdir_autoGen/runit_complexFilterTest_prostate_cat_replaced_202.R 45
myR testdir_autoGen/runit_complexFilterTest_prostate_long_164.R 45
myR testdir_autoGen/runit_complexFilterTest_prostate_long_195.R 45
myR testdir_autoGen/runit_complexFilterTest_randomdata3_155.R 45
myR testdir_autoGen/runit_complexFilterTest_stego_testing_152.R 45
myR testdir_autoGen/runit_complexFilterTest_stego_training_151.R 45
myR testdir_autoGen/runit_complexFilterTest_stego_training_162.R 45
myR testdir_autoGen/runit_complexFilterTest_stego_training_modified_156.R 45
myR testdir_autoGen/runit_complexFilterTest_syn_fp_prostate_189.R 45
myR testdir_autoGen/runit_complexFilterTest_syn_sphere3_173.R 45
myR testdir_autoGen/runit_complexFilterTest_test_150.R 45
myR testdir_autoGen/runit_complexFilterTest_test_154.R 45
myR testdir_autoGen/runit_complexFilterTest_test_159.R 45
myR testdir_autoGen/runit_complexFilterTest_test_191.R 45
myR testdir_autoGen/runit_complexFilterTest_test_200.R 45
myR testdir_autoGen/runit_complexFilterTest_test_206.R 45
myR testdir_autoGen/runit_complexFilterTest_test_26cols_single_space_sep_2_205.R 45
myR testdir_autoGen/runit_complexFilterTest_test_all_raw_top10rows_141.R 45
myR testdir_autoGen/runit_complexFilterTest_test_all_raw_top10rows_193.R 45
myR testdir_autoGen/runit_complexFilterTest_test_enum_domain_size_201.R 45
myR testdir_autoGen/runit_complexFilterTest_test_manycol_tree_158.R 45
myR testdir_autoGen/runit_complexFilterTest_test_tree_187.R 45
myR testdir_autoGen/runit_complexFilterTest_tnc3_10_194.R 45
myR testdir_autoGen/runit_complexFilterTest_train_137.R 45
myR testdir_autoGen/runit_complexFilterTest_train_146.R 45
myR testdir_autoGen/runit_complexFilterTest_train_186.R 45
myR testdir_autoGen/runit_complexFilterTest_train_196.R 45
myR testdir_autoGen/runit_complexFilterTest_train_203.R 45
myR testdir_autoGen/runit_complexFilterTest_train_207.R 45
myR testdir_autoGen/runit_complexFilterTest_train_208.R 45
myR testdir_autoGen/runit_complexFilterTest_wonkysummary_180.R 45
myR testdir_autoGen/runit_simpleFilterTest_AirlinesTrain_96.R 45
myR testdir_autoGen/runit_simpleFilterTest_Goalies_90.R 45
myR testdir_autoGen/runit_simpleFilterTest_chdage_84.R 45
myR testdir_autoGen/runit_simpleFilterTest_covtype_89.R 45
myR testdir_autoGen/runit_simpleFilterTest_datagen1_78.R 45
myR testdir_autoGen/runit_simpleFilterTest_iris22_83.R 45
myR testdir_autoGen/runit_simpleFilterTest_nhanes3_80.R 45
myR testdir_autoGen/runit_simpleFilterTest_parity_128_4_100_quad_91.R 45
myR testdir_autoGen/runit_simpleFilterTest_parse_fail_double_space_77.R 45
myR testdir_autoGen/runit_simpleFilterTest_prostate_long_94.R 45
myR testdir_autoGen/runit_simpleFilterTest_syn_2659x1049_79.R 45
myR testdir_autoGen/runit_simpleFilterTest_test_26cols_multi_space_sep_87.R 45
myR testdir_autoGen/runit_simpleFilterTest_test_88.R 45
myR testdir_autoGen/runit_simpleFilterTest_test_all_raw_top10rows_82.R 45
myR testdir_autoGen/runit_simpleFilterTest_test_enum_domain_size_85.R 45
myR testdir_autoGen/runit_simpleFilterTest_tnc6_86.R 45
myR testdir_autoGen/runit_simpleFilterTest_train_92.R 45
myR testdir_autoGen/runit_simpleFilterTest_wine_95.R 45
myR testdir_autoGen/runit_simpleFilterTest_winesPCA_93.R 45
myR testdir_autoGen/runit_sliceTest_2_100kx7_logreg_10.R 45
myR testdir_autoGen/runit_sliceTest_30k_categoricals_9.R 45
myR testdir_autoGen/runit_sliceTest_allyears2k_headers_11.R 45
myR testdir_autoGen/runit_sliceTest_arit_20.R 45
myR testdir_autoGen/runit_sliceTest_chess_2x2_500_int_12.R 45
myR testdir_autoGen/runit_sliceTest_chess_test_7.R 45
myR testdir_autoGen/runit_sliceTest_claim_prediction_train_set_10000_17.R 45
myR testdir_autoGen/runit_sliceTest_clslowbwt_19.R 45
myR testdir_autoGen/runit_sliceTest_coldom_train_2.R 45
myR testdir_autoGen/runit_sliceTest_iris_6.R 45
myR testdir_autoGen/runit_sliceTest_iris_test_numeric_missing_extra_18.R 45
myR testdir_autoGen/runit_sliceTest_pros_5.R 45
myR testdir_autoGen/runit_sliceTest_prostate_6_1.R 45
myR testdir_autoGen/runit_sliceTest_randomdata3_3.R 45
myR testdir_autoGen/runit_sliceTest_test_14.R 45
myR testdir_autoGen/runit_sliceTest_train_13.R 45
myR testdir_autoGen/runit_sliceTest_train_15.R 45
myR testdir_autoGen/runit_sliceTest_train_16.R 45
myR testdir_autoGen/runit_sliceTest_train_4.R 45
myR testdir_autoGen/runit_sliceTest_wonkysummary_8.R 45
myR testdir_single_jvm/runit_GBM_ecology.R 45
myR testdir_single_jvm/runit_GLM.R 45
myR testdir_single_jvm/runit_NN_iris_basic.R 45
myR testdir_single_jvm/runit_PCA.R 45
myR testdir_single_jvm/runit_RF.R 45
myR testdir_single_jvm/runit_glm2.R 45
myR testdir_single_jvm/runit_histograms.R 45
myR testdir_single_jvm/runit_kmeans.R 45
myR testdir_single_jvm/runit_libR_airlines.R 45
myR testdir_single_jvm/runit_libR_prostate.R 45
myR testdir_single_jvm/runit_mnist_manyCols.R 45
myR testdir_single_jvm/runit_removeAll.R 45
myR testdir_single_jvm/runit_sliceColHeadTail_iris.R 45
myR testdir_single_jvm/runit_sliceColSummary_iris.R 45
myR testdir_single_jvm/runit_sliceColTypes_iris.R 45
myR testdir_single_jvm/runit_summary2.R 45
myR testdir_single_jvm/runit_summary2_numeric.R 45
myR testdir_single_jvm/runit_summary_numeric.R 45
myR testdir_single_jvm/runit_tail_numeric.R 45
single="testdir_single_jvm"
myR $single/runit_PCA 35
myR $single/runit_GLM 35
myR $single/runit_kmeans 60
myR $single/runit_tail_numeric 60
myR $single/runit_summary_numeric 60
myR $single/runit_GBM_ecology 1200
myR $single/runit_RF 120
myR $single/runit_libR_prostate 120
myR $single/runit_sliceColHeadTail_iris 60
myR $single/runit_sliceColSummary_iris 60
myR $single/runit_sliceColTypes_iris 60
# this guy was failing? not sure why
myR $single/runit_histograms 1200
# airlines is failing summary. put it last
myR $single/runit_libR_airlines 120
# If this one fals, fail this script so the bash dies 
# We don't want to hang waiting for the cloud to terminate.
# produces xml too!
../testdir_single_jvm/n0.doit shutdown/test_shutdown.py

#***********************************************************************
# End of list of tests
#***********************************************************************

if ps -p $CLOUD_PID > /dev/null
then
    echo "$CLOUD_PID is still running after shutdown. Will kill"
    kill $CLOUD_PID
fi
ps aux | grep four_hour_cloud

jobs -l
echo ""
echo "You can stop this jenkins job now if you want. It's all done"

