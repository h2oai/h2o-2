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
source ./runner_setup.sh "$@"

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
myR testdir_autoGen/runit_complexFilterTest_2_100kx7_logreg_137 45
myR testdir_autoGen/runit_complexFilterTest_30k_categoricals_173 45
myR testdir_autoGen/runit_complexFilterTest_AllBedroomsent_Neighborhoods_181 45
myR testdir_autoGen/runit_complexFilterTest_Goalies_157 45
myR testdir_autoGen/runit_complexFilterTest_HTWO_87_one_line_dataset_1dos_160 45
myR testdir_autoGen/runit_complexFilterTest_HTWO_87_one_line_dataset_2dos_198 45
myR testdir_autoGen/runit_complexFilterTest_USArrests_172 45
myR testdir_autoGen/runit_complexFilterTest_badchars_190 45
myR testdir_autoGen/runit_complexFilterTest_baddata_185 45
myR testdir_autoGen/runit_complexFilterTest_benign_152 45
myR testdir_autoGen/runit_complexFilterTest_cars_182 45
myR testdir_autoGen/runit_complexFilterTest_cars_196 45
myR testdir_autoGen/runit_complexFilterTest_chdage_166 45
myR testdir_autoGen/runit_complexFilterTest_clslowbwt_146 45
myR testdir_autoGen/runit_complexFilterTest_coldom_test2_184 45
myR testdir_autoGen/runit_complexFilterTest_constantColumn_134 45
myR testdir_autoGen/runit_complexFilterTest_copen_162 45
myR testdir_autoGen/runit_complexFilterTest_dd_mon_yr_158 45
myR testdir_autoGen/runit_complexFilterTest_eightsq_170 45
myR testdir_autoGen/runit_complexFilterTest_fail2_24_100000_10_140 45
myR testdir_autoGen/runit_complexFilterTest_fail2_24_100000_10_177 45
myR testdir_autoGen/runit_complexFilterTest_failtoconverge_1000x501_161 45
myR testdir_autoGen/runit_complexFilterTest_failtoconverge_100x50_180 45
myR testdir_autoGen/runit_complexFilterTest_hex_443_136 45
myR testdir_autoGen/runit_complexFilterTest_hhp_9_17_12_167 45
myR testdir_autoGen/runit_complexFilterTest_iris22_151 45
myR testdir_autoGen/runit_complexFilterTest_iris22_195 45
myR testdir_autoGen/runit_complexFilterTest_iris_150 45
myR testdir_autoGen/runit_complexFilterTest_iris_test_numeric_142 45
myR testdir_autoGen/runit_complexFilterTest_iris_test_numeric_203 45
myR testdir_autoGen/runit_complexFilterTest_iris_test_numeric_extra2_168 45
myR testdir_autoGen/runit_complexFilterTest_iris_test_numeric_extra2_176 45
myR testdir_autoGen/runit_complexFilterTest_iris_train_141 45
myR testdir_autoGen/runit_complexFilterTest_is_NA_149 45
myR testdir_autoGen/runit_complexFilterTest_lowbwt_208 45
myR testdir_autoGen/runit_complexFilterTest_make_me_converge_10000x5_163 45
myR testdir_autoGen/runit_complexFilterTest_make_me_converge_10000x5_171 45
myR testdir_autoGen/runit_complexFilterTest_mixed_causes_NA_178 45
myR testdir_autoGen/runit_complexFilterTest_parse_fail_double_space_204 45
myR testdir_autoGen/runit_complexFilterTest_pbc_147 45
myR testdir_autoGen/runit_complexFilterTest_poker100_202 45
myR testdir_autoGen/runit_complexFilterTest_prostate_8_154 45
myR testdir_autoGen/runit_complexFilterTest_prostate_test_187 45
myR testdir_autoGen/runit_complexFilterTest_prostate_train_135 45
myR testdir_autoGen/runit_complexFilterTest_prostate_train_188 45
myR testdir_autoGen/runit_complexFilterTest_space_shuttle_damage_175 45
myR testdir_autoGen/runit_complexFilterTest_stego_training_modified_133 45
myR testdir_autoGen/runit_complexFilterTest_syn_2659x1049_148 45
myR testdir_autoGen/runit_complexFilterTest_syn_fp_prostate_197 45
myR testdir_autoGen/runit_complexFilterTest_syn_sphere3_194 45
myR testdir_autoGen/runit_complexFilterTest_test1_153 45
myR testdir_autoGen/runit_complexFilterTest_test_144 45
myR testdir_autoGen/runit_complexFilterTest_test_164 45
myR testdir_autoGen/runit_complexFilterTest_test_165 45
myR testdir_autoGen/runit_complexFilterTest_test_169 45
myR testdir_autoGen/runit_complexFilterTest_test_193 45
myR testdir_autoGen/runit_complexFilterTest_test_199 45
myR testdir_autoGen/runit_complexFilterTest_test_200 45
myR testdir_autoGen/runit_complexFilterTest_test_206 45
myR testdir_autoGen/runit_complexFilterTest_test_26cols_multi_space_sep_143 45
myR testdir_autoGen/runit_complexFilterTest_test_manycol_tree_159 45
myR testdir_autoGen/runit_complexFilterTest_test_manycol_tree_191 45
myR testdir_autoGen/runit_complexFilterTest_test_tree_minmax_156 45
myR testdir_autoGen/runit_complexFilterTest_test_tree_minmax_186 45
myR testdir_autoGen/runit_complexFilterTest_tnc3_10_155 45
myR testdir_autoGen/runit_complexFilterTest_train_138 45
myR testdir_autoGen/runit_complexFilterTest_train_139 45
myR testdir_autoGen/runit_complexFilterTest_train_179 45
myR testdir_autoGen/runit_complexFilterTest_train_189 45
myR testdir_autoGen/runit_complexFilterTest_train_192 45
myR testdir_autoGen/runit_complexFilterTest_train_205 45
myR testdir_autoGen/runit_complexFilterTest_uis_201 45
myR testdir_autoGen/runit_complexFilterTest_umass_chdage_183 45
myR testdir_autoGen/runit_complexFilterTest_zero_dot_zero_one_207 45
myR testdir_autoGen/runit_simpleFilterTest_Benchmark_dojo_test_83 45
myR testdir_autoGen/runit_simpleFilterTest_HTWO_87_one_line_dataset_1unix_93 45
myR testdir_autoGen/runit_simpleFilterTest_datagen1_86 45
myR testdir_autoGen/runit_simpleFilterTest_housing_91 45
myR testdir_autoGen/runit_simpleFilterTest_iris_test_missing_77 45
myR testdir_autoGen/runit_simpleFilterTest_iris_test_numeric_missing_extra_90 45
myR testdir_autoGen/runit_simpleFilterTest_iris_train_78 45
myR testdir_autoGen/runit_simpleFilterTest_iris_wheader_92 45
myR testdir_autoGen/runit_simpleFilterTest_logreg_trisum_int_cat_10000x10_89 45
myR testdir_autoGen/runit_simpleFilterTest_pharynx_85 45
myR testdir_autoGen/runit_simpleFilterTest_prostate_5_79 45
myR testdir_autoGen/runit_simpleFilterTest_syn_sphere2_80 45
myR testdir_autoGen/runit_simpleFilterTest_test_87 45
myR testdir_autoGen/runit_simpleFilterTest_test_94 45
myR testdir_autoGen/runit_simpleFilterTest_test_all_raw_top10rows_84 45
myR testdir_autoGen/runit_simpleFilterTest_train_81 45
myR testdir_autoGen/runit_simpleFilterTest_train_82 45
myR testdir_autoGen/runit_simpleFilterTest_train_88 45
myR testdir_autoGen/runit_simpleFilterTest_wonkysummary_76 45
myR testdir_autoGen/runit_sliceTest_30k_categoricals_2 45
myR testdir_autoGen/runit_sliceTest_AID362red_test_12 45
myR testdir_autoGen/runit_sliceTest_HEX_287_small_files_8 45
myR testdir_autoGen/runit_sliceTest_benign_10 45
myR testdir_autoGen/runit_sliceTest_datagen1_16 45
myR testdir_autoGen/runit_sliceTest_gt69436csv_5 45
myR testdir_autoGen/runit_sliceTest_iris_test_extra_7 45
myR testdir_autoGen/runit_sliceTest_lowbwtm11_4 45
myR testdir_autoGen/runit_sliceTest_orange_small_train_14 45
myR testdir_autoGen/runit_sliceTest_prostate_2_17 45
myR testdir_autoGen/runit_sliceTest_randomdata2_20 45
myR testdir_autoGen/runit_sliceTest_sin_pattern_9 45
myR testdir_autoGen/runit_sliceTest_swiss_1 45
myR testdir_autoGen/runit_sliceTest_syn_2659x1049x2enum_18 45
myR testdir_autoGen/runit_sliceTest_test_13 45
myR testdir_autoGen/runit_sliceTest_test_3 45
myR testdir_autoGen/runit_sliceTest_tnc3_10_11 45
myR testdir_autoGen/runit_sliceTest_train_6 45
myR testdir_autoGen/runit_sliceTest_uis_15 45
myR testdir_autoGen/runit_sliceTest_zipcodes_19 45
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

