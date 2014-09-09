package samples.expert;

import hex.CreateFrame;
import water.*;
import water.api.FrameSplitPage;
import water.fvec.Frame;
import water.fvec.RebalanceDataSet;
import water.util.Log;

import java.util.Random;

/**
 * Loads all datasets from smalldata folder for testing purposes
 */
public class LoadDatasets extends TestUtil {
  private static class MyJob extends Job { @Override public void execImpl() { new LoadDatasets().load(); } }
  public static void main(String[] args) throws Exception {
    Class job = MyJob.class;
    samples.launchers.CloudLocal.launch(job, 1);
//    samples.launchers.CloudProcess.launch(job, 2);
    //samples.launchers.CloudConnect.launch(job, "localhost:54321");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.162", "192.168.1.163", "192.168.1.164");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.163", "192.168.1.164");
    //samples.launchers.CloudRemote.launchEC2(job, 4);
  }

  void load() {
    // load these first
    String[] files = new String[]{
            "smalldata/logreg/prostate.csv",
            "smalldata/covtype/covtype.20k.data",
            "smalldata/covtype/covtype.altered.gz",
            "smalldata/weather.csv",
            "smalldata/iris/iris.csv",
            "smalldata/mnist/train.csv.gz",
            "smalldata/mnist/test.csv.gz",
    };

    //for i in `find smalldata -type f`; do echo \"$i\",; done
    String[] files2 = new String[]{
            "smalldata/1_100kx7_logreg.data.gz",
            "smalldata/2_100kx7_logreg.data.gz",
            "smalldata/Abalone.gz",
            "smalldata/adult.gz",
            "smalldata/AID362red_test.csv.gz",
            "smalldata/airlines/AirlinesTest.csv.zip",
            "smalldata/airlines/AirlinesTrain.csv.zip",
            "smalldata/airlines/airlineUUID.csv",
            "smalldata/airlines/allyears2k.zip",
            "smalldata/airlines/allyears2k_headers.zip",
//              "smalldata/airlines/hiveallyears2k/04c40d7c-33c8-486c-8f08-e24ebb8832ea_000000",
//              "smalldata/airlines/hiveallyears2k/04c40d7c-33c8-486c-8f08-e24ebb8832ed_000000",
//              "smalldata/airlines/hiveallyears2k/04c40d7c-33c8-486c-8f08-e24ebb8832ed_000001",
//              "smalldata/airlines/hiveallyears2k/04c40d7c-33c8-486c-8f08-e24ebb8832ed_000002",
//              "smalldata/airlines/hiveallyears2k/04c40d7c-33c8-486c-8f08-e24ebb8832ed_000005",
            "smalldata/airlines/uuid_airline.csv",
//              "smalldata/allstate/claim_prediction_dict.html",
            "smalldata/allstate/claim_prediction_train_set_10000.csv.gz",
            "smalldata/allstate/claim_prediction_train_set_10000_bool.csv.gz",
            "smalldata/allstate/claim_prediction_train_set_10000_int.csv.gz",
            "smalldata/anomaly/ecg_discord.csv",
            "smalldata/anomaly/ecg_discord_test.csv",
            "smalldata/anomaly/ecg_discord_train.csv",
            "smalldata/anomaly/toy_test.csv",
            "smalldata/anomaly/toy_train.csv",
            "smalldata/arcene/arcene_test.data",
            "smalldata/arcene/arcene_train.data",
            "smalldata/arcene/arcene_train_labels.labels",
            "smalldata/arcene/arcene_valid.data",
            "smalldata/arcene/arcene_valid_labels.labels",
            "smalldata/auto.csv",
            "smalldata/badchars.csv",
            "smalldata/baddata.data",
            "smalldata/bigburn.csv",
            "smalldata/boring.csv",
            "smalldata/BostonHousing.csv",
            "smalldata/cars.csv",
            "smalldata/cars_nice_header.csv",
            "smalldata/categoricals/30k_categoricals.csv.gz",
            "smalldata/categoricals/40k_categoricals.csv.gz",
            "smalldata/categoricals/AllBedrooms_Rent_Neighborhoods.csv.gz",
            "smalldata/categoricals/apartments_rec.csv",
            "smalldata/categoricals/Cleveland_heartDiseaseUCI_test.csv",
            "smalldata/categoricals/Cleveland_heartDiseaseUCI_train.csv",
            "smalldata/categoricals/TwoBedrooms_Rent_Neighborhoods.csv.gz",
            "smalldata/cebbinom.csv",
            "smalldata/cebexpanded.csv",
            "smalldata/cebexpandedREADME.rtf",
            "smalldata/chess/chess_1x2x1000/h2o/test.csv",
            "smalldata/chess/chess_1x2x1000/h2o/train.csv",
            "smalldata/chess/chess_2x1x1000/h2o/test.csv",
            "smalldata/chess/chess_2x1x1000/h2o/train.csv",
            "smalldata/chess/chess_2x2x10/h2o/test.csv",
            "smalldata/chess/chess_2x2x10/h2o/train.csv",
            "smalldata/chess/chess_2x2x10/R/test.csv",
            "smalldata/chess/chess_2x2x10/R/train.csv",
            "smalldata/chess/chess_2x2x10/rf.conf",
            "smalldata/chess/chess_2x2x10/weka/test.csv.arff",
            "smalldata/chess/chess_2x2x10/weka/train.csv.arff",
            "smalldata/chess/chess_2x2x100/h2o/test.csv",
            "smalldata/chess/chess_2x2x100/h2o/train.csv",
            "smalldata/chess/chess_2x2x100/R/test.csv",
            "smalldata/chess/chess_2x2x100/R/train.csv",
            "smalldata/chess/chess_2x2x100/rf.conf",
            "smalldata/chess/chess_2x2x100/weka/test.csv.arff",
            "smalldata/chess/chess_2x2x100/weka/train.csv.arff",
            "smalldata/chess/chess_2x2x1000/h2o/test.csv",
            "smalldata/chess/chess_2x2x1000/h2o/train.csv",
            "smalldata/chess/chess_2x2x1000/R/test.csv",
            "smalldata/chess/chess_2x2x1000/R/train.csv",
            "smalldata/chess/chess_2x2x1000/rf.conf",
            "smalldata/chess/chess_2x2x1000/weka/test.csv.arff",
            "smalldata/chess/chess_2x2x1000/weka/train.csv.arff",
            "smalldata/chess/chess_2x2x200/h2o/test.csv",
            "smalldata/chess/chess_2x2x200/h2o/train.csv",
            "smalldata/chess/chess_2x2x200/R/test.csv",
            "smalldata/chess/chess_2x2x200/R/train.csv",
            "smalldata/chess/chess_2x2x200/rf.conf",
            "smalldata/chess/chess_2x2x200/weka/test.csv.arff",
            "smalldata/chess/chess_2x2x200/weka/train.csv.arff",
            "smalldata/chess/chess_2x2x500/h2o/chess_2x2_500_int.csv",
            "smalldata/chess/chess_2x2x500/h2o/test.csv",
            "smalldata/chess/chess_2x2x500/h2o/train.csv",
            "smalldata/chess/chess_2x2x500/R/test.csv",
            "smalldata/chess/chess_2x2x500/R/train.csv",
            "smalldata/chess/chess_2x2x500/rf.conf",
            "smalldata/chess/chess_2x2x500/weka/test.csv.arff",
            "smalldata/chess/chess_2x2x500/weka/train.csv.arff",
            "smalldata/chess/chess_8x8x1000/R/test.csv",
            "smalldata/chess/chess_8x8x1000/R/train.csv",
            "smalldata/constantColumn.csv",
            "smalldata/cuse.data.csv",
            "smalldata/cusedataREADME.rtf",
            "smalldata/cuseexpanded.csv",
            "smalldata/datagen1.csv",
            "smalldata/datetime/dd-mon-yr.csv",
            "smalldata/datetime/dd-mon-yy-with-other-cols.csv",
            "smalldata/drugs.csv",
            "smalldata/dummydata.csv",
            "smalldata/fail1_100x11000.csv.gz",
            "smalldata/fail2_24_100000_10.csv.gz",
            "smalldata/fish.csv",
            "smalldata/gaussian/sdss174052.csv.gz",
            "smalldata/gbm_test/CaliforniaHousing/cal_housing.data",
            "smalldata/gbm_test/CaliforniaHousing/cal_housing.domain",
            "smalldata/gbm_test/ecology_eval.csv",
            "smalldata/gbm_test/ecology_model.csv",
            "smalldata/gbm_test/Mfgdata_gaussian_GBM_testing.csv",
            "smalldata/gisette/Gisette_train_data.csv.gzip",
            "smalldata/gisette/Gisette_train_labels.csv.gzip",
            "smalldata/gisette/Gisette_valid_data.csv.gzip",
            "smalldata/gisette/Gisette_valid_labels.csv.gzip",
            "smalldata/glm_test/poisson_tst1.csv",
            "smalldata/glm_test/prostate_cat_replaced.csv",
            "smalldata/gt69436csv.data",
            "smalldata/handmade.csv",
            "smalldata/hex-443.parsetmp_1_0_0_0.data",
            "smalldata/hhp.cut3.214.data.gz",
            "smalldata/hhp_107_01.data.gz",
            "smalldata/hhp_9_17_12.predict.data.gz",
            "smalldata/histogram_test/30k_cattest.csv",
            "smalldata/histogram_test/50_cattest_test.csv",
            "smalldata/histogram_test/50_cattest_train.csv",
            "smalldata/histogram_test/alphabet_cattest.csv",
            "smalldata/histogram_test/bigcat_5000x2.csv",
            "smalldata/histogram_test/czechboard_300x300.csv",
            "smalldata/histogram_test/swpreds_1000x3.csv",
            "smalldata/housing.raw.txt",
            "smalldata/iris/iris.csv",
            "smalldata/iris/iris.csv.gz",
            "smalldata/iris/iris.csv.zip",
            "smalldata/iris/iris.xls",
            "smalldata/iris/iris.xlsx",
            "smalldata/iris/iris2.csv",
            "smalldata/iris/iris22.csv",
//              "smalldata/iris/iris_header.csv",
            "smalldata/iris/iris_test.csv",
            "smalldata/iris/iris_train.csv",
            "smalldata/iris/iris_wheader.csv",
            "smalldata/iris/iris_wheader.csv.gz",
            "smalldata/iris/iris_wheader.csv.zip",
            "smalldata/iris/iris_wheader.nonspd.csv",
            "smalldata/iris/leads.csv",
            "smalldata/jira/850.csv",
            "smalldata/jira/hex-1789.csv",
            "smalldata/jira/pub-180.csv",
            "smalldata/jira/pub-215.csv",
            "smalldata/jira/pub-35_test.csv",
            "smalldata/jira/pub-35_train.csv",
            "smalldata/jira/pub-569.csv",
            "smalldata/jira/pub_213.csv",
            "smalldata/jira/v-11.csv",
            "smalldata/jira/v-3.csv",
            "smalldata/kaggle/bestbuy_train_10k.csv.gz",
            "smalldata/kaggle/creditsample-test.csv.gz",
            "smalldata/kaggle/creditsample-training.csv.gz",
            "smalldata/kaggle/KDDTest.arff.gz",
            "smalldata/kaggle/KDDTrain.arff.gz",
            "smalldata/linreg/data.gz",
            "smalldata/logreg/100kx7_logreg.data.gz",
            "smalldata/logreg/benign.csv",
            "smalldata/logreg/benign.xls",
            "smalldata/logreg/failtoconverge_1000x501.csv.gz",
            "smalldata/logreg/failtoconverge_100x50.csv",
            "smalldata/logreg/logreg_trisum_int_cat_10000x10.csv",
            "smalldata/logreg/make_me_converge_10000x5.csv",
            "smalldata/logreg/princeton/copen.dat",
            "smalldata/logreg/princeton/cuse.dat",
            "smalldata/logreg/princeton/housing.raw",
            "smalldata/logreg/pros.xls",
            "smalldata/logreg/prostate.csv",
            "smalldata/logreg/prostate_long.csv.gz",
            "smalldata/logreg/prostate_test.csv",
            "smalldata/logreg/prostate_train.csv",
            "smalldata/logreg/syn_2659x1049.csv",
            "smalldata/logreg/syn_2659x1049x2enum.csv",
            "smalldata/logreg/syn_8686576441534898792_10000x100.csv",
            "smalldata/logreg/umass_chdage.csv",
            "smalldata/logreg/umass_statdata/cgd.dat",
            "smalldata/logreg/umass_statdata/cgd.txt",
            "smalldata/logreg/umass_statdata/chdage.dat",
            "smalldata/logreg/umass_statdata/chdage.txt",
            "smalldata/logreg/umass_statdata/chdage_cleaned.dat",
            "smalldata/logreg/umass_statdata/clslowbwt.dat",
            "smalldata/logreg/umass_statdata/clslowbwt.txt",
            "smalldata/logreg/umass_statdata/icu.dat",
            "smalldata/logreg/umass_statdata/icu.txt",
            "smalldata/logreg/umass_statdata/lowbwt.dat",
            "smalldata/logreg/umass_statdata/lowbwt.txt",
            "smalldata/logreg/umass_statdata/lowbwtm11.dat",
            "smalldata/logreg/umass_statdata/lowbwtm11.txt",
            "smalldata/logreg/umass_statdata/meexp.dat",
            "smalldata/logreg/umass_statdata/meexp.txt",
            "smalldata/logreg/umass_statdata/nhanes3.dat",
            "smalldata/logreg/umass_statdata/nhanes3.txt",
            "smalldata/logreg/umass_statdata/pbc.dat",
            "smalldata/logreg/umass_statdata/pbc.txt",
            "smalldata/logreg/umass_statdata/pharynx.dat",
            "smalldata/logreg/umass_statdata/pharynx.txt",
            "smalldata/logreg/umass_statdata/pros.dat",
            "smalldata/logreg/umass_statdata/pros.txt",
            "smalldata/logreg/umass_statdata/uis.dat",
            "smalldata/logreg/umass_statdata/uis.txt",
            "smalldata/logreg/why_perfect_training_100x500.csv",
            "smalldata/makedata.csv",
            "smalldata/marketing_naRemoved.csv",
            "smalldata/mixed_causes_NA.csv",
            "smalldata/mnist/readme.txt",
            "smalldata/mnist/test.csv.gz",
            "smalldata/mnist/train.csv.gz",
            "smalldata/mtcars.csv",
            "smalldata/Mushroom.gz",
            "smalldata/neural/Benchmark_dojo_test.data",
            "smalldata/neural/eightsq.data",
            "smalldata/neural/Readme.txt",
            "smalldata/neural/sin_pattern.data",
            "smalldata/neural/sumsigmoids.csv",
            "smalldata/neural/sumsigmoids_test.csv",
            "smalldata/neural/two_spiral.data",
            "smalldata/neural/two_spiral.png",
            "smalldata/parity_128_4_100_quad.data",
            "smalldata/parity_128_4_2_quad.data",
            "smalldata/parse_fail_double_space.csv",
            "smalldata/parse_folder_test/prostate_0.csv",
            "smalldata/parse_folder_test/prostate_1.csv",
            "smalldata/parse_folder_test/prostate_2.csv",
            "smalldata/parse_folder_test/prostate_3.csv",
            "smalldata/parse_folder_test/prostate_4.csv",
            "smalldata/parse_folder_test/prostate_5.csv",
            "smalldata/parse_folder_test/prostate_6.csv",
            "smalldata/parse_folder_test/prostate_7.csv",
            "smalldata/parse_folder_test/prostate_8.csv",
            "smalldata/parse_folder_test/prostate_9.csv",
            "smalldata/parse_zeros_100x8500.csv.gz",
            "smalldata/pca_test/AustraliaCoast.csv",
            "smalldata/pca_test/USArrests.csv",
            "smalldata/phbirths.raw.txt",
            "smalldata/pmml/cars-cater-rf-1tree.pmml",
            "smalldata/pmml/cars-cater-rf-50trees.pmml",
            "smalldata/pmml/cars-rf-1tree.pmml",
            "smalldata/pmml/cars-rf-50trees.pmml",
            "smalldata/pmml/copen-rf-1tree.pmml",
            "smalldata/pmml/copen-rf-50trees.pmml",
            "smalldata/pmml/iris_rf_1tree.pmml",
            "smalldata/pmml/iris_rf_500trees.pmml",
            "smalldata/pmml/SampleScorecard.pmml",
            "smalldata/poisson/Goalies.csv",
            "smalldata/poker/poker-hand-testing.data",
            "smalldata/poker/poker-hand.pl",
            "smalldata/poker/poker10",
            "smalldata/poker/poker100",
            "smalldata/poker/poker1000",
            "smalldata/prostate/prostate.bin.csv.zip",
            "smalldata/prostate/prostate.csv.zip",
            "smalldata/prostate/prostate.float.csv.zip",
            "smalldata/prostate/prostate.int.csv.zip",
            "smalldata/prostate/prostate.uuid.csv.zip",
            "smalldata/quantiles/breadth.csv",
            "smalldata/random1csv.data",
            "smalldata/randomdata2.csv",
            "smalldata/randomdata3.csv",
            "smalldata/runif.csv",
            "smalldata/runifA.csv",
            "smalldata/runifB.csv",
            "smalldata/runifC.csv",
            "smalldata/smtrees.csv",
            "smalldata/space_shuttle_damage.csv",
            "smalldata/stego/stego_testing.data",
            "smalldata/stego/stego_training.data",
            "smalldata/stego/stego_training_modified.data",
            "smalldata/swiss.csv",
            "smalldata/swiss_clean.csv",
            "smalldata/syn_binary10Kx100.csv.gz",
            "smalldata/syn_fp_prostate.csv",
            "smalldata/syn_sphere2.csv",
            "smalldata/syn_sphere3.csv",
            "smalldata/test/arit.csv",
            "smalldata/test/classifier/chess_test.csv",
            "smalldata/test/classifier/chess_train.csv",
            "smalldata/test/classifier/coldom_test_1.csv",
            "smalldata/test/classifier/coldom_test_1_2.csv",
            "smalldata/test/classifier/coldom_test_2.csv",
            "smalldata/test/classifier/coldom_test_3.csv",
            "smalldata/test/classifier/coldom_train_1.csv",
            "smalldata/test/classifier/coldom_train_2.csv",
            "smalldata/test/classifier/coldom_train_3.csv",
            "smalldata/test/classifier/iris_missing_values.csv",
            "smalldata/test/classifier/iris_test.csv",
            "smalldata/test/classifier/iris_test_extra.csv",
            "smalldata/test/classifier/iris_test_extra_with_na.csv",
            "smalldata/test/classifier/iris_test_missing.csv",
            "smalldata/test/classifier/iris_test_missing_extra.csv",
            "smalldata/test/classifier/iris_test_numeric.csv",
            "smalldata/test/classifier/iris_test_numeric_extra.csv",
            "smalldata/test/classifier/iris_test_numeric_extra2.csv",
            "smalldata/test/classifier/iris_test_numeric_missing.csv",
            "smalldata/test/classifier/iris_test_numeric_missing_extra.csv",
            "smalldata/test/classifier/iris_train.csv",
            "smalldata/test/classifier/iris_train_numeric.csv",
            "smalldata/test/classifier/multi_class.test.csv",
            "smalldata/test/classifier/multi_class.train.csv",
            "smalldata/test/cm/v1.csv",
            "smalldata/test/cm/v1n.csv",
            "smalldata/test/cm/v2.csv",
            "smalldata/test/cm/v2n.csv",
            "smalldata/test/cm/v3.csv",
            "smalldata/test/cm/v4.csv",
            "smalldata/test/cm/v4n.csv",
            "smalldata/test/HEX-287-small-files.data",
            "smalldata/test/hive.txt",
            "smalldata/test/HTWO-87-one-line-dataset-0.csv",
            "smalldata/test/HTWO-87-one-line-dataset-1dos.csv",
            "smalldata/test/HTWO-87-one-line-dataset-1unix.csv",
            "smalldata/test/HTWO-87-one-line-dataset-2dos.csv",
            "smalldata/test/HTWO-87-one-line-dataset-2unix.csv",
            "smalldata/test/HTWO-87-two-lines-dataset.csv",
            "smalldata/test/HTWO-87-two-unique-lines-dataset.csv",
            "smalldata/test/is_NA.csv",
            "smalldata/test/na_test.zip",
            "smalldata/test/R/titanic.csv",
//              "smalldata/test/rmodels/covtype-rf-50tree-as-factor-X5-20k.rdata",
//              "smalldata/test/rmodels/covtype-rf-50tree-as-factor-X5.rdata",
            "smalldata/test/rmodels/covtype.rf.2",
//              "smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-3.rdata",
//              "smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata",
//              "smalldata/test/rmodels/ozone.rf.10trees.rdata",
//              "smalldata/test/rmodels/prostate-rf-10tree-asFactorCapsule.rdata",
//              "smalldata/test/rmodels/prostate-rf-1tree-asFactorCapsule.rdata",
//              "smalldata/test/rmodels/prostate-rf-2tree-asFactorCapsule.rdata",
            "smalldata/test/rmodels/rf-iris-1tree.model",
            "smalldata/test/test1.dat",
            "smalldata/test/test_26cols_comma_sep.csv",
            "smalldata/test/test_26cols_multi_space_sep.csv",
            "smalldata/test/test_26cols_single_space_sep.csv",
            "smalldata/test/test_26cols_single_space_sep_2.csv",
            "smalldata/test/test_all_raw_top10rows.csv",
            "smalldata/test/test_enum_domain_size.csv",
            "smalldata/test/test_less_than_65535_unique_names.csv",
            "smalldata/test/test_manycol_tree.csv",
            "smalldata/test/test_more_than_65535_unique_names.csv",
            "smalldata/test/test_parse_mix.csv",
            "smalldata/test/test_percentiles_distns.csv.gz",
            "smalldata/test/test_percentiles_distns.R",
            "smalldata/test/test_quote.csv",
            "smalldata/test/test_time.csv",
            "smalldata/test/test_tree.csv",
            "smalldata/test/test_tree_minmax.csv",
            "smalldata/test/test_uuid.csv",
            "smalldata/test/test_uuid_na.csv",
            "smalldata/test/test_var.csv",
            "smalldata/Test_Arabic_Digit_short.data",
            "smalldata/tinyburn.csv",
            "smalldata/titanicalt.csv",
            "smalldata/tnc3.csv",
            "smalldata/tnc3_10.csv",
            "smalldata/tnc6.csv",
            "smalldata/toy_data_RF.csv",
            "smalldata/toykm.csv",
            "smalldata/trees.csv",
            "smalldata/Twitter2DB.txt",
            "smalldata/unbalanced/orange_small_test.data.zip",
            "smalldata/unbalanced/orange_small_train.data.zip",
            "smalldata/WBIsparsedata",
            "smalldata/weather.csv",
            "smalldata/wine.data",
            "smalldata/winesPCA.csv",
            "smalldata/wonkysummary.csv",
            "smalldata/zero_dot_zero_one.csv",
            "smalldata/zero_dot_zero_zero_one.csv",
            "smalldata/zinb.csv",
            "smalldata/zip_code/zip_code_database.csv.gz",
            "smalldata/zip_code/zipcodes",
            "smalldata/zipcodes",
    };
    for (String f : files) { TestUtil.parseFromH2OFolder(f); }

//    long seed = new Random().nextLong();
//    Log.info("seed: " + seed);
//    CreateFrame cf = new CreateFrame();
//    cf.key = "random";
//    cf.rows = (long)(new Random(seed).nextFloat()*100000f + 1);
//    cf.cols = (int)(new Random(seed).nextFloat()*1000f + 1);
//    cf.categorical_fraction = 0;
//    cf.integer_fraction = new Random(seed).nextFloat();
//    cf.integer_range = 1 + new Random(seed).nextInt(5);
//    cf.value = 0;
//    cf.randomize = true;
//    cf.missing_fraction = new Random(seed).nextFloat();
//    cf.factors = 2;
//    cf.response_factors = 1;
//    cf.positive_response = true;
//    cf.seed = seed;
//    cf.serve();

//    for (String f : files2) { TestUtil.parseFromH2OFolder(f); }
//    reBalanceFrames();
//    testTrainSplitFrames();
  }

  public void reBalanceFrames () {
    final Key [] keySet = H2O.KeySnapshot.globalSnapshot().keys();
    for (Key key : keySet) {
      final Value val = DKV.get(key);
      if (val == null || !val.isFrame()) continue;
      final Frame fr = val.get();
      if (!fr._key.toString().contains("balanced")) {
        final int splits = Math.min((int)fr.numRows(), 4*H2O.NUMCPUS*H2O.CLOUD.size());
        final String name = fr._key.toString() + ".rebalanced";
        Log.info("Load balancing frame under key '" + fr._key.toString() + "' into " + splits + " splits.");
        try {
          final Key frHexBalanced = Key.make(name);
          new RebalanceDataSet(fr, frHexBalanced, splits).invoke();
        } catch(Exception ex) {
          Log.err(ex.getMessage());
        }
      }
    }
  }

  public void testTrainSplitFrames () {
    final Key []  keySet = H2O.KeySnapshot.globalSnapshot().keys();
    for (Key key : keySet) {
      final Value val = DKV.get(key);
      if (val == null || !val.isFrame()) continue;
      final Frame fr = val.get();
      if (!fr._key.toString().contains("_part")) {
        try {
          FrameSplitPage fsp = new FrameSplitPage();
          fsp.source = fr;
          long seed = new Random().nextLong();
          Log.info("seed: " + seed);
          fsp.ratios = new float[]{0.001f + new Random(seed).nextFloat()*0.99f};
          Log.info("Splitting frame under key '" + fr._key.toString() + "' into " + fsp.ratios[0] + ".");
          fsp.split_keys = new Key[]{Key.make(), Key.make()};
          fsp.split_rows = null;
          fsp.split_ratios = null;
          fsp.invoke();
        } catch(Exception ex) {
          Log.err(ex.getMessage());
        }
      }
    }
  }
}
