package hex;

import hex.deeplearning.DeepLearning;
import hex.deeplearning.DeepLearningModel;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.JUnitRunnerDebug;
import water.Key;
import water.TestUtil;
import water.UKV;
import water.api.AUC;
import water.exec.Env;
import water.exec.Exec2;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.util.Log;

import java.util.Random;

public class DeepLearningProstateTest extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test public void run() {
    Key file = NFSFileVec.make(find_test_file("smalldata/./logreg/prostate.csv"));
    Frame frame = ParseDataset2.parse(Key.make(), new Key[]{file});
    Key vfile = NFSFileVec.make(find_test_file("smalldata/./logreg/prostate.csv"));
    Frame vframe = ParseDataset2.parse(Key.make(), new Key[]{vfile});

    int count = 0;
    for (boolean replicate : new boolean[]{
            true,
//            false,
    }) {
      for (boolean load_balance : new boolean[]{
              true,
//            false,
      }) {
        for (boolean shuffle : new boolean[]{
                true,
//              false,
        }) {
          for (boolean balance_classes : new boolean[]{
                  true,
                  false,
          }) {
            for (int resp : new int[]{
                    1, //binary
                    2, //regression
                    8, //multi-class
            }) {
              for (DeepLearning.ClassSamplingMethod csm : new DeepLearning.ClassSamplingMethod[] {
                      DeepLearning.ClassSamplingMethod.Stratified,
//                    DeepLearning.ClassSamplingMethod.Uniform
              }) {
                for (int scoretraining : new int[]{
                        200,
//                      0,
                }) {
                  for (int scorevalidation : new int[]{
                          200,
//                        0,
                  }) {

                    for (int vf : new int[]{
                            0,  //no validation
                            1,  //same as source
                            -1, //different validation frame
                    }) {
                      count++;
                      Log.info("**************************)");
                      Log.info("Starting test #" + count);
                      Log.info("**************************)");
                      Frame valid = null; //no validation
                      if (vf == 1) valid = frame; //use the same frame for validation
                      else if (vf == -1) valid = vframe; //different validation frame (here: from the same file)

                      Key dest = Key.make("prostate");

                      // build the model, with all kinds of shuffling/rebalancing/sampling
                      {
                        final long seed = new Random().nextLong();
                        Log.info("Using seed: " + seed);
                        DeepLearning p = new DeepLearning();
                        p.epochs = 1.0 + new Random(seed).nextDouble();
                        p.source = frame;
                        p.hidden = new int[]{1+new Random(seed).nextInt(4), 1+new Random(seed).nextInt(6)};
                        p.response = frame.vecs()[resp];
                        if (resp == 2) p.classification = false;
                        p.destination_key = dest;
                        p.seed = seed;
                        p.validation = valid;
                        p.force_load_balance = load_balance;
                        p.replicate_training_data = replicate;
                        p.shuffle_training_data = shuffle;
                        p.score_training_samples = scoretraining;
                        p.score_validation_samples = scorevalidation;
                        p.balance_classes = balance_classes;
                        p.quiet_mode = true;
                        p.score_validation_sampling = csm;
//                      p.execImpl();

                        // Train the model via checkpointing
                        DeepLearningModel mymodel = p.initModel();
                        p.trainModel(mymodel);
                        p.trainModel(mymodel, p.epochs); //incremental training
                        p.delete();
                      }

                      // score and check result (on full data)
                      final DeepLearningModel mymodel = UKV.get(dest); //this actually *requires* frame to also still be in UKV (because of DataInfo...)
                      if (valid == null ) valid = frame;
                      if (mymodel.isClassifier()) {
                        Frame pred = mymodel.score(valid);
                        StringBuilder sb = new StringBuilder();

                        AUC auc = new AUC();
                        double threshold = 0;
                        double error = 0;
                        // binary
                        if (resp == 1) {
                          auc.actual = valid;
                          auc.vactual = valid.vecs()[resp];
                          auc.predict = pred;
                          auc.vpredict = pred.vecs()[2];
                          auc.threshold_criterion = AUC.ThresholdCriterion.maximum_F1;
                          auc.serve();
                          auc.toASCII(sb);
                          threshold = auc.threshold();
                          error = auc.err();
                          Log.info(sb);

                          // check that auc.cm() is the right CM
                          Assert.assertEquals(new ConfusionMatrix(auc.cm()).err(), error, 1e-15);

                          // check that calcError() is consistent as well (for CM=null, AUC!=null)
                          Assert.assertEquals(mymodel.calcError(valid, pred, pred, "training", false, null, auc, null), error, 1e-15);
                        }

                        // Compute CM
                        double CMerrorOrig;
                        {
                          sb = new StringBuilder();
                          water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
                          CM.actual = valid;
                          CM.vactual = valid.vecs()[resp];
                          CM.predict = pred;
                          CM.vpredict = pred.vecs()[0];
                          CM.serve();
                          sb.append("\n");
                          CM.toASCII(sb);
                          Log.info(sb);
                          CMerrorOrig = new ConfusionMatrix(CM.cm).err();
                        }

                        // confirm that orig CM was made with threshold 0.5
                        // put pred2 into UKV, and allow access
                        Frame pred2 = new Frame(Key.make("pred2"), pred.names(), pred.vecs());
                        pred2.delete_and_lock(null);
                        pred2.unlock(null);

                        if (resp == 1) {
                          // make labels with 0.5 threshold for binary classifier
                          Env ev = Exec2.exec("pred2[,1]=pred2[,3]>=" + 0.5);
                          pred2 = ev.popAry();
                          ev.subRef(pred2, "pred2");
                          ev.remove_and_unlock();

                          water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
                          CM.actual = valid;
                          CM.vactual = valid.vecs()[1];
                          CM.predict = pred2;
                          CM.vpredict = pred2.vecs()[0];
                          CM.serve();
                          sb = new StringBuilder();
                          sb.append("\n");
                          CM.toASCII(sb);
                          Log.info(sb);
                          double threshErr = new ConfusionMatrix(CM.cm).err();
                          Assert.assertEquals(threshErr, CMerrorOrig, 1e-15);

                          // make labels with AUC-given threshold for best F1
                          ev = Exec2.exec("pred2[,1]=pred2[,3]>=" + threshold);
                          pred2 = ev.popAry();
                          ev.subRef(pred2, "pred2");
                          ev.remove_and_unlock();

                          CM = new water.api.ConfusionMatrix();
                          CM.actual = valid;
                          CM.vactual = valid.vecs()[1];
                          CM.predict = pred2;
                          CM.vpredict = pred2.vecs()[0];
                          CM.serve();
                          sb = new StringBuilder();
                          sb.append("\n");
                          CM.toASCII(sb);
                          Log.info(sb);
                          double threshErr2 = new ConfusionMatrix(CM.cm).err();
                          Assert.assertEquals(threshErr2, error, 1e-15);
                        }
                        pred2.delete();
                        pred.delete();
                      } //classifier
                      mymodel.delete();
                      UKV.remove(dest);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    frame.delete();
    vframe.delete();
    Log.info("Tested " + count + " parameter combinations.");
  }
}
