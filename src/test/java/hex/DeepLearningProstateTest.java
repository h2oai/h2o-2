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

public class DeepLearningProstateTest extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test public void run() {
    Key file = NFSFileVec.make(find_test_file("smalldata/./logreg/prostate.csv"));
    Frame frame = ParseDataset2.parse(Key.make(), new Key[]{file});

    Key dest = Key.make("prostate");

    // build the model, with all kinds of shuffling/rebalancing/sampling
    {
      DeepLearning p = new DeepLearning();
      p.epochs = 35;
      p.source = frame;
      p.validation = frame;
      p.response = frame.vecs()[1];
      p.destination_key = dest;
      p.seed = 0xC01DF337;

      p.force_load_balance = true; //rebalance for multi-threading
      p.shuffle_training_data = true; //shuffle training data
      p.score_training_samples = 100; //sample training data for scoring
//      p.balance_classes = true; //rebalance classes for higher accuracy
//      p.score_validation_sampling = DeepLearning.ClassSamplingMethod.Stratified; //stratified sampling of validation set for scoring

      // Train the model via checkpointing
      DeepLearningModel mymodel = p.initModel();
      p.trainModel(mymodel);
      p.trainModel(mymodel, p.epochs); //incremental training
      p.trainModel(mymodel, p.epochs); //incremental training
      p.delete();
    }

    // score and check result (on full data)
    {
      DeepLearningModel mymodel = UKV.get(dest); //this actually *requires* frame to also still be in UKV (because of DataInfo...)
      Frame pred = mymodel.score(frame);
      StringBuilder sb = new StringBuilder();
      // Score binary classification
      AUC auc = new AUC();
      auc.actual = frame;
      auc.vactual = frame.vecs()[1];
      auc.predict = pred;
      auc.vpredict = pred.vecs()[2];
      auc.threshold_criterion = AUC.ThresholdCriterion.maximum_F1;
      auc.serve();
      auc.toASCII(sb);
      final double threshold = auc.threshold();
      final double error = auc.err();
      Log.info(sb);

      // check that auc.cm() is the right CM
      Assert.assertEquals(new ConfusionMatrix(auc.cm()).err(), error, 1e-15);

      // check that calcError() is consistent as well (for CM=null, AUC!=null)
      Assert.assertEquals(mymodel.calcError(frame, pred, "training", false, null, auc), error, 1e-15);

      // Now create labels using the same threshold as the AUC object and compute the CM

      double CMerrorOrig;
      {
        sb = new StringBuilder();
        water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
        CM.actual = frame;
        CM.vactual = frame.vecs()[1];
        CM.predict = pred;
        CM.vpredict = pred.vecs()[0];
        CM.serve();
        sb.append("\n");
        CM.toASCII(sb);
        Log.info(sb);
        CMerrorOrig = new ConfusionMatrix(CM.cm).err();
      }

      {
        // confirm that orig CM was made with threshold 0.5
        // put pred2 into UKV, and allow access
        Frame pred2 = new Frame(Key.make("pred2"), pred.names(), pred.vecs());
        pred2.delete_and_lock(null);
        pred2.unlock(null);

        // make labels with 0.5 threshold
        Env ev = Exec2.exec("pred2[,1]=pred2[,3]>=" + 0.5);
        pred2 = ev.popAry();
        ev.subRef(pred2, "pred2");
        ev.remove_and_unlock();

        water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
        CM.actual = frame;
        CM.vactual = frame.vecs()[1];
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
        CM.actual = frame;
        CM.vactual = frame.vecs()[1];
        CM.predict = pred2;
        CM.vpredict = pred2.vecs()[0];
        CM.serve();
        sb = new StringBuilder();
        sb.append("\n");
        CM.toASCII(sb);
        Log.info(sb);
        double threshErr2 = new ConfusionMatrix(CM.cm).err();
        Assert.assertEquals(threshErr2, error, 1e-15);
        pred2.delete();
      }

      pred.delete();
      mymodel.delete();
    }

    frame.delete();
  }
}
