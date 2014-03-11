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
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;

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

      // check that auc.cm() is the right CM
      Assert.assertEquals(new ConfusionMatrix(auc.cm()).err(), error, 1e-15);

      // check that calcError() is consistent as well (for CM=null, AUC!=null)
      Assert.assertEquals(mymodel.calcError(frame, pred, "training", true, null, auc), error, 1e-15);

      // Now create labels using the same threshold as the AUC object and compute the CM

      /*
      // Version 1: Exec2
//      new Frame(Key.make("pred"), pred.names(), pred.vecs()).delete_and_lock(Key.make("job")); //put prediction into KV store
//      Frame labels = Exec2.exec("pred[,0]=pred[,2]>=" + threshold).popAry(); //apply threshold criterion

      // Version 2: Without mapreduce (OK since tiny data)
      for (int i=0; i<pred.numRows(); ++i)
        pred.vecs()[0].set(i, pred.vecs()[2].at(i) >= threshold ? 1 : 0);

      water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
      CM.actual = frame;
      CM.vactual = frame.vecs()[1];
      CM.predict = pred;
      CM.vpredict = pred.vecs()[0];
      CM.serve();
      sb.append("\n");
      CM.toASCII(sb);
      Log.info(sb);

      // check that threshold-specific CM matches the AUC-reported CM
      Assert.assertEquals(new ConfusionMatrix(CM.cm).err(), error, 1e-15);
      */

      pred.delete();
      mymodel.delete();
    }

    frame.delete();
  }
}
