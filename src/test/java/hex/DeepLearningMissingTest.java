package hex;

import hex.deeplearning.DeepLearning;
import hex.deeplearning.DeepLearningModel;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.util.Log;

import java.util.*;

public class DeepLearningMissingTest extends TestUtil {

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test
  public void run() {
    long seed = new Random().nextLong();

    DeepLearningModel mymodel = null;
    Frame train = null;
    Frame test = null;
    Frame data = null;
    DeepLearning p;
    Log.info("");
    Log.info("STARTING.");
    Log.info("Using seed " + seed);

    Map<DeepLearning.MissingValuesHandling,Double> sumErr = new TreeMap<DeepLearning.MissingValuesHandling,Double>();

    StringBuilder sb = new StringBuilder();
    for (DeepLearning.MissingValuesHandling mvh : new DeepLearning.MissingValuesHandling[]{
            DeepLearning.MissingValuesHandling.Skip, DeepLearning.MissingValuesHandling.MeanImputation
    }) {
      double sumerr = 0;
      Map<Double,Double> map = new TreeMap<Double, Double>();
      for (double missing_fraction : new double[]{0, 0.1, 0.25, 0.5, 0.75, 1}) {

        try {
          Key file = NFSFileVec.make(find_test_file("smalldata/weather.csv"));
//          Key file = NFSFileVec.make(find_test_file("smalldata/mnist/test.csv.gz"));
          data = ParseDataset2.parse(Key.make("data.hex"), new Key[]{file});

          // Create holdout test data on clean data (before adding missing values)
          FrameSplitter fs = new FrameSplitter(data, new float[]{0.75f});
          H2O.submitTask(fs).join();
          Frame[] train_test = fs.getResult();
          train = train_test[0];
          test = train_test[1];

          // add missing values to the training data (excluding the response)
          if (missing_fraction > 0) {
            Frame frtmp = new Frame(Key.make(), train.names(), train.vecs());
            frtmp.remove(frtmp.numCols() - 1); //exclude the response
            DKV.put(frtmp._key, frtmp);
            InsertMissingValues imv = new InsertMissingValues();
            imv.missing_fraction = missing_fraction;
            imv.seed = seed; //use the same seed for Skip and MeanImputation!
            imv.key = frtmp._key;
            imv.serve();
            DKV.remove(frtmp._key); //just remove the Frame header (not the chunks)
          }

          // Build a regularized DL model with polluted training data, score on clean validation set
          p = new DeepLearning();
          p.source = train;
          p.validation = test;
          p.response = train.lastVec();
          p.ignored_cols = new int[]{1,22}; //only for weather data
          p.missing_values_handling = mvh;
          p.activation = DeepLearning.Activation.RectifierWithDropout;
          p.hidden = new int[]{200,200};
          p.l1 = 1e-5;
          p.input_dropout_ratio = 0.2;
          p.epochs = 10;
          p.quiet_mode = true;
          try {
            Log.info("Starting with " + missing_fraction * 100 + "% missing values added.");
            p.invoke();
          } catch(Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
          } finally {
            p.delete();
          }

          // Extract the scoring on validation set from the model
          mymodel = UKV.get(p.dest());
          DeepLearningModel.Errors[] errs = mymodel.scoring_history();
          DeepLearningModel.Errors lasterr = errs[errs.length-1];
          double err = lasterr.valid_err;

          Log.info("Missing " + missing_fraction * 100 + "% -> Err: " + err);
          map.put(missing_fraction, err);
          sumerr += err;

        } catch(Throwable t) {
          t.printStackTrace();
          throw new RuntimeException(t);
        } finally {
          // cleanup
          if (mymodel != null) {
            mymodel.delete_xval_models();
            mymodel.delete_best_model();
            mymodel.delete();
          }
          if (train != null) train.delete();
          if (test != null) test.delete();
          if (data != null) data.delete();
        }
      }
      sb.append("\nMethod: " + mvh.toString() + "\n");
      sb.append("missing fraction --> Error\n");
      for (String s : Arrays.toString(map.entrySet().toArray()).split(",")) sb.append(s.replace("=", " --> ")).append("\n");
      sb.append('\n');
      sb.append("Sum Err: " + sumerr + "\n");

      sumErr.put(mvh, sumerr);
    }
    Log.info(sb.toString());
    Assert.assertTrue(sumErr.get(DeepLearning.MissingValuesHandling.Skip) > sumErr.get(DeepLearning.MissingValuesHandling.MeanImputation));
    Assert.assertTrue(sumErr.get(DeepLearning.MissingValuesHandling.MeanImputation) < 2); //this holds true for both datasets
  }
}

