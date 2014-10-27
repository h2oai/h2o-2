package hex;

import hex.deeplearning.DeepLearning;
import hex.deeplearning.DeepLearningModel;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.util.Log;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class DeepLearningReproducibilityTest extends TestUtil {

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
    Log.info("");
    Log.info("STARTING.");
    Log.info("Using seed " + seed);

    Map<Integer,Float> repeatErrs = new TreeMap<Integer,Float>();

    int N = 6;
    StringBuilder sb = new StringBuilder();
    float repro_error = 0;
    for (boolean repro : new boolean[]{true, false}) {
      Frame[] preds = new Frame[N];
      for (int repeat = 0; repeat < N; ++repeat) {
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

          // Build a regularized DL model with polluted training data, score on clean validation set
          DeepLearning p;
          p = new DeepLearning();
          p.source = train;
          p.validation = test;
          p.response = train.lastVec();
          p.ignored_cols = new int[]{1, 22}; //for weather data
          p.activation = DeepLearning.Activation.RectifierWithDropout;
          p.hidden = new int[]{32, 58};
          p.l1 = 1e-5;
          p.l2 = 3e-5;
          p.seed = 0xbebe;
          p.input_dropout_ratio = 0.2;
          p.hidden_dropout_ratios = new double[]{0.4, 0.1};
          p.epochs = 3.32;
          p.quiet_mode = true;
          p.reproducible = repro;
          try {
            Log.info("Starting with #" + repeat);
            p.invoke();
          } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
          } finally {
            p.delete();
          }

          // Extract the scoring on validation set from the model
          mymodel = UKV.get(p.dest());
          preds[repeat] = mymodel.score(test);
          repeatErrs.put(repeat, mymodel.error());

        } catch (Throwable t) {
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
      sb.append("Reproducibility: " + (repro ? "on" : "off") + "\n");
      sb.append("Repeat # --> Validation Error\n");
      for (String s : Arrays.toString(repeatErrs.entrySet().toArray()).split(","))
        sb.append(s.replace("=", " --> ")).append("\n");
      sb.append('\n');
      Log.info(sb.toString());

      try {
        if (repro) {
          // check reproducibility
          for (Float error : repeatErrs.values()) {
            Assert.assertTrue(error.equals(repeatErrs.get(0)));
          }
          for (Frame f : preds) {
            Assert.assertTrue(f.isIdentical(preds[0]));
          }
          repro_error = repeatErrs.get(0);
        } else {
          // check standard deviation of non-reproducible mode
          double mean = 0;
          for (Float error : repeatErrs.values()) {
            mean += error;
          }
          mean /= N;
          Log.info("mean error: " + mean);
          double stddev = 0;
          for (Float error : repeatErrs.values()) {
            stddev += (error - mean) * (error - mean);
          }
          stddev /= N;
          stddev = Math.sqrt(stddev);
          Log.info("standard deviation: " + stddev);
          Assert.assertTrue(stddev < 0.1 / Math.sqrt(N));
          Log.info("difference to reproducible mode: " + Math.abs(mean - repro_error) / stddev + " standard deviations");
        }
      } finally {
        for (Frame f : preds) if (f != null) f.delete();
      }
    }
  }
}

