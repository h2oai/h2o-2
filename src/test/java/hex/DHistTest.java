package hex;

import org.junit.BeforeClass;

import hex.drf.DRF;
import hex.drf.DRF.DRFModel;

import java.util.Random;

import org.junit.Test;

import water.*;

public class DHistTest extends TestUtil {
  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  @Test public void testDBinom() {
    DRF drf = new DRF();
    Key destTrain = Key.make("data.hex");
    DRFModel model = null;

    try {
      // Configure DRF
      drf.source = parseFrame(destTrain, "../smalldata/dhisttest.csv");
      drf.response = drf.source.vecs()[1];
      drf.classification = true;
      drf.ntrees = 1;
      drf.max_depth = 1;
      drf.min_rows = 1; // = nodesize
      drf.nbins = 100;
      drf.destination_key = Key.make("DRF_model_dhist.hex");

      // Invoke DRF and block till the end
      drf.invoke();

      // Get the model
      model = UKV.get(drf.dest());
    } finally {
      drf.source.delete();
      drf.remove();
      if(model != null) model.delete(); // Remove the model
    }
  }

  @Test public void testDBinomRandGen() {
    DRF drf = new DRF();
    Key destTrain = Key.make("data.hex");
    DRFModel model = null;
    ValueArray va = null;

    int n = 50;
    long seed = 12345;
    String[] levels = new String[] {"A", "B", "C", "D"};
    try {
      // Make some data to test with.
      String[] vals = sample(levels, n, seed);
      byte  [] resp = new byte[n];
      for(int i = 0; i < n; i++) {
        resp[i] = (byte)(Math.random() < prob(vals[i]) ? 1 : 0);
      }
      va = va_maker(destTrain, vals, resp);

      // Configure DRF
      drf.source = va.asFrame();
      drf.response = drf.source.vecs()[1];
      drf.classification = true;
      drf.ntrees = 1;
      drf.max_depth = 1;
      drf.min_rows = 1; // = nodesize
      drf.nbins = 100;
      drf.destination_key = Key.make("DRF_model_dhist.hex");

      // Invoke DRF and block till the end
      drf.invoke();

      // Get the model
      model = UKV.get(drf.dest());
    } finally {
      va.delete();
      drf.source.delete();
      drf.remove();
      if(model != null) model.delete(); // Remove the model
    }
  }

  public String[] sample(String[] levels, int n, long seed) {
    Random rand = new Random(seed);
    int ncat = levels.length;

    String[] samp = new String[n];
    for(int i = 0; i < n; i++)
      samp[i] = levels[rand.nextInt(ncat)];
    return samp;
  }

  private double prob(String lev) {
    if(lev == "A") return 0.8;
    if(lev == "B") return 0.6;
    if(lev == "C") return 0.4;
    if(lev == "D") return 0.2;
    return 0.5;
  }
}
