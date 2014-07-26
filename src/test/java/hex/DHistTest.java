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
      drf.source = parseFrame(destTrain, "../smalldata/histogram_test/alphabet_cattest.csv");
      drf.response = drf.source.vecs()[1];
      drf.classification = true;
      drf.ntrees = 100;
      drf.max_depth = 5; // = interaction.depth
      drf.min_rows = 10; // = nodesize
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
