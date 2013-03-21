package hex;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import org.junit.Test;
import com.google.gson.*;

import water.*;

public class HistogramTest extends TestUtil {
  @Test public void testHistogram() {
    Key datakey = Key.make("datakey");
    try {
      // Make some data to test with.
      double[] vals = new double[10000];
      Random r = new Random(12345);
      for (int i = 0; i < 10000; i++)
        vals[i] = -1223.0 + r.nextGaussian() * 135;
      ValueArray va = va_maker(datakey, vals);
      JsonObject json = Histogram.run(va, 0);
      assertEquals(json.get("Rows").getAsLong(), 10000);
      assertEquals(json.get("SD").getAsDouble(), 136.0, 0.5);
      assertEquals(json.get("Hist_Start").getAsDouble(), -1750.0, 0.5);
      assertEquals(json.get("Hist_End").getAsDouble(), -700.0, 0.5);
      assertEquals(json.get("Hist_Bins").getAsInt(), 21);
    } finally {
      UKV.remove(datakey);
    }
  }
}
