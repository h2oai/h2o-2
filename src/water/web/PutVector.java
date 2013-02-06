
package water.web;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.Properties;
import water.*;
import water.exec.Helpers;
import water.exec.VABuilder;

/**
 *
 * @author peta
 */
public class PutVector extends JSONPage {

  public static Key storeDoublesAsValue(Key k, double[] d) {
    byte[] bits = MemoryManager.malloc1(8*d.length);
    double min = d[0];
    double max = d[0];
    double tot = 0;
    for (int i = 0; i < d.length; ++i) {
      UDP.set8d(bits, i*8,d[i]);
      if (d[i] < min)
        min = d[i];
      if (d[i] > max)
        max = d[i];
      tot += d[i];
    }
    // The 1 tiny arraylet
    Key key2 = ValueArray.getChunkKey(0, k);
    Value val = new Value(key2, bits);
    DKV.put(key2, val);
    // The metadata
    VABuilder b = new VABuilder(k.toString(),d.length).addDoubleColumn("0",min, max, tot/d.length).createAndStore(k);
    DKV.write_barrier();
    Helpers.calculateSigma(k,0);
    return k;
  }

  public static double[] parseSpaceSeparatedDoubles(String from) {
    String[] s = from.split(" ");
    double[] d = new double[s.length];
    for (int i = 0; i< s.length; ++i)
      d[i] = Double.parseDouble(s[i]);
    return d;
  }

  @Override public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    JsonObject result = new JsonObject();
    try {
      Key k = Key.make(parms.getProperty("Key"));
      double[] d = parseSpaceSeparatedDoubles(parms.getProperty("Value"));
      if (d.length>200000)
        throw new IOException("Only vectors of up to 200000 values are supported for now.");
      k = storeDoublesAsValue(k,d);
      result.addProperty("Key",k.toString());
    } catch (Exception e) {
      result.addProperty("Error", e.toString());
    }
    return result;
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key", "Value" };
  }


}
