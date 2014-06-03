package hex;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import water.*;
import water.fvec.Frame;

public class FrameSplitterTest extends TestUtil {

  @Test public void splitTinyFrame() {
    Frame   dataset = null;
    float[] ratios  = arf(0.5f);
    Frame[] splits  = null;

    try {
      dataset = frame(ar("COL1"), ear(0,1,2,3,4,5,6,7,8,9));
      FrameSplitter fs = new FrameSplitter(dataset, ratios);
      H2O.submitTask(fs).join();
      splits = fs.getResult();
      Assert.assertEquals("The expected number of split frames is ratios.length+1", ratios.length+1, splits.length);
      for (Frame f : splits)
        Assert.assertEquals("The expected number of rows in partition.", 5, f.numRows() );
    } finally {
      // cleanup
      if (dataset!=null) dataset.delete();
      if (splits!=null)
        for(Frame sf : splits) if (sf!=null) sf.delete();
    }
  }

  @Test public void computeEspcTest() {
    // Split inside chunk
    long [] espc   = ar(0L, 2297L, 4591, 7000L);
    float[] ratios = arf(0.5f);
    long[][] result = FrameSplitter.computeEspcPerSplit(espc, espc[espc.length-1], ratios);
    Assert.assertArrayEquals(ar(ar(0L, 2297L, 3500L), ar(0L, 1091L, 3500L)), result);

    // Split inside chunk #2
    espc   = ar(0L, 1500L, 3000L, 4500L, 7000L);
    ratios = arf(0.5f);
    result = FrameSplitter.computeEspcPerSplit(espc, espc[espc.length-1], ratios);
    Assert.assertArrayEquals(ar(ar(0L, 1500L, 3000L, 3500L), ar(0L, 1000L, 3500L)), result);

    // Split on chunk boundary
    espc   = ar(0L, 1500L, 3500L, 4500L, 7000L);
    ratios = arf(0.5f);
    result = FrameSplitter.computeEspcPerSplit(espc, espc[espc.length-1], ratios);
    Assert.assertArrayEquals(ar(ar(0L, 1500L, 3500L), ar(0L, 1000L, 3500L)), result);
  }
}
