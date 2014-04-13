package hex;

import junit.framework.Assert;

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
}
