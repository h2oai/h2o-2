package water.util;

import static water.util.ModelUtils.getPredictions;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class ModelUtilsTest {

  @Test
  public void getPredictionsTest() throws Exception {
    final double[] tieBreaker = new double [] { 0.82342,1435.7345,6043.222,92742.19220 };
    final float[] pred = new float [] { 1.000f, 0.002f, 0.002f, 0.005f, 0.003f, 0.002f, 0.001f, 0.002f, 0.002f, 0.004f, 0.003f, 0.002f };
    Assert.assertTrue(Arrays.equals(getPredictions(1, pred, tieBreaker), new int [] { 2 }));
    Assert.assertTrue(Arrays.equals(getPredictions(2, pred, tieBreaker), new int [] { 2, 8 }));
    Assert.assertTrue(Arrays.equals(getPredictions(3, pred, tieBreaker), new int [] { 2, 8, 3})
            || Arrays.equals(getPredictions(2, pred, tieBreaker), new int [] { 2, 8, 9 }));
    Assert.assertTrue(!Utils.contains(getPredictions(5, pred, tieBreaker), 5));
    Assert.assertTrue(!Utils.contains(getPredictions(10, pred, tieBreaker), 5));
    Assert.assertTrue(Utils.contains(getPredictions(11, pred, tieBreaker), 5));
  }

}
