package hex;

import hex.KMeans.Initialization;

import java.util.Random;

import org.junit.*;

import water.*;
import water.Job.ColumnsJob;
import water.Job.ValidatedJob;
import water.fvec.Frame;

public class JobArgsTest extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(3);
  }

//  @Test public void test1Dimension() {
//    String[] names = new String[] { "5", "2", "1", "8" };
//    double[][] items = new double[names.length][10];
//    Random rand = new Random();
//    for( int r = 0; r < items.length; r++ )
//      for( int c = 0; c < items.length; c++ )
//        items[r][c] = rand.nextDouble();
//
//    Frame frame = frame(names, items);
//    UKV.put(Key.make("test"), frame);
//
//    try {
//      ValueArray va = va_maker(source, //
//          new double[] { 1.2, 5.6, 3.7, 0.6, 0.1, 2.6 });
//
//      KMeans.start(target, va, 2, Initialization.Furthest, 100, SEED, false, 0).get();
//      KMeansModel res = UKV.get(target);
//      double[][] clusters = res.clusters();
//
//      Assert.assertEquals(1.125, clusters[0][0], 0.000001);
//      Assert.assertEquals(4.65, clusters[1][0], 0.000001);
//    } finally {
//      UKV.remove(source);
//      UKV.remove(target);
//    }
//  }
//
//  static class TestJob extends ValidatedJob {
//    @Override protected void exec() {
//      // Assert col 1 & 4 have been selected
//      Assert.assertEquals(source.vecs()[1], _train[0]);
//      Assert.assertEquals(source.vecs()[3], _train[0]);
//    }
//  }
}
