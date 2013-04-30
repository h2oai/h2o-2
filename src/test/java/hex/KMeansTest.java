package hex;

import java.util.Random;

import org.junit.*;

import water.*;
import water.util.Log;
import water.util.Log.Tag.Sys;

public class KMeansTest extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(3);
  }

  @Test public void test1Dimension() {
    Key source = Key.make("datakey");
    Key target = Key.make("datakey.kmeans");

    try {
      ValueArray va = va_maker(source, //
          new double[] { 1.2, 5.6, 3.7, 0.6, 0.1, 2.6 });

      KMeans.RAND_SEED = 8683452581122892189L;
      KMeans.run(target, va, 2, 1e-6, 0);
      KMeans.KMeansModel res = UKV.get(target);
      double[][] clusters = res.clusters();

      Assert.assertEquals(1.125, clusters[0][0], 0.000001);
      Assert.assertEquals(4.65, clusters[1][0], 0.000001);
    } finally {
      KMeans.RAND_SEED = 0;
      UKV.remove(source);
      UKV.remove(target);
    }
  }

  @Test public void testGaussian() {
    testGaussian(10000);
  }

  public void testGaussian(int rows) {
    Key source = Key.make("datakey");
    Key target = Key.make("datakey.kmeans");

    try {
      KMeans.RAND_SEED = 8683452581122892188L;
      final int columns = 100;
      double[][] goals = new double[8][columns];
      double[][] array = gauss(columns, rows, goals);
      int[] cols = new int[columns];

      for( int i = 0; i < cols.length; i++ )
        cols[i] = i;

      ValueArray va = va_maker(source, (Object[]) array);
      Timer t = new Timer();
      KMeans.run(target, va, goals.length, 1e-6, cols);
      Log.debug(Sys.KMEAN, " testGaussian rows:" + rows + ", ms:" + t);
      KMeans.KMeansModel res = UKV.get(target);
      double[][] clusters = res.clusters();

      for( double[] goal : goals ) {
        boolean found = false;
        for( double[] cluster : clusters ) {
          if( match(cluster, goal) ) {
            found = true;
            break;
          }
        }
        Assert.assertTrue(found);
      }
    } finally {
      KMeans.RAND_SEED = 0;
      UKV.remove(source);
      UKV.remove(target);
    }
  }

  public static double[][] gauss(int columns, int rows, double[][] goals) {
    // rows and cols are reversed on this one for va_maker
    Random rand = KMeans.RAND_SEED == 0 ? new Random() : new Random(KMeans.RAND_SEED);

    for( int goal = 0; goal < goals.length; goal++ )
      for( int c = 0; c < columns; c++ )
        goals[goal][c] = rand.nextDouble() * 100;

    double[][] array = new double[columns][rows];
    gauss(goals, array);
    return array;
  }

  public static void gauss(double[][] goals, double[][] array) {
    Random rand = KMeans.RAND_SEED == 0 ? new Random() : new Random(KMeans.RAND_SEED);

    for( int r = 0; r < array[0].length; r++ ) {
      final int goal = rand.nextInt(goals.length);
      for( int c = 0; c < array.length; c++ )
        array[c][r] = goals[goal][c] + rand.nextGaussian();
    }
  }

  static boolean match(double[] cluster, double[] goal) {
    for( int i = 0; i < cluster.length; i++ )
      if( Math.abs(cluster[i] - goal[i]) > 1 ) return false;
    return true;
  }

  static double dist(double[] cluster, double[] goal) {
    double sum = 0;
    for( int i = 0; i < cluster.length; i++ ) {
      double d = cluster[i] - goal[i];
      sum += d * d;
    }
    return Math.sqrt(sum / cluster.length);
  }

  @Test public void testAirline() {
    Key k1 = loadAndParseKey("h.hex", "smalldata/airlines/allyears2k.zip");
    Key target = Key.make("air.kmeans");
    ValueArray va = UKV.get(k1);
    Timer t = new Timer();
    KMeans.run(target, va, 8, 1e-2, 0);
    Log.debug(Sys.KMEAN, "ms= " + t);
    KMeans.KMeansModel res = UKV.get(target);
    double[][] clusters = res.clusters();
    UKV.remove(k1);
    UKV.remove(target);
  }

  @Test public void testSphere() {
    Key k1 = loadAndParseKey("syn_sphere3.hex", "smalldata/syn_sphere3.csv");
    Key target = Key.make(KMeans.KEY_PREFIX + "sphere");
    ValueArray va = UKV.get(k1);
    KMeans.run(target, va, 3, 1e-2, 0, 1, 2);
    KMeans.KMeansModel res = UKV.get(target);
    double[][] clusters = res.clusters();
    UKV.remove(k1);
    UKV.remove(target);
  }
}
