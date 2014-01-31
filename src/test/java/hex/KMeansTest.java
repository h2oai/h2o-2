package hex;

import hex.KMeans.Initialization;

import java.util.Random;

import org.junit.*;

import water.*;
import water.util.Log;
import water.util.Log.Tag.Sys;

public class KMeansTest extends TestUtil {
  private static final long SEED = 8683452581122892189L;

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test public void test1Dimension() {
    Key source = Key.make("datakey");
    Key target = Key.make("datakey.kmeans");
    ValueArray va = null;
    KMeansModel res = null;
    try {
      va = va_maker(source, new double[] { 1.2, 5.6, 3.7, 0.6, 0.1, 2.6 });
      KMeans.start(target, va, 2, Initialization.Furthest, 100, SEED, false, 0).get();
      res = UKV.get(target);
      double[][] clusters = res.clusters();

      Assert.assertEquals(1.125, clusters[0][0], 0.000001);
      Assert.assertEquals(4.65, clusters[1][0], 0.000001);
    } finally {
      if( va != null ) va.delete();
      if( res != null ) res.delete();
    }
  }

  @Test public void testGaussian() {
    testGaussian(10000);
  }

  public void testGaussian(int rows) {
    Key source = Key.make("datakey");
    Key target = Key.make("datakey.kmeans");
    ValueArray va = null;
    KMeansModel res = null;
    try {
      final int columns = 100;
      double[][] goals = new double[8][columns];
      double[][] array = gauss(columns, rows, goals);
      int[] cols = new int[columns];

      for( int i = 0; i < cols.length; i++ )
        cols[i] = i;

      va = va_maker(source, (Object[]) array);
      Timer t = new Timer();
      KMeans.start(target, va, goals.length, Initialization.Furthest, 100, SEED, false, cols).get();
      Log.debug(Sys.KMEAN, " testGaussian rows:" + rows + ", ms:" + t);
      res = UKV.get(target);
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
      if( va != null ) va.delete();
      if( res != null ) res.delete();
    }
  }

  public static double[][] gauss(int columns, int rows, double[][] goals) {
    // rows and cols are reversed on this one for va_maker
    Random rand = new Random(SEED);
    for( int goal = 0; goal < goals.length; goal++ )
      for( int c = 0; c < columns; c++ )
        goals[goal][c] = rand.nextDouble() * 100;
    double[][] array = new double[columns][rows];
    gauss(goals, array);
    return array;
  }

  public static void gauss(double[][] goals, double[][] array) {
    Random rand = new Random(SEED);
    for( int r = 0; r < array[0].length; r++ ) {
      final int goal = rand.nextInt(goals.length);
      for( int c = 0; c < array.length; c++ )
        array[c][r] = goals[goal][c] + rand.nextGaussian();
    }
  }

  static boolean match(double[] cluster, double[] goal) {
    for( int i = 0; i < cluster.length; i++ )
      if( Math.abs(cluster[i] - goal[i]) > 1 )
        return false;
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
    Key k1 = loadAndParseFile("h.hex", "smalldata/airlines/allyears2k.zip");
    Key target = Key.make("air.kmeans");
    ValueArray va = UKV.get(k1);
    Timer t = new Timer();
    KMeans.start(target, va, 8, Initialization.Furthest, 100, SEED, false, 0).get();
    Log.debug(Sys.KMEAN, "ms= " + t);
    KMeansModel res = UKV.get(target);
    res.clusters();
    va.delete();
    res.delete();
  }

  @Test public void testSphere() {
    Key k1 = loadAndParseFile("syn_sphere3.hex", "smalldata/syn_sphere3.csv");
    Key target = Key.make(KMeans.KEY_PREFIX + "sphere");
    ValueArray va = UKV.get(k1);
    KMeans.start(target, va, 3, Initialization.Furthest, 100, SEED, false, 0, 1, 2).get();
    KMeansModel res = UKV.get(target);
    res.clusters();
    va.delete();
    res.delete();
  }
}
