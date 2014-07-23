package hex;

import hex.KMeans.Initialization;
import hex.KMeans2.KMeans2Model;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.fvec.Frame;
import water.util.Log;
import water.util.Log.Tag.Sys;

import java.util.Random;

public class KMeans2Test extends TestUtil {
  private static final long SEED = 8683452581122892189L;
  private static final double SIGMA = 3;

  private final void testHTML(KMeans2Model m) {
    StringBuilder sb = new StringBuilder();
    KMeans2.KMeans2ModelView kmv = new KMeans2.KMeans2ModelView();
    kmv.model = m;
    kmv.toHTML(sb);
    assert(sb.length() > 0);
  }

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test public void test1Dimension() {
    double[] data = new double[] { 1.2, 5.6, 3.7, 0.6, 0.1, 2.6 };
    double[][] rows = new double[data.length][1];
    for( int i = 0; i < rows.length; i++ )
      rows[i][0] = data[i];
    Frame frame = frame(new String[] { "C0" }, rows);
    KMeans2 algo = null;

    try {
      algo = new KMeans2();
      algo.source = frame;
      algo.k = 2;
      algo.initialization = Initialization.Furthest;
      algo.max_iter = 100;
      algo.seed = SEED;
      algo.invoke();
      KMeans2Model res = UKV.get(algo.dest());
      testHTML(res);
      Assert.assertTrue(res.get_params().state == Job.JobState.DONE); //HEX-1817
      double[][] clusters = res.centers;
      Assert.assertEquals(1.125, clusters[0][0], 0.000001);
      Assert.assertEquals(4.65, clusters[1][0], 0.000001);
      res.delete();
    } finally {
      frame.delete();
    }
  }

  @Test public void testGaussian() {
    testGaussian(10000);
  }

  public void testGaussian(int rows) {
    final int columns = 100;
    double[][] goals = new double[8][columns];
    double[][] array = gauss(columns, rows, goals);
    String[] names = new String[columns];
    for( int i = 0; i < names.length; i++ )
      names[i] = "C" + i;
    Frame frame = frame(names, array);
    KMeans2 algo = null;

    try {
      algo = new KMeans2();
      algo.source = frame;
      algo.k = goals.length;
      algo.initialization = Initialization.Furthest;
      algo.max_iter = 100;
      algo.seed = SEED;
      Timer t = new Timer();
      algo.invoke();
      KMeans2Model res = UKV.get(algo.dest());
      testHTML(res);
      Log.debug(Sys.KMEAN, " testGaussian rows:" + rows + ", ms:" + t);
      double[][] clusters = res.centers;

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
      res.delete();
    } finally {
      frame.delete();
    }
  }

  public static double[][] gauss(int columns, int rows, double[][] goals) {
    // rows and cols are reversed on this one for va_maker
    Random rand = new Random(SEED);
    for( int goal = 0; goal < goals.length; goal++ )
      for( int c = 0; c < columns; c++ )
        goals[goal][c] = rand.nextDouble() * 100;
    double[][] array = new double[rows][columns];
    gauss(goals, array);
    return array;
  }

  public static void gauss(double[][] goals, double[][] array) {
    Random rand = new Random(SEED);
    for( int r = 0; r < array.length; r++ ) {
      final int goal = rand.nextInt(goals.length);
      for( int c = 0; c < array[r].length; c++ )
        array[r][c] = goals[goal][c] + rand.nextGaussian() * SIGMA;
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
    Key dest = Key.make("dest");
    Frame frame = parseFrame(dest, "smalldata/airlines/allyears2k.zip");
    KMeans2 algo = new KMeans2();
    algo.source = frame;
    algo.k = 8;
    algo.initialization = Initialization.Furthest;
    algo.max_iter = 100;
    algo.seed = SEED;
    Timer t = new Timer();
    algo.invoke();
    Log.debug(Sys.KMEAN, "ms= " + t);
    KMeans2Model res = UKV.get(algo.dest());
    testHTML(res);
    Assert.assertEquals(algo.k, res.centers.length);
    frame.delete();
    res.delete();
  }

  @Test public void testSphere() {
    Key dest = Key.make("dest");
    Frame frame = parseFrame(dest, "smalldata/syn_sphere3.csv");
    KMeans2 algo = new KMeans2();
    algo.source = frame;
    algo.k = 3;
    algo.initialization = Initialization.Furthest;
    algo.max_iter = 100;
    algo.seed = SEED;
    Timer t = new Timer();
    algo.invoke();
    Log.debug(Sys.KMEAN, "ms= " + t);
    KMeans2Model res = UKV.get(algo.dest());
    testHTML(res);
    Assert.assertEquals(algo.k, res.centers.length);
    frame.delete();
    res.delete();
  }
}
