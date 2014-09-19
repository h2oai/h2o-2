package hex;

import hex.KMeans2.KMeans2Model;
import hex.KMeans2.Initialization;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.fvec.FVecTest;
import water.fvec.Frame;
import water.fvec.ParseDataset2;
import water.util.Log;
import water.util.Log.Tag.Sys;

import java.util.Arrays;
import java.util.Random;

public class KMeans2Test extends TestUtil {
  private static final long SEED = 8683452581122892189L;
  private static final double SIGMA = 3;

  public static final void testHTML(KMeans2Model m) {
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
    KMeans2 algo;

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
    KMeans2 algo;

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
    Frame frame = parseFrame(dest, "smalldata/syn_sphere2.csv");
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

  private double[] d(double... ds) { return ds; }

  boolean close(double[] a, double[] b) {
    for (int i=0;i<a.length;++i) {
      if (Math.abs(a[i]-b[i]) > 1e-8) return false;
    }
    return true;
  }

  @Test public void testCentroids(){
    String data =
            "1, 0, 0\n" +
                    "0, 1, 0\n" +
                    "0, 0, 1\n";
    Frame fr = null;
    try {
      Key k = FVecTest.makeByteVec("yada", data);
      fr = ParseDataset2.parse(Key.make(), new Key[]{k});

      for( boolean normalize : new boolean[]{false, true}) {
        for( Initialization init : new Initialization[]{Initialization.None, Initialization.PlusPlus, Initialization.Furthest}) {
          KMeans2 parms = new KMeans2();
          parms.source = fr;
          parms.k = 3;
          parms.normalize = normalize;
          parms.max_iter = 100;
          parms.initialization = init;
          parms.seed = 0;
          parms.invoke();
          KMeans2Model kmm = UKV.get(parms.dest());
          Assert.assertTrue(kmm.centers[0][0] + kmm.centers[0][1] + kmm.centers[0][2] == 1);
          Assert.assertTrue(kmm.centers[1][0] + kmm.centers[1][1] + kmm.centers[1][2] == 1);
          Assert.assertTrue(kmm.centers[2][0] + kmm.centers[2][1] + kmm.centers[2][2] == 1);
          Assert.assertTrue(kmm.centers[0][0] + kmm.centers[1][0] + kmm.centers[2][0] == 1);
          Assert.assertTrue(kmm.centers[0][0] + kmm.centers[1][0] + kmm.centers[2][0] == 1);
          Assert.assertTrue(kmm.centers[0][0] + kmm.centers[1][0] + kmm.centers[2][0] == 1);
          testHTML(kmm);
          kmm.delete();
        }
      }

    } finally {
      if( fr  != null ) fr.delete();
    }
  }

  @Test public void testNAColLast(){
    String[] datas = new String[]{
            new String(
                    "1, 0, ?\n" + //33% NA in col 3
                            "0, 2, 0\n" +
                            "0, 0, 3\n"
            ),
            new String(
                    "1, ?, 0\n" + //33% NA in col 2
                            "0, 2, 0\n" +
                            "0, 0, 3\n"
            ),
            new String(
                    "?, 0, 0\n" + //33% NA in col 1
                            "0, 2, 0\n" +
                            "0, 0, 3\n"
    )};
    Frame fr = null;
      for (String data : datas){
        try {
          Key k = FVecTest.makeByteVec("yada", data);
          fr = ParseDataset2.parse(Key.make(), new Key[]{k});

          for (boolean drop_na : new boolean[]{false, true}) {
            for (boolean normalize : new boolean[]{false, true}) {
              for (Initialization init : new Initialization[]{Initialization.None, Initialization.PlusPlus, Initialization.Furthest}) {
                KMeans2 parms = new KMeans2();
                parms.source = fr;
                parms.k = 3;
                parms.normalize = normalize;
                parms.max_iter = 100;
                parms.initialization = init;
                parms.drop_na_cols = drop_na;
                parms.seed = 0;
                parms.invoke();
                KMeans2Model kmm = UKV.get(parms.dest());
                testHTML(kmm);
                kmm.delete();
              }
            }
          }
        } finally {
        if( fr  != null ) fr.delete();
      }
    }
  }
}
