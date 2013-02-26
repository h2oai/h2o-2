package hex;

import java.util.Random;

import org.junit.*;

import water.*;

public class KMeansTest extends TestUtil {

  @BeforeClass
  public static void stall() {
    stall_till_cloudsize(3);
  }

  @Test
  public void test1Dimension() {
    Key source = Key.make("datakey");
    Key target = Key.make(KMeans.KMeansModel.KEY_PREFIX+"datakey.kmeans");

    try {
      ValueArray va = va_maker(source, //
          new double[] { 1.2, 5.6, 3.7, 0.6, 0.1, 2.6 });

      KMeans.RAND_SEED = 8683452581122892189L;
      KMeans.run(target, va, 2, 1e-6, 0);
      KMeans.KMeansModel res = UKV.get(target, new KMeans.KMeansModel());
      double[][] clusters = res.clusters();

      Assert.assertEquals(1.125, clusters[0][0], 0.000001);
      Assert.assertEquals(4.65, clusters[1][0], 0.000001);
    } finally {
      KMeans.RAND_SEED = null;
      UKV.remove(source);
      UKV.remove(target);
    }
  }

  @Test
  public void testGaussian() {
    testGaussian(10000);
  }

  public void testGaussian(int rows) {
    Key source = Key.make("datakey");
    Key target = Key.make(KMeans.KMeansModel.KEY_PREFIX+"datakey.kmeans");

    try {
      KMeans.RAND_SEED = 8683452581122892189L;
      final int columns = 100;
      double[][] goals = new double[8][columns];
      double[][] array = gauss(columns, rows, goals);
      int[] cols = new int[columns];

      for( int i = 0; i < cols.length; i++ )
        cols[i] = i;

      ValueArray va = va_maker(source, (Object[]) array);
      long start = System.currentTimeMillis();
      KMeans.run(target, va, 8, 1e-6, cols);

      long stop = System.currentTimeMillis();
      Log.write("KMeansTest.testGaussian rows:" + rows + ", ms:" + (stop - start));
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
      KMeans.RAND_SEED = null;
      UKV.remove(source);
      UKV.remove(target);
    }
  }

  static double[][] gauss(int columns, int rows, double[][] goals) {
    // rows and cols are reversed on this one for va_maker
    double[][] array = new double[columns][rows];
    Random rand = KMeans.RAND_SEED == null ? new Random() : new Random(KMeans.RAND_SEED);

    for( int goal = 0; goal < goals.length; goal++ )
      for( int c = 0; c < columns; c++ )
        goals[goal][c] = rand.nextDouble() * 100;

    for( int r = 0; r < rows; r++ ) {
      int goal = rand.nextInt(goals.length);

      for( int c = 0; c < columns; c++ )
        array[c][r] = goals[goal][c] + rand.nextGaussian();
    }

    return array;
  }

  static boolean match(double[] cluster, double[] goal) {
    for( int i = 0; i < cluster.length; i++ )
      if( Math.abs(cluster[i] - goal[i]) > 1 )
        return false;

    return true;
  }
}
