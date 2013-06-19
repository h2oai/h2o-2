package h2o.samples;

import water.*;

/**
 * Runs a map reduce task over a dataset and sums elements of a column.
 */
public class Part04_Sum {
  public static void main(String[] args) throws Exception {
    Weaver.registerPackage("h2o.samples");
    water.Boot._init.boot(new String[] { "-mainClass", UserMain.class.getName() });
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      H2O.main(args);
      System.out.println("Cloud is up");

      TestUtil.loadAndParseFile("test", "smalldata/gaussian/sdss174052.csv.gz");

    }
  }
}
