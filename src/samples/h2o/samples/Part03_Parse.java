package h2o.samples;

import water.*;

/**
 * Loads and parse a local file.
 */
public class Part03_Parse {
  public static void main(String[] args) throws Exception {
    Weaver.registerPackage("h2o.samples");
    water.Boot._init.boot(new String[] { "-mainClass", UserMain.class.getName() });
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      H2O.main(args);

      TestUtil.loadAndParseFile("test", "smalldata/gaussian/sdss174052.csv.gz");
      System.out.println("Parsed file, key 'test' should be in the store: http://127.0.0.1:54321/StoreView.html");
    }
  }
}
