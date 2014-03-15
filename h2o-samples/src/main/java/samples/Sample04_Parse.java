package samples;

import water.H2O;
import water.TestUtil;

/**
 * Loads and parse a local file.
 */
public class Sample04_Parse {
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, args);
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);

      TestUtil.loadAndParseFile("test", "smalldata/gaussian/sdss174052.csv.gz");
      String url = "http://127.0.0.1:" + H2O.API_PORT + "/StoreView.html";
      System.out.println("Parsed file, key 'test' should be in the store: " + url);
    }
  }
}
