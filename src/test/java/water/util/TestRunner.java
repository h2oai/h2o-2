package water.util;

import hex.KMeansTest;

import java.util.ArrayList;

/**
 * Builds a cloud by duplicating current JVM settings in different processes or machines, and runs tests.
 */
public class TestRunner {
  static final String USER = "cyprien";
  static final String KEY  = System.getProperty("user.home") + "/.ssh/id_rsa";

  public static void main(String[] args) throws Exception {
    ArrayList<Separate> sites = new ArrayList<Separate>();
    int nodes = 1 + 2;

    for( int i = 0; i < nodes - 1; i++ ) {
      // sites.add(new SeparateCL());
      sites.add(new SeparateVM("VM" + i, null));

      // String host = "192.168.1.15" + (i + 1);
      // sites.add(new SeparateBox(host, USER, KEY, new String[] { "init.Boot" }));
    }

    org.junit.runner.JUnitCore.runClasses(KMeansTest.class);

    // H2O.main(new String[] {});
    // TestUtil.stall_till_cloudsize(nodes + 1);
    // new KMeansTest().testGaussian((int) 1e6);

    for( Separate site : sites )
      site.close();

    // TODO proper shutdown of remaining threads?
    System.exit(0);
  }
}
