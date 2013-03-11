package water.util;

import hex.GLMGridTest;

import java.awt.Desktop;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import water.*;

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
      sites.add(new SeparateVM("VM" + i, args));

      // String host = "192.168.1.15" + (i + 1);
      // sites.add(new SeparateBox(host, USER, KEY, new String[] { "init.Boot" }));
    }

    H2O.main(args);
    TestUtil.stall_till_cloudsize(nodes);

    Desktop desktop = Desktop.getDesktop();
    desktop.browse(new URI("http://localhost:54321/StoreView.html"));

    BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
    console.readLine();

    // new KMeansTest().testGaussian((int) 1e6);
    // org.junit.runner.JUnitCore.runClasses(KMeansTest.class);
    org.junit.runner.JUnitCore.runClasses(GLMGridTest.class);
    org.junit.runner.JUnitCore.runClasses(KVTest.class);


    for( Separate site : sites )
      site.close();

    // TODO proper shutdown of remaining threads?
    System.exit(0);
  }
}
