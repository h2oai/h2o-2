package test;

import java.awt.Desktop;
import java.io.*;
import java.net.URI;
import java.util.ArrayList;

import water.*;
import water.parser.ParseDataset;

/**
 * Builds a cloud by duplicating current JVM settings in different processes or machines, and runs tests.
 */
public class TestRunner {
  static final String USER = "cyprien";
  static final String KEY  = System.getProperty("user.home") + "/.ssh/id_rsa";

  public static void main(String[] args) throws Exception {
    H2O.main(new String[] {});
    File f = new File("smalldata/covtype/covtype.20k.data");
    // File f = new File("smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata");
    Key key = TestUtil.load_test_file(f, "test");
    Key dest = Key.make("test.hex");
    ParseDataset.parse(dest, DKV.get(key));

    Desktop desktop = Desktop.getDesktop();
    desktop.browse(new URI("http://localhost:54321/Inspect.html?key=test.hex"));

    BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
    console.readLine();

    //

    ArrayList<Separate> sites = new ArrayList<Separate>();
    int nodes = 3;

    for( int i = 0; i < nodes; i++ ) {
      // sites.add(new SeparateCL());
      // sites.add(new SeparateVM("VM" + i, null));

      String host = "192.168.1.15" + (i + 1);
      sites.add(new SeparateBox(host, USER, KEY, new String[] { "init.Boot" }));
    }

    // org.junit.runner.JUnitCore.runClasses(KMeansTest.class);

    H2O.main(new String[] {});
    TestUtil.stall_till_cloudsize(nodes + 1);
    new KMeansTest().testGaussian((int) 1e6);

    for( Separate site : sites )
      site.close();

    // TODO proper shutdown of remaining threads?
    System.exit(0);
  }
}
