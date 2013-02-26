package water.util;

import java.awt.Desktop;
import java.io.*;
import java.net.URI;
import java.util.ArrayList;

import water.*;
import water.parser.ParseDataset;

public class Sandbox {
  static final String USER = "cyprien";
  static final String KEY  = System.getProperty("user.home") + "/.ssh/id_rsa";

  public static void main(String[] args) throws Exception {
    ArrayList<Separate> sites = new ArrayList<Separate>();
    int nodes = 1 + 2;

    for( int i = 0; i < nodes - 1; i++ ) {
      // sites.add(new SeparateCL());
      sites.add(new SeparateVM("VM" + i, null));

    Desktop desktop = Desktop.getDesktop();
    // desktop.browse(new URI("http://localhost:54321/Jobs.html"));
    // desktop.browse(new URI("http://localhost:54321/Inspect.html?key=test.hex"));
    desktop.browse(new URI("http://localhost:54321/Timeline.html"));

    // org.junit.runner.JUnitCore.runClasses(KMeansTest.class);
    //org.junit.runner.JUnitCore.runClasses(GLMGridTest.class);

     H2O.main(new String[] {});
     TestUtil.stall_till_cloudsize(nodes);

     File f = new File("smalldata/covtype/covtype.20k.data");
     // File f = new File("../../aaaa/datasets/millionx7_logreg.data.gz");
     // File f = new File("smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata");
     // File f = new File("py/testdir_single_jvm/syn_datasets/hastie_4x.data");
     Key key = TestUtil.load_test_file(f, "test");
     Key dest = Key.make("test.hex");
     ParseDataset.parse(dest, DKV.get(key));

     // Key model = Key.make(KMeans.KMeansModel.KEY_PREFIX + "datakey.kmeans");
     // int[] cols = new int[10];
     // for( int i = 0; i < cols.length; i++ )
     // cols[i] = i;
     // KMeans.run(model, ValueArray.value(dest), 7, 1e-3, cols);

     Desktop desktop = Desktop.getDesktop();
     desktop.browse(new URI("http://localhost:54321/Inspect.html?key=test.hex"));

     BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
     console.readLine();

     //new KMeansTest().testGaussian((int) 1e6);

    for( Separate site : sites )
      site.close();

    // TODO proper shutdown for remaining threads?
    System.exit(0);
  }

  /**
   * Runs the Sandbox on a remote machine.
   */
  public static class Remote {
    public static void main(String[] _) throws Exception {
      String[] args = new String[] { "init.Boot", "-mainClass", Sandbox.class.getName() };
      SeparateBox box = new SeparateBox("192.168.1.150", USER, KEY, args);
      box.waitForEnd();
    }
  }
}
