package water.util;

import java.awt.Desktop;
import java.io.*;
import java.net.URI;

import water.*;
import water.TestUtil;
import water.parser.ParseDataset;

public class Sandbox {
  static final String USER = "cyprien";
  static final String KEY  = System.getProperty("user.home") + "/.ssh/id_rsa";

  public static void main(String[] args) throws Exception {
    H2O.main(new String[] {});
    TestUtil.stall_till_cloudsize(1);

    File f = new File("smalldata/covtype/covtype.20k.data");
    // File f = new File("../../aaaa/datasets/millionx7_logreg.data.gz");
    // File f = new File("smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata");
    // File f = new File("py/testdir_single_jvm/syn_datasets/hastie_4x.data");
    Key key = TestUtil.load_test_file(f, "test");
    Key dest = Key.make("test.hex");
    ParseDataset.parse(dest, new Key[]{key});
    Desktop desktop = Desktop.getDesktop();
    desktop.browse(new URI("http://localhost:54321/Inspect.html?key=test.hex"));
  }
}
