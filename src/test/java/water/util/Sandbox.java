package water.util;

import java.awt.Desktop;
import java.io.*;
import java.net.URI;

import water.*;
import water.parser.ParseDataset;

public class Sandbox {
  public static void main(String[] args) throws Exception {
    H2O.main(new String[] {});
    // File f = new File("smalldata/covtype/covtype.20k.data");
    File f = new File("../../aaaa/datasets/millionx7_logreg.data.gz");
    // File f = new File("smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata");
    Key key = TestUtil.load_test_file(f, "test");
    Key dest = Key.make("test.hex");
    ParseDataset.parse(dest, DKV.get(key));

    // Key model = Key.make(KMeans.KMeansModel.KEY_PREFIX + "datakey.kmeans");
    // int[] cols = new int[10];
    // for( int i = 0; i < cols.length; i++ )
    // cols[i] = i;
    // KMeans.run(model, ValueArray.value(dest), 7, 1e-3, cols);

    Desktop desktop = Desktop.getDesktop();
    // desktop.browse(new URI("http://localhost:54321/Jobs.html"));
    // desktop.browse(new URI("http://localhost:54321/Inspect.html?key=test.hex"));
    desktop.browse(new URI("http://localhost:54321/Timeline.html"));

    BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
    console.readLine();
  }
}
