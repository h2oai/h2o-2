package water;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.lang.ArrayUtils;

import water.parser.ParseDataset;
import water.sys.Node;
import water.sys.NodeVM;

public class Sandbox {
  public static void main(String[] args) throws Exception {
    args = (String[]) ArrayUtils.addAll(args, new String[] { "--log_headers" });
    ArrayList<Node> workers = new ArrayList<Node>();

    for( int i = 0; i < 2; i++ ) {
      // workers.add(new NodeCL(args));
      workers.add(new NodeVM(args));

      // Host host = new Host("192.168.1.15" + (i + 1));
      // workers.add(new NodeHost(host, null, args));
    }

    for( Node worker : workers ) {
      worker.inheritIO();
      worker.start();
    }
    H2O.main(args);
    TestUtil.stall_till_cloudsize(1 + workers.size());

    File f = new File("smalldata/covtype/covtype.20k.data");
    // File f = new File("../../aaaa/datasets/millionx7_logreg.data.gz");
    // File f = new File("smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata");
    // File f = new File("py/testdir_single_jvm/syn_datasets/hastie_4x.data");
    Key key = TestUtil.load_test_file(f, "test");
    Key dest = Key.make("test.hex");
    ParseDataset.parse(dest, new Key[] { key });
  }
}
