package water;

import java.io.File;

import org.apache.commons.lang.ArrayUtils;

import water.deploy.*;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.util.Utils;

public class Sandbox {
  public static void main(String[] args) throws Exception {
    String line = "-mainClass " + UserCode.class.getName() + " -beta"; // -name s8koPQJ72ZC8Jh66uGeR
    args = Utils.add(args, line.split(" "));
    Boot._init.boot2(args);
    //ec2(args);
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      localCloud(1, true, args);

      covtype();

      File f;
      Key dest, fkey;
      f = new File("smalldata/categoricals/TwoBedrooms_Rent_Neighborhoods.csv.gz");
      // f = new File("smalldata/covtype/covtype.20k.data");
      // f = new File("syn_5853362476331324036_100x11.csv");
      // f = new File("../../aaaa/datasets/millionx7_logreg.data.gz");
      // f = new File("smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata");
      // f = new File("py/testdir_single_jvm/syn_datasets/hastie_4x.data");

      dest = Key.make("train.hex");
      fkey = NFSFileVec.make(f);
      ParseDataset2.parse(dest, new Key[] { fkey });

//      Key key = TestUtil.load_test_file(f, "test");
//      Key dest = Key.make("test.hex");
//      ParseDataset.parse(dest, new Key[] { key });
//      ValueArray va = (ValueArray) UKV.get(dest);

      Utils.readConsole();

      // @formatter:off
//      double[][] array = new double[][] {
//        new double[] { 0,0,0,0 },
//        new double[] { 1.63475416828,1.63337340671,-7.01908639681,2.72330313693 },
//        new double[] { -7.01908639681,2.72330313693,-3.47665202262,4.71153347407 },
//      };
      // @formatter:on
//      Key key = Key.make("test.hex");
//       final int columns = 100;
//       double[][] goals = new double[8][columns];
//       double[][] array = KMeansTest.gauss(columns, 10000, goals);
//      ValueArray va = TestUtil.va_maker(key, (Object[]) array);

//      Key km = Key.make("test.kmeans");
//      int[] cols = new int[va._cols.length];
//      for( int i = 0; i < cols.length; i++ )
//        cols[i] = i;
//      for( int i = 0; i < 1; i++ ) {
//        KMeans job = KMeans.start(km, va, 2, 1e-6, 0, new Random().nextLong(), false, cols);
//        KMeansModel m = job.get();
//        System.out.println(m._error);
//      }
//
//      Key ap = Key.make("test.kmeans-apply");
//      KMeansApply.run(ap, (KMeansModel) UKV.get(km), va);

//    String s = "";
//    for( int i = 0; i < cols.length; i++ ) {
//      s += s.length() != 0 ? "%2C" : "";
//      s += cols[i];
//    }
//    String u = "/Plot.png?source_key=test.hex&cols=" + s + "&clusters=test.kmeans";
//    Desktop.getDesktop().browse(new URI("http://localhost:54321" + u));

      System.out.println("Done!");
    }
  }

  static void covtype() {
    File train = new File("smalldata/covtype/covtype.20k.data");
    Key dest = Key.make("covtype.20k.data.hex");
    Key fkey = water.fvec.NFSFileVec.make(train);
    ParseDataset2.parse(dest, new Key[] { fkey });
    //Frame frame = UKV.get(dest);

//    double[][] rows = new double[(int) frame.numRows()][frame.numCols()];
//    for( int r = 0; r < rows.length; r++ ) {
//      for( int c = 0; c < rows[r].length; c++ ) {
//        rows =
//      }
//    }
  }

  static void mnist() {
    File train = new File("smalldata/mnist/train.csv.gz");
    Key dest = Key.make("train.hex");
    Key fkey = water.fvec.NFSFileVec.make(train);
    water.fvec.ParseDataset2.parse(dest, new Key[] { fkey });

    File test = new File("smalldata/mnist/test.csv.gz");
    dest = Key.make("test.hex");
    fkey = water.fvec.NFSFileVec.make(test);
    water.fvec.ParseDataset2.parse(dest, new Key[] { fkey });
  }

  /**
   * Creates nodes on local machines. For in-process, launch H2O in debug to avoid conflicts with
   * multiple instances of log4j, e.g. "-ea -Xmx12G -Dh2o.debug".
   */
  public static void localCloud(int nodes, boolean inProcess, String[] args) {
    String ip = "127.0.0.1";
    int port = 54321;
    String flat = "";
    for( int i = 0; i < nodes; i++ )
      flat += ip + ":" + (port + i * 2) + '\n';
    String flatfile = Utils.writeFile(flat).getAbsolutePath();
    for( int i = 1; i < nodes; i++ ) {
      String[] a = args(args, ip, (port + i * 2), flatfile);
      Node worker = inProcess ? new NodeCL(a) : new NodeVM(a);
      worker.inheritIO();
      worker.start();
    }
    H2O.main(args(args, ip, port, flatfile));
    TestUtil.stall_till_cloudsize(nodes);
  }

  public static void localMasterRemoteWorkers(String[] workers, String[] args) {
    String local = H2O.SELF_ADDRESS.getHostAddress();
    int port = 54321;
    String flat = local + ":" + port + '\n';
    for( int i = 0; i < workers.length; i++ )
      flat += workers[i] + ":" + port + '\n';
    String flatfile = Utils.writeFile(flat).getAbsolutePath();
    for( int i = 0; i < 0; i++ ) {
      Host host = new Host("192.168.1.15" + (i + 1));
      Node worker = new NodeHost(host, args(args, host.address(), port, flatfile));
      worker.inheritIO();
      worker.start();
    }
    H2O.main(args(args, local, port, flatfile));
    TestUtil.stall_till_cloudsize(1 + workers.length);
  }

  public static void ec2(String[] args) throws Exception {
    EC2 ec2 = new EC2();
    ec2.boxes = 4;
    Cloud c = ec2.resize();
    c.clientRSyncIncludes.add("experiments/target");
    c.clientRSyncIncludes.add("smalldata");
    c.fannedRSyncIncludes.add("smalldata");
    c.jdk = "../libs/jdk";
    String java = "-ea -Xmx12G -Dh2o.debug";
    c.start(java.split(" "), args);
  }

  private static String[] args(String[] args, String ip, int port, String flatfile) {
    return (String[]) ArrayUtils.addAll(args, new String[] { //
        "-ip", ip, "-port", "" + port, "-flatfile", flatfile });
  }
}
