package water;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;

import water.deploy.*;
import water.parser.ParseDataset;
import water.util.Utils;

public class Sandbox {
  public static void main(String[] args) throws Exception {
    ArrayList<String> l = new ArrayList<String>(Arrays.asList(args));
    l.add(0, "-mainClass");
    l.add(1, UserMain.class.getName());
    Boot._init.boot2(l.toArray(new String[0]));
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      localCloud(3, true, args);

//      ValueArrayToFrameTest.test("smalldata/datagen1.csv");
//      System.out.println("Success!");

      //File f = new File("lib/resources/datasets/gaussian.csv");
      File f = new File("smalldata/Twitter2DB.txt");
      // File f = new File("smalldata/covtype/covtype.20k.data");
      // File f = new File("syn_5853362476331324036_100x11.csv");
      // File f = new File("../../aaaa/datasets/millionx7_logreg.data.gz");
      // File f = new File("smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata");
      // File f = new File("py/testdir_single_jvm/syn_datasets/hastie_4x.data");
      Key dest = Key.make("test.hex");

//      Key fkey = NFSFileVec.make(f);
//      ParseDataset2.parse(dest, new Key[] { fkey });


      Key key = TestUtil.load_test_file(f, "test");
      ParseDataset.parse(dest, new Key[] { key });
      ValueArray va = (ValueArray) UKV.get(dest);

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

    public static void remoteCloud(String[] hosts, String[] java_args, String[] args) {
      Cloud c = new Cloud(hosts, hosts);
      c.start(null, null, java_args, args);
    }

    private static String[] args(String[] args, String ip, int port, String flatfile) {
      return (String[]) ArrayUtils.addAll(args, new String[] { //
          "-ip", ip, "-port", "" + port, "-flatfile", flatfile });
    }
  }
}