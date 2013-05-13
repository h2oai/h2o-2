package water;

import org.apache.commons.lang.ArrayUtils;

import water.deploy.*;
import water.r.Shell;
import water.util.Utils;

public class Sandbox {
  public static void main(String[] args) throws Exception {
    localCloud(1, true, args);

    final String r = "" //
        + "a=load('smalldata/covtype/covtype.20k.data')\n" // ;
        + "b=parse(destination_key='covtype.hex', source_key='covtype.20k.data')\n" //
        + "c=kmeans(k='3', destination_key='covtype.kmeans', source_key='covtype.hex', cols='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54', epsilon='1.0E-2')\n";

    Shell.run(new String[] { "-f", Utils.writeFile(r).getAbsolutePath(), "--interactive" });

    // File f = new File("smalldata/gaussian/sdss174052.csv.gz");
    // File f = new File("smalldata/covtype/covtype.20k.data");
    // // File f = new File("../../aaaa/datasets/millionx7_logreg.data.gz");
    // // File f = new File("smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata");
    // // File f = new File("py/testdir_single_jvm/syn_datasets/hastie_4x.data");
    // Key key = TestUtil.load_test_file(f, "test");
    // Key dest = Key.make("test.hex");
    // ParseDataset.parse(dest, new Key[] { key });
    // ValueArray va = (ValueArray) UKV.get(dest);

    // Key key = Key.make("test.hex");
    // final int columns = 100;
    // double[][] goals = new double[8][columns];
    // double[][] array = KMeansTest.gauss(columns, 10000, goals);
    // ValueArray va = TestUtil.va_maker(key, (Object[]) array);
    //
    // Key km = Key.make("test.kmeans");
    // int[] cols = new int[va._cols.length];
    // for( int i = 0; i < cols.length; i++ )
    // cols[i] = i;
    // KMeans.run(km, va, 5, 1e-3, cols);

    // String u = "/Plot.png?source_key=test.kmeans&cols=0%2C1"
    // Desktop.getDesktop().browse(new URI("http://localhost:54321" + u));
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
    String local = H2O.findInetAddressForSelf().getHostAddress();
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
