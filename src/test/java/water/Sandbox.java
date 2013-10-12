package water;

import java.io.File;

import org.apache.commons.lang.ArrayUtils;

import water.api.FrameSplit;
import water.deploy.*;
import water.fvec.Frame;
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

      // covtype();

      //Frame frame = water.TestUtil.parseFrame("smalldata/covtype/covtype.20k.data");
      //Frame frame = water.TestUtil.parseFrame("smalldata/categoricals/AllBedrooms_Rent_Neighborhoods.csv.gz");
//      Frame frame = airlines();
//      Frame[] frames = new FrameSplit().splitFrame(frame, new double[] { .8, .1, .1 });
//      UKV.put(Key.make("train.hex"), frames[0]);
//      UKV.put(Key.make("valid.hex"), frames[1]);
//      UKV.put(Key.make("test.hex"), frames[2]);

//    String u = "/Plot.png?source_key=test.hex&cols=" + s + "&clusters=test.kmeans";
//    Desktop.getDesktop().browse(new URI("http://localhost:54321" + u));

      System.out.println("Ready");
    }
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

  public static Frame airlines() {
    Frame frame = water.TestUtil.parseFrame("smalldata/airlines/allyears2k_headers.zip");
    frame.remove("DepTime");
    frame.remove("CRSDepTime");
    frame.remove("ArrTime");
    frame.remove("CRSArrTime");
    frame.remove("ArrDelay");
    frame.remove("DepDelay");
    frame.remove("CarrierDelay");
    frame.remove("WeatherDelay");
    frame.remove("NASDelay");
    frame.remove("SecurityDelay");
    frame.remove("LateAircraftDelay");
    frame.remove("IsArrDelayed");
    return frame;
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
