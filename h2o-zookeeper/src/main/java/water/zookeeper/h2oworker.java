package water.zookeeper;

import java.io.*;
import java.net.*;

import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import water.H2O;
import water.zookeeper.nodes.ClusterPayload;
import water.zookeeper.nodes.MasterPayload;
import water.zookeeper.nodes.WorkerPayload;


public class h2oworker {
  static EmbeddedH2OConfig _embeddedH2OConfig;

  /**
   * Start an H2O instance in the local JVM.
   */
  public static class UserMain {
    private static void usage() {
      String s =
      "H2O Zookeeper worker wrapper options:\n" +
      "\n" +
      "Usage:  java [-Xmx<size>] -cp h2o-zookeeper.jar water.zookeeper.h2oworker -zk a:b:c:d:e -zkroot /zk/path/h2o-uuid [h2o options...]\n" +
      "\n";

      System.out.println(s);
      H2O.printHelp();

      System.exit(1);
    }

    private static void registerEmbeddedH2OConfig(String[] args) {
      String zk = null;
      String zkroot = null;

      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-zk")) {
          i++;
          zk = args[i];
        }
        else if (args[i].equals("-zkroot")) {
          i++;
          zkroot = args[i];
        }
      }

      if (zk == null) {
        System.out.println("\nERROR: -zk must be specified\n");
        usage();
      }

      if (zkroot == null) {
        System.out.println("\nERROR: -zkroot must be specified\n");
        usage();
      }

      _embeddedH2OConfig = new EmbeddedH2OConfig();
      _embeddedH2OConfig.setZk(zk);
      _embeddedH2OConfig.setZkRoot(zkroot);
      H2O.setEmbeddedH2OConfig(_embeddedH2OConfig);
    }

    public static void main(String[] args) {
      registerEmbeddedH2OConfig(args);
      H2O.main(args);
    }
  }

  private static class EmbeddedH2OConfig extends water.AbstractEmbeddedH2OConfig {
    volatile String _zk;
    volatile String _zkroot;
    volatile String _embeddedWebServerIp = "(Unknown)";
    volatile int _embeddedWebServerPort = -1;
    volatile int _numNodes = -1;

    void setZk(String value) {
      _zk = value;
    }

    void setZkRoot(String value) {
      _zkroot = value;
    }

    private static long getProcessId() throws Exception {
      // Note: may fail in some JVM implementations
      // therefore fallback has to be provided

      // something like '<pid>@<hostname>', at least in SUN / Oracle JVMs
      final String jvmName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
      final int index = jvmName.indexOf('@');

      if (index < 1) {
        // part before '@' empty (index = 0) / '@' not found (index = -1)
        throw new Exception ("Can't get process Id");
      }

      return Long.parseLong(jvmName.substring(0, index));
    }

    @Override
    public void notifyAboutEmbeddedWebServerIpPort (InetAddress ip, int port) {
      _embeddedWebServerIp = ip.getHostAddress();
      _embeddedWebServerPort = port;

      try {
        int sessionTimeoutMillis = Constants.SESSION_TIMEOUT_MILLIS;
        ZooKeeper z = new ZooKeeper(_zk, sessionTimeoutMillis, null);
        byte[] payload;
        payload = z.getData(_zkroot, null, null);
        ClusterPayload cp = ClusterPayload.fromPayload(payload, ClusterPayload.class);
        _numNodes = cp.numNodes;
        if (_numNodes <= 0) {
          System.out.println("ERROR: numNodes must be > 0 (" + _numNodes + ")");
          System.exit(1);
        }

        WorkerPayload wp = new WorkerPayload();
        wp.ip = ip.getHostAddress();
        wp.port = port;
        wp.pid = getProcessId();
        payload = cp.toPayload();
        z.create(_zkroot + "/nodes/", payload, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
      }
      catch (Exception e) {
        System.out.println(e);
        System.exit(1);
      }
    }

    @Override
    public boolean providesFlatfile() {
      return true;
    }

    @Override
    public String fetchFlatfile() throws Exception {
      System.out.printf("EmbeddedH2OConfig: fetchFlatfile called\n");

      int sessionTimeoutMillis = Constants.SESSION_TIMEOUT_MILLIS;
      ZooKeeper z = new ZooKeeper(_zk, sessionTimeoutMillis, null);
      String flatfile = "";

      while (true) {
        List<String> list = z.getChildren(_zkroot + "/nodes", true);

        if (list.size() == _numNodes) {
          for (String child : list) {
             System.out.println("TOM CHILD " + child);
          }
          System.out.println("TOM EXITING");
          System.exit(1);
          break;
        }
        else if (list.size() > _numNodes) {
          System.out.println("EmbeddedH2OConfig: fetchFlatfile sees too many nodes (" + list.size() + " > " + _numNodes + ")");
          System.exit(1);
        }

        Thread.sleep(1000);
      }

      System.out.printf("EmbeddedH2OConfig: fetchFlatfile returned\n");
      System.out.println("------------------------------------------------------------");
      System.out.println(flatfile);
      System.out.println("------------------------------------------------------------");
      return flatfile;
    }

    @Override
    public void notifyAboutCloudSize (InetAddress ip, int port, int size) {
      _embeddedWebServerIp = ip.getHostAddress();
      _embeddedWebServerPort = port;

      System.out.printf("EmbeddedH2OConfig: notifyAboutCloudSize called (%s, %d, %d)\n", ip.getHostAddress(), port, size);
      if (size == _numNodes) {
        System.out.printf("EmbeddedH2OConfig: notifyAboutCloudSize claiming master...\n");
        try {
          int sessionTimeoutMillis = Constants.SESSION_TIMEOUT_MILLIS;
          ZooKeeper z = new ZooKeeper(_zk, sessionTimeoutMillis, null);
          MasterPayload mp = new MasterPayload();
          mp.ip = ip.getHostAddress();
          mp.port = port;
          mp.pid = getProcessId();
          byte[] payload;
          payload = mp.toPayload();
          z.create(_zkroot + "/master", payload, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          System.out.printf("EmbeddedH2OConfig: notifyAboutCloudSize claimed master\n");
        }
        catch (KeeperException.NodeExistsException e) {
          System.out.printf("EmbeddedH2OConfig: notifyAboutCloudSize lost claiming race to another node (this is normal)\n");
        }
        catch (Exception e) {
          System.out.println(e);
          System.exit(1);
        }
      }
    }

    @Override
    public void exit(int status) {
      System.out.printf("EmbeddedH2OConfig: exit called (%d)\n", status);
      System.exit(status);
    }

    @Override
    public void print() {
      System.out.println("EmbeddedH2OConfig print()");
    }
  }

  public void run(String[] args) throws IOException, InterruptedException {
    try {
      water.Boot.main(UserMain.class, args);
    }
    catch (Exception e) {
      System.out.println(e);
      System.exit(1);
    }
  }

  public static void main (String[] args) {
    try {
      h2oworker m = new h2oworker();
      m.run(args);
    }
    catch (Exception e) {
      System.out.println (e);
    }
  }
}
