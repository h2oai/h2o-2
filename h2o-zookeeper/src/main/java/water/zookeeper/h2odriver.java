package water.zookeeper;

import org.apache.zookeeper.*;
import water.zookeeper.nodes.ClusterPayload;
import water.zookeeper.nodes.MasterPayload;

import java.util.concurrent.TimeoutException;

public class h2odriver {
  final static int CLOUD_FORMATION_SETTLE_DOWN_SECONDS = 2;

  // Used by the running object.
  private String _zk;
  private String _zkroot;
  private int _numNodes;
  private int _cloudFormationTimeoutSeconds;

  // Used by parseArgs, not by the running object.
  public static String g_zk = "";
  public static String g_zkroot = "";
  public static int g_numNodes = -1;
  public static int g_cloudFormationTimeoutSeconds = Constants.DEFAULT_CLOUD_FORMATION_TIMEOUT_SECONDS;
  public static boolean g_start = false;
  public static boolean g_wait = false;

  public void setZk(String v) {
    _zk = v;
  }

  public void setZkroot(String v) {
    _zkroot = v;
  }

  public void setNumNodes(int v) {
    _numNodes = v;
  }

  public void setCloudFormationTimeoutSeconds(int v) {
    _cloudFormationTimeoutSeconds = v;
  }

  public void doStart() throws Exception {
    ZooKeeper z = ZooKeeperFactory.makeZk(_zk);
    ClusterPayload cp = new ClusterPayload();
    cp.numNodes = _numNodes;
    byte[] payload = cp.toPayload();
    z.create(_zkroot, payload, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    z.create(_zkroot + "/nodes", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    z.close();
  }

  public MasterPayload doWait() throws Exception {
    ZooKeeper z = ZooKeeperFactory.makeZk(_zk);
    byte[] payload;
    payload = z.getData(_zkroot, null, null);
    ClusterPayload cp = ClusterPayload.fromPayload(payload, ClusterPayload.class);
    z.close();
    assert (cp.numNodes > 0);

    long startMillis = System.currentTimeMillis();
    while (true) {
      z = ZooKeeperFactory.makeZk(_zk);
      if (z.exists(_zkroot, false) == null) {
        z.close();
        throw new Exception("ZooKeeper node does not exist: " + _zkroot);
      }

      try {
        payload = z.getData(_zkroot + "/master", null, null);
        MasterPayload mp = MasterPayload.fromPayload(payload, MasterPayload.class);
        z.close();
        Thread.sleep(CLOUD_FORMATION_SETTLE_DOWN_SECONDS);
        return mp;
      }
      catch (KeeperException.NoNodeException e) {
        // This is OK, do nothing
      }

      long now = System.currentTimeMillis();
      if (Math.abs(now - startMillis) > (_cloudFormationTimeoutSeconds * 1000)) {
        z.close();
        throw new TimeoutException("Timed out waiting for cloud to form");
      }

      Thread.sleep(1000);
    }
  }

  /**
   * Print usage and exit 1.
   */
  static void usage() {
    System.err.printf("" +
                    "Step 1: Create a new Zookeeper h2o cloud hierarchy:\n" +
                    "    java -cp h2o-zookeeper.jar water.zookeeper.h2odriver -zk a:b:c:d:e -zkroot /zk/path/h2o-uuid -n <numNodes> -start\n" +
                    "\n" +
                    "Step 2: Wait for an h2o cloud to come up:\n" +
                    "    java -cp h2o-zookeeper.jar water.zookeeper.h2odriver -zk a:b:c:d:e -zkroot /zk/path/h2o-uuid -wait [-timeout sec]\n" +
                    "\n" +
                    "Exit value:\n" +
                    "          0 for success; nonzero otherwise.\n" +
                    "\n"
    );

    System.exit(1);
  }

  /**
   * Print an error message, print usage, and exit 1.
   * @param s Error message
   */
  static void error(String s) {
    System.err.printf("\nERROR: " + "%s\n\n", s);
    usage();
  }

  /**
   * Parse remaining arguments after the ToolRunner args have already been removed.
   * @param args Argument list
   */
  static void parseArgs(String[] args) {
    int i = 0;
    while (true) {
      if (i >= args.length) {
        break;
      }

      String s = args[i];
      if (s.equals("-h") ||
              s.equals("help") ||
              s.equals("-help") ||
              s.equals("--help")) {
        usage();
      }
      else if (s.equals("-zk")) {
        i++; if (i >= args.length) { usage(); }
        g_zk = args[i];
      }
      else if (s.equals("-zkroot")) {
        i++; if (i >= args.length) { usage(); }
        g_zkroot = args[i];
      }
      else if (s.equals("-n") ||
              s.equals("-nodes")) {
        i++; if (i >= args.length) { usage(); }
        g_numNodes = Integer.parseInt(args[i]);
      }
      else if (s.equals("-timeout")) {
        i++; if (i >= args.length) { usage(); }
        g_cloudFormationTimeoutSeconds = Integer.parseInt(args[i]);
      }
      else if (s.equals("-start")) {
        g_start = true;
      }
      else if (s.equals("-wait")) {
        g_wait = true;
      }
      else {
        error("Unrecognized option " + s);
      }

      i++;
    }

    // Check for mandatory arguments.
    if (g_start) {
      if (g_numNodes < 1) {
        error("Number of H2O nodes must be greater than 0 (must specify -n)");
      }
    }

    if (g_wait) {
      if (g_numNodes != -1) {
        error("-nodes option may not be combined with -wait option");
      }
    }

    if (!g_start && !g_wait) {
      error("-start or -wait must be specified");
    }
  }

  public static void main(String[] args) throws Exception {
    parseArgs(args);

    if (g_start) {
      h2odriver d = new h2odriver();
      d.setZk(g_zk);
      d.setZkroot(g_zkroot);
      d.setNumNodes(g_numNodes);
      d.doStart();
    }
    else if (g_wait) {
      h2odriver d = new h2odriver();
      d.setZk(g_zk);
      d.setZkroot(g_zkroot);
      d.setCloudFormationTimeoutSeconds(g_cloudFormationTimeoutSeconds);
      MasterPayload mp = d.doWait();
      System.out.println(mp.ip + ":" + mp.port);
    }
    else {
      System.out.println("bad path");
      System.exit(1);
    }
  }
}
