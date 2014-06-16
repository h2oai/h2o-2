package water.zookeeper;

import java.io.*;
import java.net.*;

import water.H2O;


public class h2oworker {
  static EmbeddedH2OConfig _embeddedH2OConfig;

  /**
   * Start an H2O instance in the local JVM.
   */
  public static class UserMain {
    private static void usage() {
      // TODO
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

    void setZk(String value) {
      _zk = value;
    }

    void setZkRoot(String value) {
      _zkroot = value;
    }

    @Override
    public void notifyAboutEmbeddedWebServerIpPort (InetAddress ip, int port) {
      _embeddedWebServerIp = ip.getHostAddress();
      _embeddedWebServerPort = port;
    }

    @Override
    public boolean providesFlatfile() {
      return true;
    }

    @Override
    public String fetchFlatfile() throws Exception {
      // TODO
      String flatfile = "";

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
