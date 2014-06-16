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
    private static void registerEmbeddedH2OConfig(String[] args) {
      String ip = null;
      int port = -1;
      int mport = -1;

      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-driverip")) {
          i++;
          ip = args[i];
        }
        else if (args[i].equals("-driverport")) {
          i++;
          port = Integer.parseInt(args[i]);
        }
        else if (args[i].equals("-mapperport")) {
          i++;
          mport = Integer.parseInt(args[i]);
        }
      }

      _embeddedH2OConfig = new EmbeddedH2OConfig();
      _embeddedH2OConfig.setDriverCallbackIp(ip);
      _embeddedH2OConfig.setDriverCallbackPort(port);
      _embeddedH2OConfig.setMapperCallbackPort(mport);
      H2O.setEmbeddedH2OConfig(_embeddedH2OConfig);
    }

    public static void main(String[] args) {
      registerEmbeddedH2OConfig(args);
      H2O.main(args);
    }
  }

  private static class EmbeddedH2OConfig extends water.AbstractEmbeddedH2OConfig {
    volatile String _driverCallbackIp;
    volatile int _driverCallbackPort = -1;
    volatile int _mapperCallbackPort = -1;
    volatile String _embeddedWebServerIp = "(Unknown)";
    volatile int _embeddedWebServerPort = -1;

    void setDriverCallbackIp(String value) {
      _driverCallbackIp = value;
    }

    void setDriverCallbackPort(int value) {
      _driverCallbackPort = value;
    }

    void setMapperCallbackPort(int value) {
      _mapperCallbackPort = value;
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
      // TODO
      water.Boot.main(UserMain.class, args);
    }
    catch (Exception e) {
      System.out.println(e);
      System.exit(1);
    }
  }

  /**
   * For debugging only.
   */
  public static void main (String[] args) {
    try {
      h2oworker m = new h2oworker();
      m.run(null);
    }
    catch (Exception e) {
      System.out.println (e);
    }
  }
}
