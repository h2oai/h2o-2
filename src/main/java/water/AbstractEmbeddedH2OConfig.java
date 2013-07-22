package water;

import java.net.InetAddress;

/**
 * This class is a small shim between a main java program (such as a
 * Hadoop mapper) and an embedded full-capability H2O.
 */
public abstract class AbstractEmbeddedH2OConfig {
  /**
   * Tell the embedding software that H2O has started an embedded
   * web server on an IP and port.
   *
   * @param ip IP address this H2O can be reached at.
   * @param port Port this H2O can be reached at (for REST API and browser).
   */
  public abstract void notifyAboutEmbeddedWebServerIpPort(InetAddress ip, int port);

  /**
   * Tell the embedding software that this H2O instance belongs to
   * a cloud of a certain size.
   *
   * @param ip IP address this H2O can be reached at.
   * @param port Port this H2O can be reached at (for REST API and browser).
   * @param size Number of H2O instances in the cloud.
   */
  public abstract void notifyAboutCloudSize(InetAddress ip, int port, int size);

  /**
   * Tell the embedding software that H2O wants the process to exit.
   * This should not return.  The embedding software should do any
   * required cleanup and then call exit with the status.
   *
   * @param status Process-level exit status
   */
  public abstract void exit (int status);

  /**
   * Print debug information.
   */
  public abstract void print();
}
