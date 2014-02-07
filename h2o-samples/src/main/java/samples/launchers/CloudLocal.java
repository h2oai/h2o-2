package samples.launchers;

import water.*;
import water.deploy.*;
import water.util.Utils;

public class CloudLocal {
  /**
   * Launches a 1 node cluster. You might want to increase the JVM heap, e.g. -Xmx12G.
   */
  public static void main(String[] args) throws Exception {
    launch(null, 1);
  }

  /**
   * Launches a local multi-nodes cluster by spawning additional JVMs. JVM parameters and classpath
   * are replicated from the current one.
   */
  public static void launch(Class<? extends Job> job, int nodes) throws Exception {
    // Additional logging info
    System.setProperty("h2o.debug", "true");
    Boot.main(UserCode.class, new String[] { "" + nodes, job != null ? job.getName() : "null" });
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      int nodes = Integer.parseInt(args[0]);
      String ip = "127.0.0.1";
      int port = 54321;
      // Flat file is not necessary, H2O can auto-discover nodes using multi-cast, added
      // here for increased determinism and as a way to get multiple clouds on same box
      String flat = "";
      for( int i = 0; i < nodes; i++ )
        flat += ip + ":" + (port + i * 2) + '\n';
      String flatfile = Utils.writeFile(flat).getAbsolutePath();
      for( int i = 1; i < nodes; i++ ) {
        String[] a = args(ip, (port + i * 2), flatfile);
        Node worker = new NodeVM(a);
        worker.inheritIO();
        worker.start();
      }
      H2O.main(args(ip, port, flatfile));
      TestUtil.stall_till_cloudsize(nodes);
      System.out.println("");
      System.out.println("Cloud is up");
      System.out.println("Go to http://127.0.0.1:54321");
      System.out.println("");

      if( !args[1].equals("null") ) {
        Class<Job> job = weaveClass(args[1]);
        job.newInstance().fork();
      }
    }
  }

  static Class weaveClass(String name) throws Exception {
    String pack = name.substring(0, name.lastIndexOf('.'));
    LaunchJar.weavePackages(pack);
    return Class.forName(name);
  }

  static String[] args(String ip, int port, String flatfile) {
    return new String[] { "-ip", ip, "-port", "" + port, "-flatfile", flatfile, "-beta" };
  }
}
