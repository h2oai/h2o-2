package samples.launchers;

import water.*;
import water.deploy.Node;
import water.deploy.NodeCL;
import water.util.Utils;

public class CloudProcess {
  /**
   * Experimental configuration where multiple cluster nodes are launched in the same process using
   * separate class loaders. Can simplify distributed debugging, e.g. by allowing cluster wide
   * breakpoints etc.
   */
  public static void main(String[] args) throws Exception {
    launch(null, 3);
  }

  public static void launch(Class<? extends Job> job, int nodes) throws Exception {
    // Additional logging info
    System.setProperty("h2o.debug", "true");
    String ip = "127.0.0.1";
    int port = 54321;
    // Flat file is not necessary, H2O can auto-discover nodes using multi-cast, added
    // here for increased determinism and as a way to get multiple clouds on same box
    String flat = "";
    for( int i = 0; i < nodes; i++ )
      flat += ip + ":" + (port + i * 2) + '\n';
    String flatfile = Utils.writeFile(flat).getAbsolutePath();
    for( int i = 0; i < nodes; i++ ) {
      String[] a = CloudLocal.args(ip, (port + i * 2), flatfile);
      Node node;
      if( i == 0 ) {
        a = Utils.append(a, new String[] { "" + nodes, job != null ? job.getName() : "null" });
        node = new NodeCL(UserCode.class, a);
      } else
        node = new NodeCL(H2O.class, a);
      node.inheritIO();
      node.start();
    }
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);
      int nodes = Integer.parseInt(args[7]);
      String job = args[8];
      TestUtil.stall_till_cloudsize(nodes);
      System.out.println("Cloud is up");
      System.out.println("Go to http://127.0.0.1:54321");

      if( !job.equals("null") ) {
        Class<Job> c = CloudLocal.weaveClass(job);
        c.newInstance().fork();
      }
    }
  }
}
