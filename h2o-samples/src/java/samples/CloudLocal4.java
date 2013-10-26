package samples;

import water.*;
import water.deploy.*;
import water.util.Utils;

/**
 * Creates a 4 nodes cluster on the local machine.
 */
public class CloudLocal4 {
  public static void main(String[] args) throws Exception {
    launch(4, true, null);
  }

  /**
   * @param nodes
   * @param inProcess
   *          True: launches all nodes in current process using separate class loaders. Can simplify
   *          distributed debugging, e.g. by allowing cluster wide breakpoints etc.<br>
   *          False: Use current process for the first node, and launches JVMs for the others.
   * @param job
   *          Optional job to launch once the cluster is ready.
   */
  public static void launch(int nodes, boolean inProcess, Class<? extends Job> job) throws Exception {
    // Additional logging info, and avoids issues like multiple log4j instances for inProcess
    System.setProperty("h2o.debug", "true");
    Boot.main(UserCode.class, new String[] { "" + nodes, "" + inProcess, job != null ? job.getName() : "null" });
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      int nodes = Integer.parseInt(args[0]);
      boolean inProcess = args[1].equals("true");
      String ip = "127.0.0.1";
      int port = 54321;
      // Flat file is not necessary, H2O can auto-discover nodes using multi-cast,
      // added here for increased determinism and to allow multiple clouds on same box
      String flat = "";
      for( int i = 0; i < nodes; i++ )
        flat += ip + ":" + (port + i * 2) + '\n';
      String flatfile = Utils.writeFile(flat).getAbsolutePath();
      for( int i = 1; i < nodes; i++ ) {
        String[] a = args(ip, (port + i * 2), flatfile);
        Node worker = inProcess ? new NodeCL(a) : new NodeVM(a);
        worker.inheritIO();
        worker.start();
      }
      H2O.main(args(ip, port, flatfile));
      TestUtil.stall_till_cloudsize(nodes);
      System.out.println("Cloud is up");
      System.out.println("Go to http://127.0.0.1:54321");

      Class<Job> job = args[2].equals("null") ? null : (Class) Class.forName(args[2]);
      if( job != null )
        job.newInstance().fork();
    }

    private static String[] args(String ip, int port, String flatfile) {
      return new String[] { "-ip", ip, "-port", "" + port, "-flatfile", flatfile };
    }
  }
}
