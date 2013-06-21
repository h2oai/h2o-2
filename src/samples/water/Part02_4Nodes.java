package water;

import water.deploy.Node;
import water.deploy.NodeVM;

/**
 * Creates a cluster on the local machine by launching additional JVMs.
 */
public class Part02_4Nodes {
  // Ignore this boilerplate main, c.f. previous samples
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserMain.class, args);
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      // Creates 3 other nodes on the local machine
      for( int i = 0; i < 3; i++ ) {
        Node n = new NodeVM(args);
        n.inheritIO();
        n.start();
      }
      H2O.main(args);
      H2O.waitForCloudSize(4);

      System.out.println("Cloud is up");
      System.out.println("Four machines should show Cloud Status: http://127.0.0.1:54321/Cloud.html");
    }
  }
}
