package samples;

import water.deploy.Node;
import water.deploy.NodeVM;

/**
 * Creates a cluster on the local machine by launching JVMs.
 * If you are running this from inside an IDE, some require running with the debugger enabled for this to work properly.
 */
public class Sample02_4Nodes {
  public static void main(String[] args) throws Exception {
    // Creates 3 other nodes on the local machine
    for( int i = 0; i < 3; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    water.Boot.main(args);

    System.out.println("");
    System.out.println("");
    System.out.println("Go to http://127.0.0.1:54321");
    System.out.println("");
    System.out.println("");
  }
}
