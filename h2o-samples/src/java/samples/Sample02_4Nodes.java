package water;

import water.deploy.Node;
import water.deploy.NodeVM;

/**
 * Creates a cluster on the local machine by launching JVMs.
 */
public class Sample02_4Nodes {
  public static void main(String[] args) throws Exception {
    // Creates 3 other nodes on the local machine
    for( int i = 0; i < 3; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    Boot.main(args);
  }
}
