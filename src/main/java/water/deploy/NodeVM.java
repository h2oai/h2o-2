package water.deploy;

import water.Boot;

/**
 * Creates a node in a VM.
 */
public class NodeVM extends VM implements Node {
  public NodeVM() {
    this(null);
  }

  public NodeVM(String[] args) {
    this(null, args);
  }

  public NodeVM(String[] javaArgs, String[] nodeArgs) {
    super(javaArgs, nodeArgs);
  }

  public static void main(String[] args) throws Exception {
    exitWithParent();
    Boot.main(args);
  }
}