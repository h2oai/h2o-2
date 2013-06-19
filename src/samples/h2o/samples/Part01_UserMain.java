package h2o.samples;

import water.H2O;
import water.Weaver;

/**
 * Shows how to run your own main method. H2O needs to run in a separate class loader, so running
 * your own code requires using H2O's '-mainClass' parameter.
 */
public class Part01_UserMain {
  public static void main(String[] args) throws Exception {
    // Ignore, temporary boilerplate
    Weaver.registerPackage("h2o.samples");
    water.Boot._init.boot(new String[] { "-mainClass", UserMain.class.getName() });
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      /*
       * Starts an H2O instance in the local JVM. This method is the one invoked by default when no
       * '-mainClass' parameter is specified.
       */
      H2O.main(args);
      System.out.println("Cloud is up");
      System.out.println("Go to http://127.0.0.1:54321");
    }
  }
}
