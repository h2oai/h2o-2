package samples;

import water.H2O;

/**
 * H2O needs to run in a separate class loader, so running your own code with H2O
 * requires using a separate class, invoked by Boot. (Same as using the '-mainClass'
 * parameter when running from command line).
 */
public class Sample03_UserCode {
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, args);
  }

  public static class UserCode {
    // Entry point can be called 'main' or 'userMain'
    public static void userMain(String[] args) throws Exception {
      // Starts an H2O instance in the current class loader
      H2O.main(args);

      // Your code runs in the same class loader, so it can access H2O state
      System.out.println("Cloud has " + H2O.CLOUD.size() + " node.");
      System.exit(0);
    }
  }
}
