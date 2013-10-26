package samples;

import water.H2O;
import water.Job;

public class CloudLocal1 {
  /**
   * Launches a 1 node cluster. You might want to increase the JVM heap, e.g. -Xmx12G.
   */
  public static void main(String[] args) throws Exception {
    water.Boot.main(args);
    System.out.println("Cloud is up");
    System.out.println("Go to http://127.0.0.1:54321");
  }

  /**
   * Launches a 1 node cluster and runs a job on it.
   */
  public static void launch(Class<? extends Job> job) throws Exception {
    water.Boot.main(UserCode.class, new String[] { job.getName() });
  }

  /**
   * H2O needs to run in a separate class loader, so running custom code with H2O requires using a
   * separate class, invoked by Boot. (Same as using the '-mainClass' parameter when running from
   * command line).
   */
  public static class UserCode {
    // Entry point can be called 'main' or 'userMain'
    public static void userMain(String[] args) throws Exception {
      // Starts an H2O instance in the current class loader
      H2O.main(args);

      // Create and launch specified job
      Job job = (Job) Class.forName(args[0]).newInstance();
      job.fork();
    }
  }
}
