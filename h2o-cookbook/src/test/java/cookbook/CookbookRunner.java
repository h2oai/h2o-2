package cookbook;

import java.util.ArrayList;
import java.util.List;

import org.junit.internal.TextListener;
import org.junit.runner.*;
import org.junit.runner.notification.Failure;

import water.H2O;
import water.TestUtil;
import water.deploy.NodeCL;
import water.util.Log;
import water.util.Utils;

/**
 * Run cookbook examples.
 *
 * Runs all the cookbook examples.
 * Does not require any arguments.
 */
public class CookbookRunner {
  /**
   * Number of H2O nodes to start.
   *
   * Note that all nodes are started within one process by this harness.
   * This approach is good for unit testing.  You would not want to deploy to production like this.
   *
   * Each H2O node is wrapped in it's own classloader.  The H2O nodes communicate with each other
   * using sockets even though they all live in the same process.  This gives very realistic test
   * behavior.
   */
  public static final int NODES = 3;

  /**
   * Main program.
   *
   * No args required.  Exit code 0 means all tests passed, nonzero otherwise.
   * Each H2O node needs two ports, n and n+1.  Start assigning ports at 54321.
   *
   * @param args (ignored)
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int[] ports = new int[NODES];
    for( int i = 0; i < ports.length; i++ )
      ports[i] = 54321 + i * 2;

    // Create a flatfile on the fly so nodes can find each other.
    String flat = "";
    for( int i = 0; i < ports.length; i++ )
      flat += "127.0.0.1:" + ports[i] + "\n";
    flat = Utils.writeFile(flat).getAbsolutePath();

    // Start the individual nodes.
    for( int i = 0; i < ports.length; i++ ) {
      Class c;
      if (i == 0) {
        // Choose a node to be special and run the test class code.
        c = UserCode.class;
      }
      else {
        // All other nodes (second and on) need to be started but just join up and form the cloud.
        c = SecondNode.class;
      }
      NodeCL n = new NodeCL(c, ("-ip 127.0.0.1 -port " + ports[i] + " -flatfile " + flat).split(" "));
      n.start();
    }
  }

  /**
   * Start the lead node where the test driver executes.
   */
  public static class UserCode {
    /**
     * Start method called by the new classloader for the lead node.
     * Every H2O node gets its own classloader.
     *
     * @param args Any arguments passed to H2O main for this node.
     */
    public static void userMain(String[] args) {
      H2O.main(args);
      TestUtil.stall_till_cloudsize(NODES);

      //
      // Set up the list of test classes to run.
      //
      List<Class> tests = new ArrayList<Class>();
      // tests.add(cookbook.Cookbook.class);
      tests.add(H2OCookbook.class);
      tests.add(FrameCookbook.class);
      tests.add(VecChunkDemo.class);
      tests.add(KeyDemo.class);
      tests.add(FillNAsWithMeanDemo01.class);
      tests.add(FillNAsWithMeanDemo02.class);
      tests.add(FillNAsWithMeanDemo03.class);
      tests.add(FramDemo.class);
      tests.add(VecDemo.class);
      tests.add(ChunkDemo.class);
      //
      // Run tests.
      //
      JUnitCore junit = new JUnitCore();
      junit.addListener(new LogListener());
      Result result = junit.run(tests.toArray(new Class[0]));

      //
      // Report final result and exit with the correct exit value indicating pass or fail.
      //
      if( result.getFailures().size() == 0 ) {
        Log.info("ALL TESTS PASSED");
        H2O.exit(0);
      }
      else {
        Log.err("SOME TESTS FAILED");
        H2O.exit(1);
      }
    }
  }

  /**
   * Start a second (or beyond) node to form a multinode cloud.
   */
  public static class SecondNode {
    /**
     * Start method called by the new classloader for a non-lead node.
     *
     * @param args Any arguments passed to H2O main for this node.
     */
    public static void userMain(String[] args) {
      H2O.main(args);
    }
  }

  /**
   * Print out information about each test.
   */
  static class LogListener extends TextListener {
    LogListener() {
      super(System.out);
    }

    @Override public void testRunFinished(Result result) {
      printHeader(result.getRunTime());
      printFailures(result);
      printFooter(result);
    }

    @Override public void testStarted(Description description) {
      Log.info("");
      Log.info("Starting test " + description);
    }

    @Override public void testFailure(Failure failure) {
      Log.info("Test failed " + failure);
    }

    @Override public void testIgnored(Description description) {
      Log.info("Ignoring test " + description);
    }
  }
}
