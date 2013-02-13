package water.util;

/**
 * Runs TestRunner on a remote machine.
 */
public class TestRunnerRemote {
  public static void main(String[] _) throws Exception {
    String[] args = new String[] { "init.Boot", "-mainClass", "test.TestRunner" };
    SeparateBox box = new SeparateBox("192.168.1.150", TestRunner.USER, TestRunner.KEY, args);
    box.waitForEnd();
  }
}
