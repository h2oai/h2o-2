package samples;

/**
 * Launches a 1 node cluster.
 */
public class Sample01_1Node {
  public static void main(String[] args) throws Exception {
    water.Boot.main(args);
    System.out.println("Cloud is up");
    System.out.println("Go to http://127.0.0.1:54321");
  }
}
