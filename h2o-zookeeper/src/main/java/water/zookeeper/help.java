package water.zookeeper;

public class help {
  public static void main(String[] args) {
    String s =
            "\n" +
            "H2O Zookeeper wrapper.\n" +
            "\n" +
            "Usage:  java [-Xmx<size>] -cp h2o-zookeeper.jar water.zookeeper.h2odriver\n" +
            "            OR\n" +
            "        java [-Xmx<size>] -cp h2o-zookeeper.jar water.zookeeper.h2oworker\n"
            ;

    System.out.println(s);
    System.exit(1);
  }
}
