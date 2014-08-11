package water.zookeeper;

import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperFactory {
  public static ZooKeeper makeZk(String zk) throws Exception {
    ZooKeeper z = new ZooKeeper(zk, Constants.SESSION_TIMEOUT_MILLIS, null);

    int trial = 0;
    while (true) {
      trial++;
      if (trial > 10) {
        throw new Exception("ZooKeeper tried too many times to reach CONNECTED state");
      }

      if (z.getState() == ZooKeeper.States.CONNECTED) {
        return z;
      }

      Thread.sleep(1000);
    }
  }
}
