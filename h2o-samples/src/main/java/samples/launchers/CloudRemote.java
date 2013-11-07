package samples.launchers;

import water.Job;
import water.deploy.*;
import water.util.Log;

/**
 * Builds a remote cluster. H2O jar, or classes from current workspace, are deployed through rsync.
 * <nl>
 * Note: This technique is intended for debug and experimentation purposes only, please refer to the
 * documentation to deploy an H2O cluster.
 */
public class CloudRemote {
  public static void main(String[] args) throws Exception {
    // launchEC2(null);
    launchIPs(null);
  }

  /**
   * Starts EC2 machines and builds a cluster.
   */
  public static void launchEC2(Class<? extends Job> job) throws Exception {
    EC2 ec2 = new EC2();
    ec2.boxes = 4;
    Cloud c = ec2.resize();
    launch(c, job);
  }

  /**
   * The current user is assumed to have ssh access (key-pair, no password) to the remote machines.
   * H2O will be deployed to '~/h2o_rsync/'.
   */
  public static void launchIPs(Class<? extends Job> job) throws Exception {
    Cloud cloud = new Cloud();
    cloud.publicIPs.add("192.168.1.161");
    cloud.publicIPs.add("192.168.1.162");
    cloud.publicIPs.add("192.168.1.163");
    cloud.publicIPs.add("192.168.1.164");
    launch(cloud, job);
  }

  public static void launch(Cloud cloud, Class<? extends Job> job) throws Exception {
    String h2o = VM.h2oFolder().getPath();
    cloud.clientRSyncIncludes.add(h2o + "/target");
    cloud.clientRSyncIncludes.add(h2o + "/h2o-samples/target");
    cloud.clientRSyncIncludes.add(h2o + "/lib");
    cloud.clientRSyncIncludes.add(h2o + "/smalldata");

    // The fanned rsync (between master and slaves) will have the two 'target' merged
    cloud.fannedRSyncIncludes.add("target");
    cloud.fannedRSyncIncludes.add("lib");
    cloud.fannedRSyncIncludes.add("smalldata");

    cloud.clientRSyncExcludes.add("**/h2o.jar");
    cloud.clientRSyncExcludes.add("**/javadoc");
    cloud.clientRSyncExcludes.add("lib/javassist");
    cloud.clientRSyncExcludes.add("**/*-sources.jar");

    String java = "-ea -Xmx8G -Dh2o.debug";
    String node = "-mainClass " + UserCode.class.getName() + " " + (job != null ? job.getName() : null) + " -beta";
    cloud.start(java.split(" "), node.split(" "));
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      Log.info("Java location: " + System.getProperty("java.home"));

      String job = args[0].equals("null") ? null : args[0];
      if( job != null ) {
        Class<Job> c = CloudLocal.weaveClass(job);
        c.newInstance().fork();
      }
    }
  }
}
