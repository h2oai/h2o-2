package samples;

import water.*;
import water.deploy.Cloud;
import water.deploy.EC2;
import water.util.Log;

/**
 * Builds a remote cluster. H2O jar, or classes from current workspace, are deployed through rsync.
 * <nl>
 * Note: This technique is intended for debug and experimentation purposes only, please refer to the
 * documentation to deploy an H2O cluster.
 */
public class CloudRemote {
  public static void main(String[] args) throws Exception {
    launchEC2(null);
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
    if( Boot._init.fromJar() )
      cloud.clientRSyncIncludes.add("target/h2o.jar");
    else {
      cloud.clientRSyncIncludes.add("target");
      cloud.clientRSyncIncludes.add("lib");
    }
    cloud.fannedRSyncIncludes.addAll(cloud.clientRSyncIncludes);

    cloud.clientRSyncIncludes.add("h2o-samples/target");
    cloud.clientRSyncIncludes.add("../smalldata");
    cloud.fannedRSyncIncludes.add("smalldata");

    if( !Boot._init.fromJar() ) {
      cloud.clientRSyncExcludes.add("target/*.jar");
      cloud.clientRSyncExcludes.add("target/javadoc/**");
      cloud.clientRSyncExcludes.add("lib/javassist");
      cloud.clientRSyncExcludes.add("**/*-sources.jar");
    }

    cloud.jdk = "../libs/jdk";
    String java = "-ea -Xmx120G -Dh2o.debug";
    String node = "-mainClass " + UserCode.class.getName() + " " + (job != null ? job.getName() : null);
    cloud.start(java.split(" "), node.split(" "));
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);
      Log.info("java: " + System.getProperty("java.home"));
      TestUtil.stall_till_cloudsize(H2O.STATIC_H2OS.size());

      System.out.println("Cloud is up, local port 54321 forwarded");
      System.out.println("Go to http://127.0.0.1:54321");

      Class<Job> job = args[2].equals("null") ? null : (Class) Class.forName(args[2]);
      if( job != null )
        job.newInstance().fork();
    }
  }
}
