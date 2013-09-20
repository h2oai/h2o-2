package water.deploy;

import java.io.*;
import java.util.*;

import org.apache.commons.codec.binary.Base64;

import water.H2O;
import water.util.Log;
import water.util.Utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;

/**
 * Runs EC2 instances. This class is intended for debug purposes only. The recommended
 * infrastructure for H2O's deployment on AWS is documented here:<br>
 * https://github.com/0xdata/h2o/wiki/How-to-run-H2O-tests-on-EC2-clusters
 */
public class EC2 {
  private static final String USER = System.getProperty("user.name");
  private static final String NAME = USER + "-H2O-Cloud";

  /**
   * AWS credentials file with format: <br>
   * accessKey=XXXXXXXXXXXXXXXXXXXX <br>
   * secretKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
   */
  public String AWSCredentials = H2O.DEFAULT_CREDENTIALS_LOCATION;
  public int boxCount;
  public String region = "us-east-1";
  public String image = "ami-e1357b88"; // Ubuntu Raring 13.04 amd64
  public String type = "m1.xlarge";
  public String securityGroup = "default";
  public boolean confirm = true;
  public Set<String> rsyncIncludes = new HashSet<String>();
  public Set<String> rsyncExcludes = new HashSet<String>();
  public String[] javaArgs;
  public String[] userArgs;

//@formatter:off
  static String cloudConfig = "" +
      l("#cloud-config") +
      l("users:") +
      l("  - name: " + USER) +
      l("    sudo: ALL=(ALL) NOPASSWD:ALL") +
      l("    ssh-authorized-keys:") +
      l("      - " + pubKey()) +
      l("    shell: /bin/bash") +
      l("") +
      l("runcmd:") +
      l("  - iptables -I INPUT -p tcp --dport 22 -j DROP") +
      l("  - echo 'fs.file-max = 524288' > /etc/sysctl.d/increase-max-fd.conf") +
      l("  - sysctl -w fs.file-max=524288") +
      l("  - echo '* soft nofile 524288' > /etc/security/limits.d/increase-max-fd-soft.conf") +
      l("  - echo '* hard nofile 524288' > /etc/security/limits.d/increase-max-fd-hard.conf") +
//      l("  - apt-get update") +
//      l("  - apt-get -y install openjdk-7-jdk") +
//      l("  - apt-get -y install openvpn") +
      l("  - iptables -D INPUT 1") +
      l("");
  static String l(String line) { return line + "\n"; }
  //@formatter:on

  public void run() throws Exception {
    Cloud c = resize();
    c._clientRSyncIncludes.addAll(rsyncIncludes);
    c._clientRSyncExcludes.addAll(rsyncExcludes);
    c.start(javaArgs, userArgs);
  }

  /**
   * Create or terminate EC2 instances. Uses their Name tag to find existing ones.
   */
  public Cloud resize() throws Exception {
    AmazonEC2Client ec2 = new AmazonEC2Client(H2O.getAWSCredentials());
    ec2.setEndpoint("ec2." + region + ".amazonaws.com");
    DescribeInstancesResult describeInstancesResult = ec2.describeInstances();
    List<Reservation> reservations = describeInstancesResult.getReservations();
    List<Instance> instances = new ArrayList<Instance>();

    for( Reservation reservation : reservations ) {
      for( Instance instance : reservation.getInstances() ) {
        String ip = ip(instance);
        if( ip != null ) {
          String name = null;
          if( instance.getTags().size() > 0 )
            name = instance.getTags().get(0).getValue();
          if( NAME.equals(name) )
            instances.add(instance);
        }
      }
    }
    System.out.println("Found " + instances.size() + " EC2 instances for user " + USER);

    if( instances.size() > boxCount ) {
      for( int i = 0; i < instances.size() - boxCount; i++ ) {
        // TODO terminate
      }
    } else if( instances.size() < boxCount ) {
      int launchCount = boxCount - instances.size();
      System.out.println("Creating " + launchCount + " EC2 instances.");
      if( confirm ) {
        System.out.println("Please confirm [y/n]");
        String s = Utils.readConsole();
        if( s == null || !s.equalsIgnoreCase("y") )
          throw new Exception("Aborted");
      }

      RunInstancesRequest request = new RunInstancesRequest();
      request.withInstanceType(type);
      request.withImageId(image);
      request.withMinCount(launchCount).withMaxCount(launchCount);
      request.withSecurityGroupIds(securityGroup);
      request.withUserData(new String(Base64.encodeBase64(cloudConfig.getBytes())));

      RunInstancesResult runInstances = ec2.runInstances(request);
      ArrayList<String> ids = new ArrayList<String>();
      for( Instance instance : runInstances.getReservation().getInstances() )
        ids.add(instance.getInstanceId());

      List<Instance> created = wait(ec2, ids);
      System.out.println("Created " + created.size() + " EC2 instances.");
      instances.addAll(created);
    }

    String[] pub = new String[instances.size()];
    String[] prv = new String[instances.size()];
    for( int i = 0; i < instances.size(); i++ ) {
      pub[i] = instances.get(i).getPublicIpAddress();
      prv[i] = instances.get(i).getPrivateIpAddress();
    }
    System.out.println("EC2 public IPs: " + Utils.join(' ', pub));
    System.out.println("EC2 private IPs: " + Utils.join(' ', prv));
    Cloud cloud = new Cloud();
    cloud._publicIPs.addAll(Arrays.asList(pub));
    cloud._privateIPs.addAll(Arrays.asList(prv));
    return cloud;
  }

  private static String pubKey() {
    BufferedReader r = null;
    try {
      String pub = System.getProperty("user.home") + "/.ssh/id_rsa.pub";
      r = new BufferedReader(new FileReader(new File(pub)));
      return r.readLine();
    } catch( IOException e ) {
      throw Log.errRTExcept(e);
    } finally {
      if( r != null )
        try {
          r.close();
        } catch( IOException e ) {
          throw Log.errRTExcept(e);
        }
    }
  }

  private List<Instance> wait(AmazonEC2Client ec2, List<String> ids) {
    System.out.println("Establishing ssh connections, make sure security group '" //
        + securityGroup + "' allows incoming TCP 22.");
    boolean tagsDone = false;
    for( ;; ) {
      try {
        if( !tagsDone ) {
          CreateTagsRequest createTagsRequest = new CreateTagsRequest();
          createTagsRequest.withResources(ids).withTags(new Tag("Name", NAME));
          ec2.createTags(createTagsRequest);
          tagsDone = true;
        }
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        request.withInstanceIds(ids);
        DescribeInstancesResult result = ec2.describeInstances(request);
        List<Reservation> reservations = result.getReservations();
        List<Instance> instances = new ArrayList<Instance>();
        for( Reservation reservation : reservations )
          for( Instance instance : reservation.getInstances() )
            if( ip(instance) != null )
              instances.add(instance);
        if( instances.size() == ids.size() ) {
          // Try to connect to SSH port on each box
          if( canConnect(instances) )
            return instances;
        }
      } catch( AmazonServiceException _ ) {
        // Ignore and retry
      }
      try {
        Thread.sleep(500);
      } catch( InterruptedException e ) {
        throw Log.errRTExcept(e);
      }
    }
  }

  private static String ip(Instance instance) {
    String ip = instance.getPublicIpAddress();
    if( ip != null && ip.length() != 0 )
      if( instance.getState().getName().equals("running") )
        return ip;
    return null;
  }

  private static boolean canConnect(List<Instance> instances) {
    for( Instance instance : instances ) {
      try {
        String ssh = "ssh -q" + Host.SSH_OPTS + " " + instance.getPublicIpAddress();
        Process p = Runtime.getRuntime().exec(ssh + " exit");
        if( p.waitFor() != 0 )
          return false;
      } catch( Exception e ) {
        return false;
      } finally {
      }
    }
    return true;
  }
}
