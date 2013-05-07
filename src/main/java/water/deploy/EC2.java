package water.deploy;

import java.io.*;
import java.util.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;

import water.Arguments;
import water.H2O;
import water.deploy.Cloud.Master;
import water.util.Log;
import water.util.Utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;

public abstract class EC2 {
  private static final String USER = System.getProperty("user.name");
  private static final String NAME = USER + "-H2O-Cloud";

  public static class Config extends Arguments.Opt {
    String region = "us-east-1";
    String type = "m1.small";
    String secg = "default";
    boolean conf = true;
    String incl; // additional rsync includes
    String excl; // additional rsync excludes
  }

  public static void main(String[] args) throws Exception {
    Config config;
    int count;
    String[] remaining;
    try {
      Arguments arguments = new Arguments(args);
      config = new Config();
      arguments.extract(config);
      remaining = Arrays.copyOfRange(args, arguments.firstFlag(), args.length);
      count = Integer.parseInt(remaining[0]);
      remaining = Arrays.copyOfRange(remaining, 1, remaining.length);
    } catch( Exception ex ) {
      Config defaults = new Config();
      System.out.println("Usage: h2o_on_ec2 [options] <machine_count> args");
      System.out.println();
      System.out.println("Options and default values:");
      System.out.println("  -region=" + defaults.region);
      System.out.println("  -type=" + defaults.type);
      System.out.println("  -secg=" + defaults.secg + " (Security Group, must allow ssh (TCP 22))");
      System.out.println("  -conf=" + defaults.conf + " (Confirm before starting new instances)");
      System.out.println("  -incl='' (additional rsync includes, e.g. py:smalldata)");
      System.out.println("  -excl='' (additional rsync excludes, e.g. sandbox:*.pyc)");
      System.out.println();
      System.out.println("AWS credentials are looked for at './AwsCredentials.properties'");
      return;
    }
    run(config, count, remaining);
  }

  public static void run(Config config, int count, String[] args) throws Exception {
    Cloud c = resize(config.region, config.type, config.secg, config.conf, count);

    // Take first box as cloud master
    Host master = new Host(c.publicIPs()[0]);
    String[] includes = config.incl != null ? config.incl.split(File.pathSeparator) : null;
    String[] excludes = config.excl != null ? config.excl.split(File.pathSeparator) : null;
    includes = (String[]) ArrayUtils.addAll(Host.defaultIncludes(), includes);
    excludes = (String[]) ArrayUtils.addAll(Host.defaultExcludes(), excludes);

    File flatfile = Utils.tempFile(Utils.join('\n', c.privateIPs()));
    includes = (String[]) ArrayUtils.add(includes, flatfile.getAbsolutePath());

    master.rsync(includes, excludes);

    ArrayList<String> list = new ArrayList<String>();
    list.add("-mainClass");
    list.add(Master.class.getName());
    list.add("-flatfile");
    list.add(flatfile.getName());
    list.add("-log_headers");
    list.addAll(Arrays.asList(args));
    RemoteRunner.launch(master, list.toArray(new String[0]));
  }

  /**
   * Create or terminate EC2 instances. Uses their Name tag to find existing ones.
   *
   * @return Public IPs
   */
  public static Cloud resize(String region, String type, String secg, boolean confirm, int count) throws IOException {
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
          if( instance.getTags().size() > 0 ) name = instance.getTags().get(0).getValue();
          if( NAME.equals(name) ) instances.add(instance);
        }
      }
    }
    System.out.println("Found " + instances.size() + " EC2 instances.");

    if( instances.size() > count ) {
      for( int i = 0; i < instances.size() - count; i++ ) {
        // TODO terminate
      }
    } else if( instances.size() < count ) {
      int launchCount = count - instances.size();
      System.out.println("Creating " + launchCount + " EC2 instances.");
      if( confirm ) {
        System.out.println("Please confirm [y/n]");
        String s = Utils.readConsole();
        if( s == null || !s.equalsIgnoreCase("y") ) throw new RuntimeException("Aborted");
      }

      RunInstancesRequest request = new RunInstancesRequest();
      request.withInstanceType(type);
      if( region.startsWith("us-east-1") ) request.withImageId("ami-fc75ee95");
      else if( region.startsWith("us-west-1") ) request.withImageId("ami-64d1fc21");
      else if( region.startsWith("us-west-2") ) // Oregon
      request.withImageId("ami-52bf2b62");
      else if( region.startsWith("eu-west-1") ) request.withImageId("ami-5e93992a");
      else if( region.startsWith("ap-southeast-1") ) // Singapore
      request.withImageId("ami-ac9ed2fe");
      else if( region.startsWith("ap-southeast-2") ) // Sydney
      request.withImageId("ami-283eaf12");
      else if( region.startsWith("ap-northeast-1") ) // Tokyo
      request.withImageId("ami-153fbf14");
      else if( region.startsWith("sa-east-1") ) // Sao Paulo
      request.withImageId("ami-db6bb0c6");

      request.withMinCount(launchCount).withMaxCount(launchCount);
      request.withSecurityGroupIds(secg);
      // TODO what's the right way to have boxes in same availability zone?
      // request.withPlacement(new Placement(region + "c"));
      request.withUserData(new String(Base64.encodeBase64(cloudConfig().getBytes())));

      RunInstancesResult runInstances = ec2.runInstances(request);
      ArrayList<String> ids = new ArrayList<String>();
      for( Instance instance : runInstances.getReservation().getInstances() )
        ids.add(instance.getInstanceId());

      List<Instance> created = wait(ec2, ids, secg);
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
    return new Cloud(pub, prv);
  }

//@formatter:off
  private static String cloudConfig() {
    return "#cloud-config\n" +
      "apt_update: false\n" +
      "runcmd:\n" +
      " - grep -q 'fs.file-max = 524288' /etc/sysctl.conf || echo -e '\\nfs.file-max = 524288' >> /etc/sysctl.conf\n" +
      " - sysctl -w fs.file-max=524288\n" +
      " - echo -e '* soft nofile 524288\\n* hard nofile 524288' > /etc/security/limits.d/increase-max-fd.conf\n" +
      // " - echo -e 'iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-ports 8080' >> /etc/rc.d/rc.local\n" +
      // " - yum -y remove java-1.6.0-openjdk\n" +
      // " - yum -y install java-1.7.0-openjdk\n" +
      " - useradd " + USER + "\n" +
      " - mkdir -p /home/" + USER + "/.ssh" + "\n" +
      " - cd /home/" + USER + "\n" +
      " - echo -e '\\n" + USER + " ALL=(ALL) NOPASSWD:ALL\\n' >> /etc/sudoers\n" +
      // " - wget http://h2o_rsync.s3.amazonaws.com/h2o_rsync.zip\n" +
      // " - unzip h2o_rsync.zip\n" +
      " - chown -R " + USER + ':' + USER + " /home/" + USER + "\n" +
      " - echo '" + pubKey() + "' >> .ssh/authorized_keys\n" +
      "";
  }
//@formatter:on

  private static String pubKey() {
    BufferedReader r = null;
    try {
      String pub = System.getProperty("user.home") + "/.ssh/id_rsa.pub";
      r = new BufferedReader(new FileReader(new File(pub)));
      return r.readLine();
    } catch( IOException e ) {
      throw Log.errRTExcept(e);
    } finally {
      if( r != null ) try {
        r.close();
      } catch( IOException e ) {
        throw Log.errRTExcept(e);
      }
    }
  }

  private static List<Instance> wait(AmazonEC2Client ec2, List<String> ids, String secg) {
    System.out.println("Establishing ssh connections, make sure security group '" + secg + "' allows incoming TCP 22.");
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
            if( ip(instance) != null ) instances.add(instance);
        if( instances.size() == ids.size() ) {
          // Try to connect to SSH port on each box
          if( canConnect(instances) ) return instances;
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
    if( ip != null && ip.length() != 0 ) if( instance.getState().getName().equals("running") ) return ip;
    return null;
  }

  private static boolean canConnect(List<Instance> instances) {
    for( Instance instance : instances ) {
      try {
        String ssh = Host.ssh() + " -q" + Host.SSH_OPTS + " " + instance.getPublicIpAddress();
        Process p = Runtime.getRuntime().exec(ssh + " exit");
        if( p.waitFor() != 0 ) return false;
      } catch( Exception e ) {
        return false;
      } finally {}
    }
    return true;
  }
}
