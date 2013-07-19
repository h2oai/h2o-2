package water.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;

/**
 * Created with IntelliJ IDEA.
 * User: tomk
 * Date: 7/18/13
 * Time: 4:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class gen_flatfile extends Configured implements Tool {
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String trackerIpPort = conf.get("mapred.job.tracker");
    // System.err.println("mapred.job.tracker: " + trackerIpPort);
    String[] arr = trackerIpPort.split(":");
    String host = arr[0];
    int port = Integer.parseInt(arr[1]);
    // System.err.println("host: " + host);
    // System.err.println("port: " + port);

    InetSocketAddress addr = new InetSocketAddress(host, port);
    JobClient client = new JobClient(addr, conf);
    Collection<String> names = client.getClusterStatus(true).getActiveTrackerNames();
    for (String name : names) {
      String n = name.substring("tracker_".length(), name.indexOf(':'));
      String s = InetAddress.getByName(n).getHostAddress();
      System.out.println(s);
    }

    return 0;
  }

  /**
   * Main entry point
   * @param args Full program args, including those that go to ToolRunner.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new gen_flatfile(), args);
    System.exit(exitCode);
  }
}
