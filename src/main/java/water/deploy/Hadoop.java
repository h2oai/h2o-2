package water.deploy;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import water.*;
import water.hdfs.HdfsLoader;
import water.util.Utils;

public class Hadoop {
  public static class Config extends Arguments.Opt {
    String version = "cdh3";
    String user = "hduser";
    String name_server = "hdfs://127.0.0.1:8020";
    String tracker = "hdfs://127.0.0.1:8021";
    int port = 54321;
  }

  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments(args);
    Config config = new Config();
    arguments.extract(config);
    String[] remaining = new String[0];
    if( arguments.firstFlag() >= 0 )
      remaining = Arrays.copyOfRange(args, arguments.firstFlag(), args.length);

    H2O.OPT_ARGS.hdfs_version = config.version;
    HdfsLoader.initialize();

    HadoopTool.main(config, remaining);
  }

  public static class HadoopTool extends Configured implements Tool {
    private static final String HOSTS_KEY = "h2o.hosts";
    private static final String PORT_KEY = "h2o.port";

    static class H2OMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
      @Override protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        try {
          String hosts = "";
          String port = context.getConfiguration().get(PORT_KEY);
          for( String host : context.getConfiguration().get(HOSTS_KEY).split(",") )
            hosts += host + ":" + port + '\n';
          File flat = Utils.tempFile(hosts);
          Boot.main(new String[] { "-name", "hadoop", "-port", port, "-flatfile", flat.getAbsolutePath() });
          for( ;; ) {
            // Report progress or task gets killed
            context.progress();
            Thread.sleep(10000);
          }
        } catch( Exception ex ) {
          throw new RuntimeException(ex);
        }
      }

      @Override protected void map(LongWritable key, Text value, Context context) throws IOException,
          InterruptedException {
      };
    }

    @Override public int run(String[] args) throws Exception {
      Job job = new Job(getConf());
      job.setJobName("H2O");
      job.setMapperClass(H2OMapper.class);
      job.setInputFormatClass(NopInputFormat.class);
      job.setOutputFormatClass(NopOutputFormat.class);
      job.setNumReduceTasks(0);
      job.waitForCompletion(true);
      return 0;
    }

    private static class NopInputFormat extends InputFormat<Void, Void> {
      @Override public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        ArrayList<InputSplit> list = new ArrayList<InputSplit>();
        for( String host : context.getConfiguration().get(HOSTS_KEY).split(",") )
          list.add(new NopInputSplit(host));
        return list;
      }

      @Override public RecordReader<Void, Void> createRecordReader(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
        return new NopRecordReader();
      }
    }

    private static class NopOutputFormat extends OutputFormat<Void, Void> {
      @Override public RecordWriter<Void, Void> getRecordWriter(TaskAttemptContext context) throws IOException,
          InterruptedException {
        return new NopRecordWriter();
      }

      @Override public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
      }

      @Override public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException,
          InterruptedException {
        return new NopOutputCommitter();
      }
    }

    private static class NopInputSplit extends InputSplit implements Writable {
      final String _loc;

      NopInputSplit(String loc) {
        _loc = loc;
      }

      @SuppressWarnings("unused") NopInputSplit() {
        _loc = null;
      }

      @Override public long getLength() throws IOException, InterruptedException {
        return 0;
      }

      @Override public String[] getLocations() throws IOException, InterruptedException {
        return new String[] { _loc };
      }

      @Override public void write(DataOutput out) throws IOException {
      }

      @Override public void readFields(DataInput in) throws IOException {
      }
    }

    private static class NopRecordReader extends RecordReader {
      @Override public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
          InterruptedException {
      }

      @Override public boolean nextKeyValue() throws IOException, InterruptedException {
        throw new RuntimeException("TODO Auto-generated method stub");
      }

      @Override public Object getCurrentKey() throws IOException, InterruptedException {
        throw new RuntimeException("TODO Auto-generated method stub");
      }

      @Override public Object getCurrentValue() throws IOException, InterruptedException {
        throw new RuntimeException("TODO Auto-generated method stub");
      }

      @Override public float getProgress() throws IOException, InterruptedException {
        throw new RuntimeException("TODO Auto-generated method stub");
      }

      @Override public void close() throws IOException {
        throw new RuntimeException("TODO Auto-generated method stub");
      }
    }

    private static class NopRecordWriter extends RecordWriter {
      @Override public void write(Object key, Object value) throws IOException, InterruptedException {
        throw new RuntimeException("TODO Auto-generated method stub");
      }

      @Override public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        throw new RuntimeException("TODO Auto-generated method stub");
      }
    }

    static class NopOutputCommitter extends OutputCommitter {
      @Override public void setupJob(JobContext jobContext) throws IOException {
      }

      @Override public void setupTask(TaskAttemptContext taskContext) throws IOException {
      }

      @Override public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return false;
      }

      @Override public void commitTask(TaskAttemptContext taskContext) throws IOException {
      }

      @Override public void abortTask(TaskAttemptContext taskContext) throws IOException {
      }
    }

    public static void main(Config config, String[] args) throws Exception {
      //   Logger.getRootLogger().setLevel(Level.ALL);
      System.setProperty("HADOOP_USER_NAME", config.user);
      Configuration conf = new Configuration();
      conf.set("fs.default.name", config.name_server);
      conf.set("mapred.job.tracker", config.tracker);
      conf.set("mapreduce.framework.name", "classic");
      // conf.set("hadoop.job.ugi", "hduser,hduser");
      conf.setInt("mapred.tasktracker.map.tasks.maximum", 1);
      conf.set("mapred.jar", "/home/cypof/h2o/target/h2o.jar");
      conf.set("mapred.child.java.opts", "-Xms256m -Xmx2g -XX:+UseSerialGC");
      conf.set("mapred.job.map.memory.mb", "4096");
      conf.set("mapred.job.reduce.memory.mb", "1024");

      String hosts = "";
      URI tracker = new URI(config.tracker);
      JobClient client = new JobClient(new InetSocketAddress(tracker.getHost(), tracker.getPort()), conf);
      for( String name : client.getClusterStatus(true).getActiveTrackerNames() ) {
        String host = name.substring("tracker_".length(), name.indexOf(':'));
        hosts += InetAddress.getAllByName(host)[0].getHostAddress() + ',';
      }
      conf.set(HOSTS_KEY, hosts);
      conf.set(PORT_KEY, "" + config.port);

      ToolRunner.run(conf, new HadoopTool(), args);
    }
  }
}