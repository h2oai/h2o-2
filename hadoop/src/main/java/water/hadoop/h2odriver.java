package water.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Driver class to start a Hadoop mapreduce job which wraps an H2O cluster launch.
 *
 * All mapreduce I/O is typed as <Text, Text>.
 * The first Text is the Key (Mapper Id).
 * The second Text is the Value (a log output).
 *
 * Adapted from
 * https://svn.apache.org/repos/asf/hadoop/common/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/SleepJob.java
 */
public class h2odriver extends Configured implements Tool {
  final static String FLATFILE_NAME = "flatfile.txt";

  // Options that are parsed by the main thread before other threads are created.
  static String jobtrackerName = null;
  static int numNodes = -1;
  static String outputPath = null;
  static String mapperXmx = null;
  static String driverCallbackIp = null;
  static int driverCallbackPort = 0;          // By default, let the system pick the port.
  static boolean disown = false;
  static String clusterReadyFileName = null;

  // Runtime state that might be touched by different threads.
  volatile ServerSocket driverCallbackSocket = null;
  volatile Job job = null;
  volatile CtrlCHandler ctrlc = null;
  volatile boolean clusterIsUp = false;

  public static class H2ORecordReader extends RecordReader<Text, Text> {
    H2ORecordReader() {
    }

    public void initialize(InputSplit split, TaskAttemptContext context) {
    }

    public boolean nextKeyValue() throws IOException {
      return false;
    }

    public Text getCurrentKey() { return null; }
    public Text getCurrentValue() { return null; }
    public void close() throws IOException { }
    public float getProgress() throws IOException { return 0; }
  }

  public static class EmptySplit extends InputSplit implements Writable {
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() { return 0L; }
    public String[] getLocations() { return new String[0]; }
  }

  public static class H2OInputFormat extends InputFormat<Text, Text> {
    H2OInputFormat() {
    }

    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      List<InputSplit> ret = new ArrayList<InputSplit>();
      int numSplits = numNodes;
      for (int i = 0; i < numSplits; ++i) {
        ret.add(new EmptySplit());
      }
      return ret;
    }

    public RecordReader<Text, Text> createRecordReader(
            InputSplit ignored, TaskAttemptContext taskContext)
            throws IOException {
      H2ORecordReader trr = new H2ORecordReader();
      return trr;
    }
  }

  /**
   * Handle Ctrl-C and other catchable shutdown events.
   * If we successfully catch one, then try to kill the hadoop job if
   * we have not already been told it completed.
   *
   * (Of course kill -9 cannot be handled.)
   */
  class CtrlCHandler extends Thread {
    volatile boolean _complete = false;

    public void setComplete() {
      _complete = true;
    }

    @Override
    public void run() {
      if (_complete) {
        return;
      }

      boolean killed = false;

      try {
        System.out.println("Attempting to clean up hadoop job...");
        job.killJob();
        for (int i = 0; i < 5; i++) {
          if (job.isComplete()) {
            System.out.println("Killed.");
            killed = true;
            break;
          }

          Thread.sleep(1000);
        }
      }
      catch (Exception _) {
      }
      finally {
        if (! killed) {
          System.out.println("Kill attempt failed, please clean up job manually.");
        }
      }
    }
  }

  /**
   * Read and handle one Mapper->Driver Callback message.
   */
  class CallbackHandlerThread extends Thread {
    private Socket _s;

    private void createClusterReadyFile(String ip, int port) throws Exception {
      String fileName = clusterReadyFileName + ".tmp";
      String text = ip + ":" + port + "\n";
      try {
        File file = new File(fileName);
        BufferedWriter output = new BufferedWriter(new FileWriter(file));
        output.write(text);
        output.flush();
        output.close();

        File file2 = new File(clusterReadyFileName);
        boolean success = file.renameTo(file2);
        if (! success) {
          throw new Exception ("Failed to create file " + clusterReadyFileName);
        }
      } catch ( IOException e ) {
        e.printStackTrace();
      }
    }

    public void setSocket (Socket value) {
      _s = value;
    }

    @Override
    public void run() {
      MapperToDriverMessage msg = new MapperToDriverMessage();
      try {
        msg.readMessage(_s);
        char type = msg.getType();
        if (type == MapperToDriverMessage.TYPE_EOF_NO_MESSAGE) {
          // Ignore it.
          return;
        }

        // System.out.println("Read message with type " + (int)type);
        if (type == MapperToDriverMessage.TYPE_EMBEDDED_WEB_SERVER_IP_PORT) {
          // System.out.println("H2O node " + msg.getEmbeddedWebServerIp() + ":" + msg.getEmbeddedWebServerPort() + " started");
        }
        else if (type == MapperToDriverMessage.TYPE_CLOUD_SIZE) {
          System.out.println("H2O node " + msg.getEmbeddedWebServerIp() + ":" + msg.getEmbeddedWebServerPort() + " reports H2O cluster size " + msg.getCloudSize());
          if (msg.getCloudSize() == numNodes) {
            // Do this under a synchronized block to avoid getting multiple cluster ready notification files.
            synchronized (h2odriver.class) {
              if (! clusterIsUp) {
                if (clusterReadyFileName != null) {
                  createClusterReadyFile(msg.getEmbeddedWebServerIp(), msg.getEmbeddedWebServerPort());
                  System.out.println("Cluster notification file (" + clusterReadyFileName + ") created.");
                }
                clusterIsUp = true;
              }
            }
          }
        }
        else if (type == MapperToDriverMessage.TYPE_EXIT) {
          System.out.println("H2O node " + msg.getEmbeddedWebServerIp() + ":" + msg.getEmbeddedWebServerPort() + " exited with status " + msg.getExitStatus());
        }
        else {
          System.err.println("MapperToDriverMessage: Read invalid type (" + type + ") from socket, ignoring...");
        }
        _s.close();
      }
      catch (Exception e) {
        System.out.println("Exception occurred in CallbackHandlerThread");
        System.out.println(e.toString());
        if (e.getMessage() != null) {
          System.out.println(e.getMessage());
        }
        e.printStackTrace();
      }
    }
  }

  /**
   * Start a long-running thread ready to handle Mapper->Driver messages.
   */
  class CallbackManager extends Thread {
    private ServerSocket _ss;

    public void setServerSocket (ServerSocket value) {
      _ss = value;
    }

    @Override
    public void run() {
      while (true) {
        try {
          Socket s = _ss.accept();
          CallbackHandlerThread t = new CallbackHandlerThread();
          t.setSocket(s);
          t.start();
        }
        catch (Exception e) {
          System.out.println("Exception occurred in CallbackManager");
          System.out.println(e.toString());
          if (e.getMessage() != null) {
            System.out.println(e.getMessage());
          }
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Print usage and exit 1.
   */
  static void usage() {
    System.err.printf(
            "\n" +
                    "Usage: h2odriver\n" +
                    "          -files <.../flatfile.txt>\n" +
                    "          -libjars <.../h2o.jar>\n" +
                    "          [other generic Hadoop ToolRunner options]\n" +
                    "          [-h | -help]\n" +
                    "          [-jobname <name of job in jobtracker (defaults to: 'H2O_nnnnn')>]\n" +
                    "              (Note nnnnn is chosen randomly to produce a unique name)\n" +
                    "          [-driverif <ip address of mapper->driver callback interface>]" +
                    "          [-driverport <port of mapper->driver callback interface>]" +
                    "          [-disown]" +
                    "          [-notify <notification file name>]" +
                    "          -mapperXmx <per mapper Java Xmx heap size>\n" +
                    "          -n | -nodes <number of H2O nodes (i.e. mappers) to create>\n" +
                    "          -o | -output <hdfs output dir>\n" +
                    "\n" +
                    "Notes:\n" +
                    "          o  Each H2O node runs as a mapper.\n" +
                    "          o  Only one mapper may be run per host.\n" +
                    "          o  There are no combiners or reducers.\n" +
                    "          o  Each H2O cluster should have a unique jobname.\n" +
                    "          o  -mapperXmx, -nodes and -output are required.\n" +
                    "\n" +
                    "          o  -mapperXmx is set to both Xms and Xmx of the mapper to reserve\n" +
                    "             memory up front.\n" +
                    "          o  -files flatfile.txt is required and must be named flatfile.txt.\n" +
                    "          o  -libjars with an h2o.jar is required.\n" +
                    "          o  -driverif and -driverport let the user optionally specify the\n" +
                    "             network interface and port (on the driver host) for callback\n" +
                    "             messages from the mapper to the driver.\n" +
                    "          o  -disown causes the driver to exit as soon as the cloud forms.\n" +
                    "             Otherwise, Ctrl-C of the driver kills the Hadoop Job.\n" +
                    "          o  -notify specifies a file to write when the cluster is up.\n" +
                    "             The file contains one line with the IP and port of the embedded\n" +
                    "             web server for one of the H2O nodes in the cluster.  e.g.\n" +
                    "                 192.168.1.100:54321\n" +
                    "          o  All mappers must start before the H2O cloud is considered up.\n" +
                    "\n" +
                    "Examples:\n" +
                    "          hadoop jar h2odriver_HHH.jar water.hadoop.h2odriver -jt <yourjobtracker>:<yourport> -files flatfile.txt -libjars h2o.jar -mapperXmx 1g -n 1 -o hdfsOutputDir\n" +
                    "          (Choose the proper h2odriver (_HHH) for your version of hadoop.\n" +
                    "\n" +
                    "flatfile.txt:\n" +
                    "          The flat file must contain the list of possible IP addresses an H2O\n" +
                    "          node (i.e. mapper) may be scheduled on.  One IP address per line.\n" +
                    "          For example:\n" +
                    "              192.168.1.100\n" +
                    "              192.168.1.101\n" +
                    "              etc.\n" +
                    "\n"
    );

    System.exit(1);
  }

  /**
   * Print an error message, print usage, and exit 1.
   * @param s Error message
   */
  static void error(String s) {
    System.err.printf("\nERROR: " + "%s\n\n", s);
    usage();
  }

  /**
   * Parse remaining arguments after the ToolRunner args have already been removed.
   * @param args Argument list
   */
  void parseArgs(String[] args) {
    int i = 0;
    while (true) {
      if (i >= args.length) {
        break;
      }

      String s = args[i];
      if (s.equals("-h") ||
              s.equals("help") ||
              s.equals("-help") ||
              s.equals("--help")) {
        usage();
      }
      else if (s.equals("-n") ||
              s.equals("-nodes")) {
        i++; if (i >= args.length) { usage(); }
        numNodes = Integer.parseInt(args[i]);
      }
      else if (s.equals("-o") ||
              s.equals("-output")) {
        i++; if (i >= args.length) { usage(); }
        outputPath = args[i];
      }
      else if (s.equals("-jobname")) {
        i++; if (i >= args.length) { usage(); }
        jobtrackerName = args[i];
      }
      else if (s.equals("-mapperXmx")) {
        i++; if (i >= args.length) { usage(); }
        mapperXmx = args[i];
      }
      else if (s.equals("-driverif")) {
        i++; if (i >= args.length) { usage(); }
        driverCallbackIp = args[i];
      }
      else if (s.equals("-driverport")) {
        i++; if (i >= args.length) { usage(); }
        driverCallbackPort = Integer.parseInt(args[i]);
      }
      else if (s.equals("-disown")) {
        disown = true;
      }
      else if (s.equals("-notify")) {
        i++; if (i >= args.length) { usage(); }
        clusterReadyFileName = args[i];
      }
      else {
        error("Unrecognized option " + s);
      }

      i++;
    }

    // Check for mandatory arguments.
    if (numNodes < 1) {
      error("Number of H2O nodes must be greater than 0 (must specify -n)");
    }
    if (outputPath == null) {
      error("Missing required option -output");
    }
    if (mapperXmx == null) {
      error("Missing required option -mapperXmx");
    }

    // Check for sane arguments.
    if (! mapperXmx.matches("[1-9][0-9]*[mgMG]")) {
      error("-mapperXmx invalid (try something like -mapperXmx 4g)");
    }

    if (jobtrackerName == null) {
      Random rng = new Random();
      int num = rng.nextInt(99999);
      jobtrackerName = "H2O_" + num;
    }
  }

  static String calcMyIp() throws Exception {
    Enumeration nis = NetworkInterface.getNetworkInterfaces();

    System.out.println("Determining driver host interface for mapper->driver callback...");
    while (nis.hasMoreElements()) {
      NetworkInterface ni = (NetworkInterface) nis.nextElement();
      Enumeration ias = ni.getInetAddresses();
      while (ias.hasMoreElements()) {
        InetAddress ia = (InetAddress) ias.nextElement();
        String s = ia.getHostAddress();
        System.out.println("    [Possible callback IP address: " + s + "]");
      }
    }

    InetAddress ia = InetAddress.getLocalHost();
    String s = ia.getHostAddress();

    return s;
  }

  private void waitForClusterToComeUp() throws Exception {
    while (true) {
      if (job.isComplete()) {
        break;
      }

      if (clusterIsUp) {
        break;
      }

      final int ONE_SECOND_MILLIS = 1000;
      Thread.sleep (ONE_SECOND_MILLIS);
    }
  }

  private void waitForClusterToShutdown() throws Exception {
    while (true) {
      if (job.isComplete()) {
        break;
      }

      final int ONE_SECOND_MILLIS = 1000;
      Thread.sleep (ONE_SECOND_MILLIS);
    }
  }

  private int run2(String[] args) throws Exception {
    // Parse arguments.
    // ----------------
    parseArgs (args);

    // Set up callback address and port.
    // ---------------------------------
    if (driverCallbackIp == null) {
      driverCallbackIp = calcMyIp();
    }
    driverCallbackSocket = new ServerSocket();
    driverCallbackSocket.setReuseAddress(true);
    InetSocketAddress sa = new InetSocketAddress(driverCallbackIp, driverCallbackPort);
    driverCallbackSocket.bind(sa, driverCallbackPort);
    int actualDriverCallbackPort = driverCallbackSocket.getLocalPort();
    CallbackManager cm = new CallbackManager();
    cm.setServerSocket(driverCallbackSocket);
    cm.start();
    System.out.println("    [Using mapper->driver callback IP address and port: " + driverCallbackIp + ":" + actualDriverCallbackPort + "]");
    System.out.println("    [You can override these with -driverif and -driverport.]");

    // Set up configuration.
    // ---------------------
    Configuration conf = getConf();
//        conf.set("mapred.child.java.opts", "-Dh2o.FINDME=ignored");
//        conf.set("mapred.map.child.java.opts", "-Dh2o.FINDME2=ignored");
//        conf.set("mapred.map.child.java.opts", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8999");
    String mapChildJavaOpts = "-Xms" + mapperXmx + " -Xmx" + mapperXmx;
    conf.set("mapred.child.java.opts", mapChildJavaOpts);
    conf.set("mapred.map.child.java.opts", mapChildJavaOpts);       // MapR 2.x requires this.

    // This is really silly, but without this line, the following ugly warning
    // gets emitted as the very first line of output, which is confusing for
    // the user.
    // Generic options parser is used automatically by ToolRunner, but somehow
    // that framework is not smart enough to disable the warning.
    //
    // Eliminates this runtime warning!
    // "WARN mapred.JobClient: Use GenericOptionsParser for parsing the arguments. Applications should implement Tool for the same."
    conf.set("mapred.used.genericoptionsparser", "true");

    // We don't want hadoop launching extra nodes just to shoot them down.
    // Not good for in-memory H2O processing!
    conf.set("mapreduce.map.speculative", "false");
    conf.set("mapred.map.tasks.speculative.execution", "false");

    conf.set("mapred.map.max.attempts", "1");
    conf.set("mapred.job.reuse.jvm.num.tasks", "1");
    conf.set(h2omapper.H2O_JOBTRACKERNAME_KEY, jobtrackerName);

    conf.set(h2omapper.H2O_DRIVER_IP_KEY, driverCallbackIp);
    conf.set(h2omapper.H2O_DRIVER_PORT_KEY, Integer.toString(actualDriverCallbackPort));

    // Set up job stuff.
    // -----------------
    job = new Job(conf, jobtrackerName);
    job.setJarByClass(getClass());
    job.setInputFormatClass(H2OInputFormat.class);
    job.setMapperClass(h2omapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path("ignored"));
    if (outputPath != null) {
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }

    // Run job.  We are running a zero combiner and zero reducer configuration.
    // ------------------------------------------------------------------------
    job.submit();
    System.out.println("Job name '" + jobtrackerName + "' submitted");
    System.out.println("JobTracker job ID is '" + job.getJobID() + "'");

    // Register ctrl-c handler to try to clean up job when possible.
    ctrlc = new CtrlCHandler();
    Runtime.getRuntime().addShutdownHook(ctrlc);

    System.out.printf("Waiting for H2O cluster to come up...\n", numNodes);
    waitForClusterToComeUp();
    if (job.isComplete()) {
      System.out.println("H2O cluster failed to come up.");
      ctrlc.setComplete();
      return 2;
    }

    System.out.printf("H2O cluster (%d nodes) is up.\n", numNodes);
    if (disown) {
      System.out.println("Disowning cluster and exiting.");
      Runtime.getRuntime().removeShutdownHook(ctrlc);
      return 0;
    }

    System.out.println("(Press Ctrl-C to kill the cluster.)");
    System.out.println("Blocking until the H2O cluster shuts down...");
    waitForClusterToShutdown();
    ctrlc.setComplete();
    boolean success = job.isSuccessful();
    if (! success) {
      return 1;
    }

    return 0;
  }

  /**
   * The run method called by ToolRunner.
   * @param args Arguments after ToolRunner arguments have been removed.
   * @return Exit value of program.
   * @throws Exception
   */
  @Override
  public int run(String[] args) {
    int rv = -1;

    try {
      rv = run2(args);
    }
    catch (org.apache.hadoop.mapred.FileAlreadyExistsException e) {
      if (ctrlc != null) { ctrlc.setComplete(); }
      System.out.println("ERROR: " + (e.getMessage() != null ? e.getMessage() : "(null)"));
      System.exit(1);
    }
    catch (Exception e) {
      System.out.println("ERROR: " + (e.getMessage() != null ? e.getMessage() : "(null)"));
      e.printStackTrace();
      System.exit(1);
    }

    return rv;
  }

  /**
   * Ensure that flatfile.txt is a file passed to -files.
   * @param args Full program args, including those that go to ToolRunner.
   */
  static void validateFlatfile(String[] args) {
    boolean found = false;

    outerloop:
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-files")) {
        i++; if (i >= args.length) { break; }
        String s = args[i];
        String[] arr = s.split(",");

        for (int j = 0; j < arr.length; j++) {
          if (arr[j].equals(FLATFILE_NAME)) {
            found = true;
            break outerloop;
          }
        }

        break outerloop;
      }
    }

    if (! found) {
      error(FLATFILE_NAME + " must be specified in -files");
    }
  }

  /**
   * Main entry point
   * @param args Full program args, including those that go to ToolRunner.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    validateFlatfile(args);
    int exitCode = ToolRunner.run(new h2odriver(), args);
    System.exit(exitCode);
  }
}
