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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
public class H2ODriver extends Configured implements Tool {
    final static String DEFAULT_JOBTRACKER_NAME = "H2O";
    final static String FLATFILE_NAME = "flatfile.txt";
    static String jobtrackerName = DEFAULT_JOBTRACKER_NAME;
    static int numNodes = -1;
    static String outputPath = null;
    static String mapperXmx = null;

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
     * Print usage and exit 1.
     */
    static void usage() {
        String prog = H2ODriver.class.getSimpleName();

        System.err.printf(
"\n" +
"Usage: " + prog + "\n" +
"          -files <.../flatfile.txt>\n" +
"          [other generic Hadoop ToolRunner options]\n" +
"          [-h | -help]\n" +
"          [-jobname <name of job in jobtracker (default: '" + DEFAULT_JOBTRACKER_NAME + "')>]\n" +
"          -mapperXmx <per mapper Java Xmx heap size>\n" +
"          -n | -nodes <number of h2o nodes (i.e. mappers) to create>\n" +
"          -o | -output <hdfs output dir>\n" +
"\n" +
"Notes:\n" +
"          o  Each H2O node runs as a mapper.\n" +
"          o  All mappers must come up simultaneously before the job proceeds.\n" +
"          o  Only one mapper may be run per host (if more land on one host," +
"             the subsequent mappers will exit and get rescheduled by hadoop).\n" +
"          o  Mapper output (part-n-xxxxx) is log output from that mapper.\n" +
"          o  -mapperXmx is set to both Xms and Xmx of the mapper to reserve" +
"             memory up front.\n" +
"          o  There are no combiners or reducers.\n" +
"          o  flatfile.txt is required and must be named flatfile.txt.\n" +
"          o  -mapperXmx, -n and -o are required.\n" +
"\n" +
"Examples:\n" +
"          " + prog + " -jt <yourjobtracker>:<yourport> -files flatfile.txt -mapperXmx 1g -n 1 -o hdfsOutputDir\n" +
"          " + prog + " -jt <yourjobtracker>:<yourport> -files flatfile.txt -mapperXmx 1g -n 4 -o hdfsOutputDir -jobname H2O_PROD\n" +
"\n" +
"flatfile.txt:\n" +
"          The flat file must contain the list of possible IP addresses an H2O\n" +
"          node (i.e. mapper) may be scheduled on.  One IP address per line." +
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
    }

    /**
     * The run method called by ToolRunner.
     * @param args Arguments after ToolRunner arguments have been removed.
     * @return Exit value of program.
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        // Parse arguments.
        // ----------------
        parseArgs (args);

        // Set up configuration.
        // ---------------------
        Configuration conf = getConf();
        conf.set("mapred.child.java.opts", "-Xms" + mapperXmx + " -Xmx" + mapperXmx);

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

        // Set up job stuff.
        // -----------------
    	Job job = new Job(conf, jobtrackerName);
    	job.setJarByClass(getClass());
        job.setInputFormatClass(H2OInputFormat.class);
    	job.setMapperClass(H2OMapper.class);
        job.setNumReduceTasks(0);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("ignored"));
        if (outputPath != null) {
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
        }

        // Run job.  Wait for all mappers to complete.  We are running a zero
        // combiner and zero reducer configuration.
        // ------------------------------------------------------------------
        boolean success = job.waitForCompletion(true);
        if (! success) {
            return 1;
        }

        return 0;
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
	    int exitCode = ToolRunner.run(new H2ODriver(), args);
    	System.exit(exitCode);
    }
}
