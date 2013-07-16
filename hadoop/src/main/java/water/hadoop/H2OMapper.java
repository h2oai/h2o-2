package water.hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import water.H2O;

import water.util.Log;


/**
 * Interesting Configuration properties:
 * mapper	mapred.local.dir=/tmp/hadoop-tomk/mapred/local/taskTracker/tomk/jobcache/job_local1117903517_0001/attempt_local1117903517_0001_m_000000_0
 */
public class h2omapper extends Mapper<Text, Text, Text, Text> {
    /**
     * Start an H2O instance in the local JVM.
     */
    public static class UserMain {
        public static void main(String[] args) {
            Log.POST(30, "Entered UserMain");
            Log.POST(31, "built textId");
            try {
                Log.POST(32, "top of try");
                H2O.main(args);
                Log.POST(33, "after H2O.main");
            }
            catch (Exception e) {
                Log.POST(37, "exception occurred");
                try {
                    e.printStackTrace();
                }
                catch (Exception _) {
                    System.err.println("_context.write excepted in UserMain");
                    _.printStackTrace();
                }
            }
            finally {
                Log.POST(38, "top of finally");
                Log.POST(38, "bottom of finally");
            }
            Log.POST(39, "leaving UserMain");
        }
    }

    /**
     * Emit a bunch of logging output at the beginning of the map task.
     * @throws IOException
     * @throws InterruptedException
     */
    private void emitLogHeader(Context context, String mapredTaskId) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Text textId = new Text(mapredTaskId);

        for (Map.Entry<String, String> entry: conf) {
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            context.write(textId, new Text(sb.toString()));
        }

        context.write(textId, new Text("----- Properties -----"));
        String[] plist = {
                "mapred.local.dir",
                "mapred.child.java.opts",
        };
        for (String k : plist) {
            String v = conf.get(k);
            if (v == null) {
                v = "(null)";
            }
            context.write(textId, new Text(k + " " + v));
        }
        String userDir = System.getProperty("user.dir");
        context.write(textId, new Text("user.dir " + userDir));

        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            context.write(textId, new Text("hostname " + localMachine.getHostName()));
        }
        catch (java.net.UnknownHostException uhe) {
            // handle exception
        }

        context.write(textId, new Text("----- Flat File -----"));
        BufferedReader reader = new BufferedReader(new FileReader("flatfile.txt"));
        String line;
        while ((line = reader.readLine()) != null) {
            context.write(textId, new Text(line));
        }
        context.write(textId, new Text("---------------------"));
    }

    /**
     * Identify hadoop mapper counter
     */
    public static enum H2O_MAPPER_COUNTER {
        HADOOP_COUNTER_HEARTBEAT
    }

    /**
     * Hadoop heartbeat keepalive thread.  Periodically update a counter so that
     * jobtracker knows not to kill the job.
     */
    public class CounterThread extends Thread {
        Context _context;
        Counter _counter;
        final int TEN_SECONDS_MILLIS = 10 * 1000;

        CounterThread (Context context, Counter counter) {
            _context = context;
            _counter = counter;
        }

        @Override
        public void run() {
            while (true) {
                _context.progress();
                _counter.increment(1);
                try {
                    Thread.sleep (TEN_SECONDS_MILLIS);
                }
                catch (Exception e) {}
            }
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Log.POST(0, "Entered run");

        Configuration conf = context.getConfiguration();
        String mapredTaskId = conf.get("mapred.task.id");
        Text textId = new Text(mapredTaskId);

        emitLogHeader(context, mapredTaskId);
        Log.POST(10, "After emitLogHeader");

        Counter counter = context.getCounter(H2O_MAPPER_COUNTER.HADOOP_COUNTER_HEARTBEAT);
        Thread counterThread = new CounterThread(context, counter);
        counterThread.start();

        String ice_root = conf.get("mapred.local.dir");
        String jobtrackerName = conf.get("hexdata.jobtrackername");
        context.write(textId, new Text("mapred.local.dir is " + ice_root));

        String[] args = {
            "-ice_root", ice_root,
            "-flatfile", "flatfile.txt",
            "-port", "54321",
            "-inherit_log4j",
            "-name", jobtrackerName
        };

        context.write(textId, new Text("before water.Boot.main()"));
        try {
            Log.POST(11, "Before boot");
            water.Boot.main(UserMain.class, args);
            Log.POST(12, "After boot");
        }
        catch (Exception e) {
            Log.POST(13, "Exception in boot");
            context.write(textId, new Text("exception in water.Boot.main()"));

            String s = e.getMessage();
            if (s == null) { s = "(null exception message)"; }
            context.write(textId, new Text(s));

            s = e.toString();
            if (s == null) { s = "(null exception toString)"; }
            context.write(textId, new Text(s));

            StackTraceElement[] els = e.getStackTrace();
            for (int i = 0; i < els.length; i++) {
                StackTraceElement el = els[i];
                s = el.toString();
                context.write(textId, new Text("    " + s));
            }
        }
        finally {
            Log.POST(14, "Top of finally");
            context.write(textId, new Text("after water.Boot.main()"));
        }

        Log.POST(15, "Entering wait loop");
        while (true) {
            int ONE_MINUTE_MILLIS = 60 * 1000;
            Thread.sleep (ONE_MINUTE_MILLIS);
//            break;
        }

//        Log.POST(1000, "Leaving run");
//        System.exit (0);
    }

    /**
     * For debugging only.
     */
    public static void main (String[] args) {
        try {
            h2omapper m = new h2omapper();
            m.run(null);
        }
        catch (Exception e) {
            System.out.println (e);
        }
    }
}
