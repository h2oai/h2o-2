import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import water.H2O;

/**
 * Interesting Configuration properties:
 * mapper	mapred.local.dir=/tmp/hadoop-tomk/mapred/local/taskTracker/tomk/jobcache/job_local1117903517_0001/attempt_local1117903517_0001_m_000000_0
 */
public class H2OMapper extends Mapper<Text, Text, Text, Text> {
    static Context _context;       // Hadoop mapreduce context
    static String _mapredTaskId;

    /**
     * Emit a bunch of logging output at the beginning of the map task.
     * @throws IOException
     * @throws InterruptedException
     */
    private void emitLogHeader() throws IOException, InterruptedException {
        Configuration conf = _context.getConfiguration();
        Text textId = new Text(_mapredTaskId);

        for (Map.Entry<String, String> entry: conf) {
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            _context.write(textId, new Text(sb.toString()));
        }

        _context.write(textId, new Text("----- Properties -----"));
        String[] plist = {
                "mapred.local.dir",
                "mapred.child.java.opts",
        };
        for (String k : plist) {
            String v = conf.get(k);
            if (v == null) {
                v = "(null)";
            }
            _context.write(textId, new Text(k + " " + v));
        }
        String userDir = System.getProperty("user.dir");
        _context.write(textId, new Text("user.dir " + userDir));

        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            _context.write(textId, new Text("hostname " + localMachine.getHostName()));
        }
        catch (java.net.UnknownHostException uhe) { // [beware typo in code sample -dmw]
            // handle exception
        }

        _context.write(textId, new Text("----- Flat File -----"));
        BufferedReader reader = new BufferedReader(new FileReader("flatfile.txt"));
        String line;
        while ((line = reader.readLine()) != null) {
            _context.write(textId, new Text(line));
        }
        _context.write(textId, new Text("---------------------"));
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
        Counter _counter;
        final int ONE_MINUTE_MILLIS = 60 * 1000;

        CounterThread (Counter counter) {
            _counter = counter;
        }

        @Override
        public void run() {
            while (true) {
                _counter.increment(1);
                try {
                    Thread.sleep (ONE_MINUTE_MILLIS);
                }
                catch (Exception e) {}
            }
        }
    }

    public static class UserMain {
        /**
         * Start an H2O instance in the local JVM.
         */
        public static void main(String[] args) throws Exception {
            Text textId = new Text(_mapredTaskId);
            _context.write(textId, new Text("before H2O.main()"));
            H2O.main(args);
            _context.write(textId, new Text("after H2O.main()"));
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
//        try {
//            String[] args = {};
//            water.Boot.main(UserMain.class, args);
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }

        _context = context;
        Configuration conf = context.getConfiguration();
        _mapredTaskId = conf.get("mapred.task.id");
        Text textId = new Text(_mapredTaskId);

        emitLogHeader();

        Counter counter = context.getCounter(H2O_MAPPER_COUNTER.HADOOP_COUNTER_HEARTBEAT);
        Thread counterThread = new CounterThread(counter);
        counterThread.start();

        String ice_root = conf.get("mapred.local.dir");
        _context.write(textId, new Text("mapred.local.dir is " + ice_root));

        String[] args = {
//            "-ice_root", ice_root,
            "-flatfile", "flatfile.txt",
            "-port", "54321",
        };

        _context.write(textId, new Text("before water.Boot.main()"));
        try {
            water.Boot.main(UserMain.class, args);
        }
        catch (Exception e) {
            _context.write(textId, new Text("exception in water.Boot.main()"));

            String s = e.getMessage();
            if (s == null) {
                s = "(null exception message)";
            }
            _context.write(textId, new Text(s));

            s = e.toString();
            if (s == null) {
                s = "(null exception toString)";
            }
            _context.write(textId, new Text(s));

            StackTraceElement[] els = e.getStackTrace();
            for (int i = 0; i < els.length; i++) {
                StackTraceElement el = els[i];
                s = el.toString();
                _context.write(textId, new Text("    " + s));
            }
        }
        finally {
            _context.write(textId, new Text("after water.Boot.main()"));

            while (true) {
                int FIVE_SECONDS_MILLIS = 5 * 1000;
                Thread.sleep (FIVE_SECONDS_MILLIS);
            }
        }
    }

    public static void main (String[] args) {
        try {
            H2OMapper m = new H2OMapper();
            m.run(null);
        }
        catch (Exception e) {
            System.out.println (e);
        }
    }
}
