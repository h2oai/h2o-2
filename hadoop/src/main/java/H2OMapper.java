import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Interesting Configuration properties:
 * mapper	mapred.local.dir=/tmp/hadoop-tomk/mapred/local/taskTracker/tomk/jobcache/job_local1117903517_0001/attempt_local1117903517_0001_m_000000_0
 */
public class H2OMapper extends Mapper<Text, Text, Text, Text> {
    /**
     * Emit a bunch of logging output at the beginning of the map task.
     * @param context Hadoop MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    private void emitLogHeader(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String mapredTaskId = conf.get("mapred.task.id");
        String id = mapredTaskId;
        Text textId = new Text(id);

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
        catch (java.net.UnknownHostException uhe) { // [beware typo in code sample -dmw]
            // handle exception
        }

        context.write(textId, new Text("----- Flat File -----"));
        BufferedReader reader = new BufferedReader(new FileReader("flatfile.txt"));
        String line = null;
        while ((line = reader.readLine()) != null) {
            context.write(textId, new Text(line));
        }
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

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        emitLogHeader(context);

        Counter counter = context.getCounter(H2O_MAPPER_COUNTER.HADOOP_COUNTER_HEARTBEAT);
        Thread counterThread = new CounterThread(counter);
        counterThread.start();

        int millis = 20 * 1000;
        Thread.sleep(millis);
    }
}
