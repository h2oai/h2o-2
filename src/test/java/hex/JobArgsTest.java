package hex;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.junit.*;

import water.Job.ValidatedJob;
import water.*;
import water.api.RequestServer;
import water.fvec.Frame;

public class JobArgsTest extends TestUtil {
  @BeforeClass public static void stall() {
//    stall_till_cloudsize(3);
  }

  @Test public void testIndexesVsNames() throws Exception {
    String[] names = new String[] { "5", "2", "1", "8", "4" };
    double[][] items = new double[10][names.length];
    for( int r = 0; r < items.length; r++ )
      for( int c = 0; c < items[r].length; c++ )
        items[r][c] = r;

    Key key = Key.make("test");
    Key dst = Key.make("dest");
    Frame frame = frame(names, items);
    UKV.put(key, frame);
    try {
      RequestServer.registerRequest(new JobArgsTestJob());

      HttpClient client = new HttpClient();
      String args = "" + //
          "destination_key=" + dst + "&" + //
          "source=" + key + "&" + //
          "response=2&" + //
          "cols=2,4";
      GetMethod get = new GetMethod("http://127.0.0.1:54321/JobArgsTestJob.html?" + args);
      Assert.assertEquals(200, client.executeMethod(get));
      for( ;; ) {
        get = new GetMethod("http://127.0.0.1:54321/Jobs.json");
        Assert.assertEquals(200, client.executeMethod(get));
        Res jobs = readJson(get.getResponseBodyAsString(), Res.class);
        //{"jobs":[{"key":"$03017f00000132d4ffffffff$3ef4c5ae-81bd-4ca4-95a9-29b1c0796ffa","description":"JobArgsTestJob","destination_key":"123","start_time":"2013-10-03T17:03:38-0700","end_time":"","progress":0.0,"cancelled":false}],"response":{"status":"done","h2o":"cypof","node":"/127.0.0.1:54321","time":1}}
        for( Job job : jobs.jobs )
          if( job.destination_key.equals(dst.toString()) )
            if( job.end_time.length() > 0 )
              break;
      }
    } finally {
      RequestServer.unregisterRequest(new JobArgsTestJob());
      UKV.remove(key);
    }
  }

  static class Res {
    Job[] jobs;
  }

  static class Job {
    String key;
    String destination_key;
    String end_time;
  }

  static class JobArgsTestJob extends ValidatedJob {
    @Override protected void exec() {
      // Assert col 2 & 4 have been selected
      Assert.assertEquals(source.vecs()[1], _train[0]);
      Assert.assertEquals(source.vecs()[3], _train[1]);
    }
  }
}
