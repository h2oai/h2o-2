package hex;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.junit.Assert;

import water.Key;
import water.TestUtil;

public class HttpTest extends TestUtil {
  HttpClient _client = new HttpClient();

  public static class Get {
    public int _status;
    public Object _res;
  }

  public Get get(String uri, Class c) {
    GetMethod get = new GetMethod("http://127.0.0.1:54321/" + uri);
    Get res = new Get();
    try {
      res._status = _client.executeMethod(get);
      res._res = readJson(get.getResponseBodyAsString(), c);
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
    return res;
  }

  public void waitForJob(Key dst) throws Exception {
    for( ;; ) {
      Get get = get("Jobs.json", JobsRes.class);
      Assert.assertEquals(200, get._status);
      for( Job job : ((JobsRes) get._res).jobs )
        if( job.destination_key.equals(dst.toString()) )
          if( job.end_time.length() > 0 )
            return;
    }
  }

  public static class JobsRes {
    Job[] jobs;
  }

  public static class Job {
    String key;
    String destination_key;
    String end_time;
  }
}
