package hex;

import java.io.InputStreamReader;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.junit.Ignore;
import org.junit.Test;

import water.Key;
import water.TestUtil;

import dontweave.gson.Gson;

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
      if( res._status == 200 ) {
        Gson gson = new Gson();
        res._res = gson.fromJson(new InputStreamReader(get.getResponseBodyAsStream()), c);
      }
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
    get.releaseConnection();
    return res;
  }

  public String waitForJob(Key dst) throws Exception {
    for( ;; ) {
      boolean exists = false;
      Get get = get("Jobs.json", JobsRes.class);
      assert get._status == 200;
      if( ((JobsRes) get._res).jobs != null ) {
        for( Job job : ((JobsRes) get._res).jobs ) {
          if( job.destination_key != null && job.destination_key.equals(dst.toString()) ) {
            exists = true;
            if( job.end_time != null && job.end_time.length() > 0 )
              return job.exception != null && job.exception.length() > 0 ? job.exception : null;
          }
        }
      }
      if( !exists )
        return null;
      Thread.sleep(100);
    }
  }

  public static class JobsRes {
    Job[] jobs;
  }

  public static class Job {
    String key;
    String destination_key;
    String end_time;
    String exception;
  }

  @Test @Ignore public void dummy_test() {
    /* this is just a dummy test to avoid JUnit complains about missing test */
  }
}
