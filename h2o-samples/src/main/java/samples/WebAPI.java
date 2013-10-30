package samples;

import java.io.InputStreamReader;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;

import com.google.gson.Gson;

/**
 * Invokes an H2O functionality through the Web API.
 */
public class WebAPI {
  public static void main(String[] args) throws Exception {
    HttpClient client = new HttpClient();
    GetMethod get = new GetMethod("http://127.0.0.1:54321/Jobs.json");
    int status = client.executeMethod(get);
    if( status != 200 )
      throw new Exception(get.getStatusText());
    Gson gson = new Gson();
    JobsRes res = gson.fromJson(new InputStreamReader(get.getResponseBodyAsStream()), JobsRes.class);
    System.out.println("Running jobs:");
    for( Job job : res.jobs )
      System.out.println(job.description + " " + job.destination_key);
    get.releaseConnection();
  }

  public static class JobsRes {
    Job[] jobs;
  }

  public static class Job {
    String key;
    String description;
    String destination_key;
    String end_time;
    String exception;
  }
}
