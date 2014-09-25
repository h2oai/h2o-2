package samples.expert;

import dontweave.gson.Gson;
import dontweave.gson.JsonElement;
import dontweave.gson.JsonObject;
import dontweave.gson.JsonParser;
import dontweave.gson.internal.Streams;
import dontweave.gson.stream.JsonWriter;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import water.util.Utils;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;


/**
 * Invokes H2O functionality through the Web API.
 */
public class WebAPI {
  static final String URL = "http://127.0.0.1:54321";
  static final File JSON_FILE = new File(Utils.tmp(), "model.json");

  public static void main(String[] args) throws Exception {
    listJobs();
    exportModel();
    importModel();
  }

  /**
   * Lists jobs currently running.
   */
  static void listJobs() throws Exception {
    HttpClient client = new HttpClient();
    GetMethod get = new GetMethod(URL + "/Jobs.json");
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

  /**
   * Exports a model to a JSON file.
   */
  static void exportModel() throws Exception {
    HttpClient client = new HttpClient();
    GetMethod get = new GetMethod(URL + "/2/ExportModel.json?model=MyInitialNeuralNet");
    int status = client.executeMethod(get);
    if( status != 200 )
      throw new Exception(get.getStatusText());
    JsonObject response = (JsonObject) new JsonParser().parse(new InputStreamReader(get.getResponseBodyAsStream()));
    JsonElement model = response.get("model");
    JsonWriter writer = new JsonWriter(new FileWriter(JSON_FILE));
    writer.setLenient(true);
    writer.setIndent("  ");
    Streams.write(model, writer);
    writer.close();
    get.releaseConnection();
  }

  /**
   * Imports a model from a JSON file.
   */
  public static void importModel() throws Exception {
    // Upload file to H2O
    HttpClient client = new HttpClient();
    PostMethod post = new PostMethod(URL + "/Upload.json?key=" + JSON_FILE.getName());
    Part[] parts = { new FilePart(JSON_FILE.getName(), JSON_FILE) };
    post.setRequestEntity(new MultipartRequestEntity(parts, post.getParams()));
    if( 200 != client.executeMethod(post) )
      throw new RuntimeException("Request failed: " + post.getStatusLine());
    post.releaseConnection();

    // Parse the key into a model
    GetMethod get = new GetMethod(URL + "/2/ImportModel.json?" //
        + "destination_key=MyImportedNeuralNet&" //
        + "type=NeuralNetModel&" //
        + "json=" + JSON_FILE.getName());
    if( 200 != client.executeMethod(get) )
      throw new RuntimeException("Request failed: " + get.getStatusLine());
    get.releaseConnection();
  }
}
