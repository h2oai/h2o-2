package samples.launchers;

import java.io.*;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.*;

import water.Job;
import water.deploy.LaunchJar;
import water.deploy.VM;
import water.util.Utils;

/**
 * Runs a job on an existing cloud by injecting a jar.
 */
public class CloudConnect {
  /**
   * Build a jar file from project classes, and launch the job.
   */
  public static void launch(Class<? extends Job> job, String host) throws Exception {
    launch(job, host, new File(VM.h2oFolder(), "h2o-samples/target/classes"));
  }

  public static void launch(Class<? extends Job> job, String host, File classes) throws Exception {
    File jar = File.createTempFile("h2o", ".jar");
    jar(jar, classes);
    launch(job.getName(), host, jar);
  }

  /**
   * Upload jars to an existing cloud and launches a custom job. Uses the Web API.
   */
  public static void launch(String job, String host, File... jars) throws Exception {
    HttpClient client = new HttpClient();
    String args = "job_class=" + job + "&jars=";
    for( File f : jars ) {
      PostMethod post = new PostMethod("http://" + host + "/Upload.json?key=" + f.getName());
      Part[] parts = { new FilePart(f.getName(), f) };
      post.setRequestEntity(new MultipartRequestEntity(parts, post.getParams()));
      if( 200 != client.executeMethod(post) )
        throw new RuntimeException("Request failed: " + post.getStatusLine());
      args += f.getName() + ",";
      post.releaseConnection();
    }

    String href = new LaunchJar().href();
    GetMethod get = new GetMethod("http://" + host + href + ".json?" + args);
    if( 200 != client.executeMethod(get) )
      throw new RuntimeException("Request failed: " + get.getStatusLine());
    get.releaseConnection();
  }

  public static void jar(File jar, File... folders) {
    try {
      JarOutputStream stream = new JarOutputStream(new FileOutputStream(jar));
      for( File folder : folders )
        add(folder, folder, stream);
      stream.close();
    } catch( IOException e ) {
      throw new RuntimeException(e);
    }
  }

  private static void add(File root, File source, JarOutputStream target) throws IOException {
    String name = source.getPath().substring(root.getPath().length()).replace("\\", "/");
    if( name.startsWith("/") )
      name = name.substring(1);
    if( source.isDirectory() ) {
      if( !name.isEmpty() ) {
        if( !name.endsWith("/") )
          name += "/";
        JarEntry entry = new JarEntry(name);
        entry.setTime(source.lastModified());
        target.putNextEntry(entry);
        target.closeEntry();
      }
      for( File child : source.listFiles() )
        add(root, child, target);
    } else {
      JarEntry entry = new JarEntry(name);
      entry.setTime(source.lastModified());
      target.putNextEntry(entry);
      Utils.readFile(source, target);
      target.closeEntry();
    }
  }
}
