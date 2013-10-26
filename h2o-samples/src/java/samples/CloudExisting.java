package samples;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.*;

import water.Job;
import water.deploy.LaunchJar;
import water.util.Utils;

/**
 * Runs a job on an existing cloud by injecting a jar at runtime.
 */
public class CloudExisting {
  /**
   * Build a jar file from project classes, and launch the job.
   */
  public static void launch(InetSocketAddress host, Class<? extends Job> job) throws Exception {
    File jar = File.createTempFile("h2o", ".jar");
    jar(jar, new File("target"));
    launch(host, job.getName(), jar);
  }

  /**
   * Upload jars to an existing cloud and launches a custom job. Uses the Web API.
   */
  public static void launch(InetSocketAddress host, String job, File... jars) throws Exception {
    HttpClient client = new HttpClient();
    String args = "job_class=" + job + "&jars=";
    for( File f : jars ) {
      PostMethod post = new PostMethod("http://127.0.0.1:54321/Upload.json?key=" + f.getName());
      Part[] parts = { new FilePart(f.getName(), f) };
      post.setRequestEntity(new MultipartRequestEntity(parts, post.getParams()));
      if( 200 != client.executeMethod(post) )
        throw new RuntimeException("Request failed: " + post.getStatusLine());
      args += f.getName() + ",";
    }

    String href = new LaunchJar().href();
    GetMethod get = new GetMethod("http://127.0.0.1:54321" + href + ".json?" + args);
    if( 200 != client.executeMethod(get) )
      throw new RuntimeException("Request failed: " + get.getStatusLine());
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
