package water.api;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import water.*;
import water.api.RequestServer.API_VERSION;
import water.fvec.Frame;
import water.persist.PersistHdfs;
import water.util.Log;

import java.io.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExportFiles extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET =
    "Export a Frame from H2O onto a file system (local disk or HDFS).";

  @Override
  public API_VERSION[] supportedVersions() { return SUPPORTS_ONLY_V2; }

  @API(help="Key to an existing H2O Frame.", required=true,filter=Default.class)
  Key src_key;

  @API(help="Path to a file on either local disk of connected node or HDFS.",required=true,filter=GeneralFile.class,gridable=false)
  String path;

  public static String link(Key k, String content){
    return  "<a href='/2/ExportFiles.html?src_key=" + k.toString() + "'>" + content + "</a>";
  }

   /**
   * Iterates over fields and their annotations, and creates argument handlers.
   */
  boolean _local = false;
  @Override protected void registered(API_VERSION version) { super.registered(version); }
  protected Response serve() {
    try {
      // pull everything local
      Log.info("ExportFiles processing (" + path + ")");
      if (DKV.get(src_key) == null) throw new IllegalArgumentException(src_key.toString() + " not found.");
      Object value = DKV.get(src_key).get();
      // create a stream to read the entire VA or Frame
      if (!(value instanceof ValueArray) && !(value instanceof Frame)) throw new UnsupportedOperationException("Can only export Frames or ValueArrays.");
      InputStream csv = value instanceof ValueArray ? new ValueArray.CsvVAStream((ValueArray) value, null) : ((Frame) value).toCSV(true);
      String p2 = path.toLowerCase();
      if( p2.startsWith("hdfs://" ) ) serveHdfs(csv);
      else if( p2.startsWith("s3n://"  ) ) serveHdfs(csv);
      else serveLocalDisk(csv);

      return RequestBuilders.Response.done(this);
    } catch (Throwable t) {
      return RequestBuilders.Response.error(t);
    }
  }

  protected void serveHdfs(InputStream csv) throws IOException {
    if (isBareS3NBucketWithoutTrailingSlash(path)) { path += "/"; }
    ArrayList<String> succ = new ArrayList<String>();
    ArrayList<String> fail = new ArrayList<String>();
    Path p = new Path(path);
    PersistHdfs.addFolder2(p, succ, fail);

    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(p.toUri(), PersistHdfs.CONF);
    fs.mkdirs(p.getParent());

    FSDataOutputStream s = fs.create(p);
    byte[] buffer = new byte[1024];
    try {
      int len;
      while ((len = csv.read(buffer)) > 0) {
        s.write(buffer, 0, len);
      }
    } finally {
      s.close();
      Log.info("Key '" + src_key.toString() +  "' was written to " + path.toString());
    }
  }

  private void serveLocalDisk(InputStream csv) throws IOException {
    _local = true;
    OutputStream output = null;
    try {
      File f = new File(path);
      if( f.exists() ) throw new IllegalArgumentException("File " + path + " already exists.");
      output = new FileOutputStream(path.toString());
      byte[] buffer = new byte[1024];
      int len;
      while((len = csv.read(buffer)) > 0) {
        output.write(buffer, 0, len);
      }
      Log.info("Key '" + src_key.toString() +  "' was written to " + (_local && H2O.CLOUD.size() > 1 ? H2O.SELF_ADDRESS + ":" : "") + path.toString());
    } finally {
      if (output != null) output.close();
    }
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    DocGen.HTML.section(sb, "Export done. Key '" + src_key.toString() +
            "' was written to " + (_local && H2O.CLOUD.size() > 1 ? H2O.SELF_ADDRESS + ":" : "") + path.toString());
    return true;
  }

  private boolean isBareS3NBucketWithoutTrailingSlash(String s) {
    Pattern p = Pattern.compile("s3n://[^/]*");
    Matcher m = p.matcher(s);
    boolean b = m.matches();
    return b;
  }

}
