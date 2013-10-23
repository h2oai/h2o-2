package water.api;

import java.io.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import com.google.gson.JsonArray;
import com.google.gson.internal.Streams;

import water.*;
import water.api.RequestBuilders.Response;
import water.api.RequestServer.API_VERSION;
import water.persist.PersistHdfs;
import water.util.FileIntegrityChecker;
import water.util.Log;

public class ImportFiles2 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET =
    "Map a file from the source (either local host filesystem,hdfs, or s3) into H2O memory.  Data is "+
    "loaded lazily, when the Key is read (usually in a Parse2 command, to build " +
    "a Frame key).  (Warning: Every host in the cluster must have this file visible locally!)";

  protected String parseLink(String k, String txt) { return Parse2.link(k, txt); }
  String parse() { return "Parse2.query"; }
  @Override
  public API_VERSION[] supportedVersions() { return SUPPORTS_ONLY_V2; }

  @API(help="Path to file/folder on either local disk/hdfs/s3",required=true,filter=GeneralFile.class)
  String path;


  @API(help="successfully imported files")
  String [] files;

  @API(help="keys of imported files")
  String [] keys;

  @API(help="files that failed to load")
  String [] fails;

  /**
   * Iterates over fields and their annotations, and creates argument handlers.
   */
  @Override protected void registered(API_VERSION version) {
    super.registered(version);
  }
  @Override protected Response serve() {
    if(path != null){
      String p2 = path.toLowerCase();
      if(p2.startsWith("hdfs://") || p2.startsWith("s3n://"))serveHdfs();
      else if(p2.startsWith("s3://")) serveS3();
      else serveLocalDisk();
    }
    return new Response(Response.Status.done, this, -1, -1, null);
  }

  protected void serveHdfs(){
    if (isBareS3NBucketWithoutTrailingSlash(path)) { path += "/"; }
    Log.info("ImportHDFS processing (" + path + ")");

    try {
      PersistHdfs.addFolder(new Path(path), succ, fail);
    } catch( IOException e ) {
      StringBuilder sb = new StringBuilder();
      PrintWriter pw = new PrintWriter(Streams.writerForAppendable(sb));
      e.printStackTrace(pw);
      pw.flush();
      Log.err(e);
      return Response.error(sb.toString());
    }
    DKV.write_barrier();

  }
  protected void serveS3(){
    throw H2O.unimpl();
  }
  protected void serveLocalDisk(){
    File f = new File(path);
    if(!f.exists())throw new IllegalArgumentException("File " + path + " does not exist!");
    FileIntegrityChecker c = FileIntegrityChecker.check(new File(path),true);
    ArrayList<String> afails = new ArrayList();
    ArrayList<String> afiles = new ArrayList();
    ArrayList<String> akeys  = new ArrayList();
    Futures fs = new Futures();
    for( int i = 0; i < c.size(); ++i ) {
      Key k = c.importFile(i, fs);
      if( k == null ) {
        afails.add(c.getFileName(i));
      } else {
        afiles.add(c.getFileName(i));
        akeys .add(k.toString());
      }
    }
    fs.blockForPending();
    fails = afails.toArray(new String[0]);
    files = afiles.toArray(new String[0]);
    keys  = akeys .toArray(new String[0]);
  }

  // HTML builder
  @Override public boolean toHTML( StringBuilder sb ) {
    if( files.length > 1 )
      sb.append("<div class='alert'>")
        .append(parseLink("*"+path+"*", "Parse all into hex format"))
        .append(" </div>");

    DocGen.HTML.title(sb,"files");
    DocGen.HTML.arrayHead(sb);
    for( int i=0; i<files.length; i++ )
      sb.append("<tr><td><a href='"+parse()+"?source_key=").append(keys[i]).
        append("'>").append(files[i]).append("</a></td></tr>");
    DocGen.HTML.arrayTail(sb);

    if( fails.length > 0 )
      DocGen.HTML.array(DocGen.HTML.title(sb,"fails"),fails);
    return true;
  }

  private boolean isBareS3NBucketWithoutTrailingSlash(String s) {
    Pattern p = Pattern.compile("s3n://[^/]*");
    Matcher m = p.matcher(s);
    boolean b = m.matches();
    return b;
  }
}
