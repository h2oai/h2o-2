package water.api;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.hadoop.fs.Path;

import tachyon.client.TachyonFS;
import tachyon.org.apache.thrift.TException;
import tachyon.thrift.ClientFileInfo;
import water.*;
import water.api.RequestServer.API_VERSION;
import water.fvec.*;
import water.persist.*;
import water.util.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ImportFiles2 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET =
    "Map a file from the source (either localhost filesystem, HDFS, or S3) into H2O memory.  Data is "+
    "loaded lazily, when the Key is read (usually in a Parse2 command, to build " +
    "a Frame key).  (Warning: Every host in the cluster must have this file visible locally!)";

  protected String parseLink(String k, String txt) { return Parse2.link(k, txt); }
  String parse() { return "Parse2.query"; }
  @Override
  public API_VERSION[] supportedVersions() { return SUPPORTS_ONLY_V2; }

  @API(help="Path to file/folder on either local disk/hdfs/s3",required=true,filter=GeneralFile.class,gridable=false)
  String path;

  @API(help="Common prefix for all successfully imported file keys")
  String prefix;

  @API(help="successfully imported files")
  String [] files;

  @API(help="keys of imported files")
  String [] keys;

  @API(help="files that failed to load")
  String [] fails;

  @API(help="Prior Keys that matched a prefix of the imported path, and were removed prior to (re)importing")
  String[] dels;

  public static Key[] importPath(String path){
    File f = new File(path);
    assert f.exists():"file not found: " + f.getAbsolutePath();
    ImportFiles2 imp = new ImportFiles2();
    imp.path = path;
    imp.serve();
    Key [] res = new Key[imp.keys.length];
    for(int i = 0; i < res.length; ++i)
      res[i] = Key.make(imp.keys[i]);
    return res;
  }
  /**
   * Iterates over fields and their annotations, and creates argument handlers.
   */
  @Override protected void registered(API_VERSION version) {
    super.registered(version);
  }
  @Override protected Response serve() {
    try{
      if(path != null){
        String p2 = path.toLowerCase();
        if( false ) ;
        else if( p2.startsWith("hdfs://" ) ) serveHdfs();
        else if( p2.startsWith("s3n://"  ) ) serveHdfs();
        else if( p2.startsWith("maprfs:/"  ) ) serveHdfs(); // "maprfs:/datasets" is legal
        else if( p2.startsWith("s3://"   ) ) serveS3();
        else if( p2.startsWith("http://" ) ) serveHttp();
        else if( p2.startsWith("https://") ) serveHttp();
        else if( p2.startsWith("tachyon://")) serveTachyon();
        else serveLocalDisk();
      }
      return Response.done(this);
    } catch( Throwable e ) {
      return Response.error(e);
    }
  }

  protected void serveHdfs() throws IOException{
    if (isBareS3NBucketWithoutTrailingSlash(path)) { path += "/"; }
    Log.info("ImportHDFS processing (" + path + ")");
    ArrayList<String> succ = new ArrayList<String>();
    ArrayList<String> fail = new ArrayList<String>();
    PersistHdfs.addFolder2(new Path(path), succ, fail);
    keys = succ.toArray(new String[succ.size()]);
    files = keys;
    fails = fail.toArray(new String[fail.size()]);
    this.prefix = getCommonPrefix(keys);
    DKV.write_barrier();
  }


   protected void serveS3(){
    Futures fs = new Futures();
    assert path.startsWith("s3://");
    path = path.substring(5);
    int bend = path.indexOf('/');
    if(bend == -1)bend = path.length();
    String bucket = path.substring(0,bend);
    String prefix = bend < path.length()?path.substring(bend+1):"";
    AmazonS3 s3 = PersistS3.getClient();
    if( !s3.doesBucketExist(bucket) )
      throw new IllegalArgumentException("S3 Bucket " + bucket + " not found!");;
    ArrayList<String> succ = new ArrayList<String>();
    ArrayList<String> fail = new ArrayList<String>();
    ObjectListing currentList = s3.listObjects(bucket, prefix);
    while(true){
      for(S3ObjectSummary obj:currentList.getObjectSummaries())
        try {
          succ.add(S3FileVec.make(obj,fs).toString());
        } catch( Throwable e ) {
          fail.add(obj.getKey());
          Log.err("Failed to loadfile from S3: path = " + obj.getKey() + ", error = " + e.getClass().getName() + ", msg = " + e.getMessage());
        }
      if(currentList.isTruncated())
        currentList = s3.listNextBatchOfObjects(currentList);
      else
        break;
    }
    keys = succ.toArray(new String[succ.size()]);
    files = keys;
    fails = fail.toArray(new String[fail.size()]);
    this.prefix = getCommonPrefix(keys);
  }

  private void serveLocalDisk() {
    File f = new File(path);
    if(!f.exists())throw new IllegalArgumentException("File " + path + " does not exist!");
    ArrayList<String> afiles = new ArrayList();
    ArrayList<String> akeys  = new ArrayList();
    ArrayList<String> afails = new ArrayList();
    ArrayList<String> adels  = new ArrayList();
    FileIntegrityChecker.check(f).syncDirectory(afiles,akeys,afails,adels);
    files = afiles.toArray(new String[0]);
    keys  = akeys .toArray(new String[0]);
    fails = afails.toArray(new String[0]);
    dels  = adels .toArray(new String[0]);
    prefix = getCommonPrefix(keys);
  }

  protected void serveHttp() {
    try {
      java.net.URL url = new URL(path);
      Key k = Key.make(path);
      InputStream is = url.openStream();
      if( is == null ) {
        Log.err("Unable to open stream to URL " + path);
      }

      UploadFileVec.readPut(k, is);
      fails = new String[0];
      String[] filesArr = { path };
      files = filesArr;
      String[] keysArr = { k.toString() };
      keys = keysArr;
      this.prefix = getCommonPrefix(keys);
    }
    catch( Throwable e) {
      String[] arr = { path };
      fails = arr;
      files = new String[0];
      keys = new String[0];
    }
  }

  private void serveTachyon() {
    assert path.startsWith(PersistTachyon.PREFIX) : "Path "+path+" is not prefixed by tachyon prefix " + PersistTachyon.PREFIX;
    TachyonFS client = null;
    ArrayList<String> succ = new ArrayList<String>();
    ArrayList<String> fail = new ArrayList<String>();
    try {
      String[] pathComponents = PersistTachyon.decode(path);
      String serverUri = pathComponents[0];
      // Be explicit, it would be possible to use default client URI, but better is throw an error
      if (serverUri==null || serverUri.isEmpty()) throw new IllegalArgumentException("The " + path + " is not legall URI - it is missing tachyon server URI (e.g., tachyon://localhost:19998/)." );
      client = ((PersistTachyon) Persist.I[Value.TACHYON]).createClient(PersistTachyon.PREFIX+serverUri);
      String rootFolder = pathComponents[1];
      List<ClientFileInfo> filesOnTFS= client.listStatus(rootFolder); // do a recursive descend
      Futures fs = new Futures();
      for (ClientFileInfo f : filesOnTFS ) {
        try {
          succ.add(TachyonFileVec.make(serverUri, f, fs).toString());
        } catch (Throwable t) {
          fail.add(f.getName());
          Log.err("Failed to loadfile from Tachyon: path = " + f.path + ", error = " + t.getClass().getName() + ", msg = " + t.getMessage());
        }
      }
      keys = succ.toArray(new String[succ.size()]);
      files = keys;
      fails = fail.toArray(new String[fail.size()]);
      this.prefix = getCommonPrefix(keys);
    } catch (IOException e) {
      fillEmpty("Cannot access specified file(s) on tachyon FS, because " + e.getMessage());
    } finally {
      if (client!=null) try { client.close(); } catch (TException _ ) {};
    }
  }

  private void fillEmpty(String failure) {
    fails = new String[] {failure};
    files = new String[0];
    keys = new String[0];
  }

  private String getCommonPrefix(String[] keys) {
    String prefix = new String();
    if(keys.length > 0) prefix = keys[0];

    for(int i = 1; i < keys.length; i++) {
      String tmp = keys[i];
      int j = 0;
      for(; j < Math.min(prefix.length(), tmp.length()); j++) {
        if(prefix.charAt(j) != tmp.charAt(j)) break;
      }
      prefix = prefix.substring(0, j);
    }
    return prefix;
  }

  // HTML builder
  @Override public boolean toHTML( StringBuilder sb ) {
    if(files == null)return false;
    if( files != null && files.length > 1 )
      sb.append("<div class='alert'>")
        .append(parseLink("*"+prefix+"*", "Parse all into hex format"))
        .append(" </div>");

    DocGen.HTML.title(sb,"files");
    DocGen.HTML.arrayHead(sb);
    for( int i=0; i<files.length; i++ )
      sb.append("<tr><td><a href='"+parse()+"?source_key=").append(keys[i]).
        append("'>").append(files[i]).append("</a></td></tr>");
    DocGen.HTML.arrayTail(sb);

    if( fails.length > 0 )
      DocGen.HTML.array(DocGen.HTML.title(sb,"fails"),fails);
    if( dels != null && dels.length > 0 )
      DocGen.HTML.array(DocGen.HTML.title(sb,"Keys deleted before importing"),dels);
    return true;
  }

  private boolean isBareS3NBucketWithoutTrailingSlash(String s) {
    Pattern p = Pattern.compile("s3n://[^/]*");
    Matcher m = p.matcher(s);
    boolean b = m.matches();
    return b;
  }
}
