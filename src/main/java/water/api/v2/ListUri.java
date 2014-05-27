package water.api.v2;

import java.io.*;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.io.Closeables;
import com.google.gson.*;

import water.*;
import water.api.Parse;
import water.api.Request;
import water.api.RequestArguments.GeneralFile;
import water.api.RequestArguments.Str;
import water.api.RequestBuilders.*;
import water.api.RequestServer.API_VERSION;
import water.persist.PersistHdfs;
import water.persist.PersistS3;
import water.util.FileIntegrityChecker;
import water.util.Log;

public class ListUri extends Request {
  private final Str _uri = new Str("uri");
  private final Str _sourceType = new Str("source_type");
  private String dstTime = "";

  @Override protected Response serve() {

    DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
    Date date = new Date();
    dstTime = "_"+dateFormat.format(date);

    if (_sourceType.value().equals("s3")){
      return uploadS3();
    }else if (_sourceType.value().equals("http")){
      return uriUpload();
    }else  if (_sourceType.value().equals("hdfs")){
        return uploadHdfs();
    }else if (_sourceType.value().equals("cluster")){
        return uploadPath();
    }

    JsonObject json = new JsonObject();
    json.add("uris", new JsonPrimitive(_uri.value()));
    Response r = Response.done(json);

    return r;
  }

  @Override protected String href(API_VERSION v) {
    return v.prefix() + "list_uri";
  }

  private Response uriUpload(){
    InputStream s = null;
    String urlStr = _uri.value();
    try {
       if( urlStr.startsWith("file:///") ) {
        urlStr = urlStr.substring("file:///".length());
        File f = new File(urlStr);
        urlStr = "file:///"+f.getCanonicalPath();
      }
      URL url = new URL(urlStr);
      Key k = Key.make(urlStr);
      s = url.openStream();
      if( s == null )
        return Response.error("Unable to open stream to URL "+url.toString());
      ValueArray.readPut(k, s);
      JsonObject json = new JsonObject();
      JsonArray urisArray = new JsonArray();
      JsonObject jObject = new JsonObject();
      jObject.add("uri", new JsonPrimitive(k.toString()));
      jObject.add("size", new JsonPrimitive(0));
      urisArray.add(jObject);
      json.add("uris", urisArray);
      String fileName = k.toString().substring( k.toString().lastIndexOf('/')+1, k.toString().length() );
      String fileNameWithoutExtn = fileName.substring(0, fileName.lastIndexOf('.'));
      //json.add("dst", new JsonPrimitive(k.make().toString()));
      json.add("dst", new JsonPrimitive(fileNameWithoutExtn+dstTime+".hex"));

      Response r = Response.custom(json);
      r.setBuilder(KEY, new KeyElementBuilder());
      return r;
    } catch( IllegalArgumentException e ) {
      return Response.error("Not a valid key: "+ urlStr);
    } catch( IOException e ) {
      return Response.error(e);
    } finally {
      Closeables.closeQuietly(s);
    }
  }

  private Response uploadHdfs(){
      String pstr = _uri.value();
      if (isBareS3NBucketWithoutTrailingSlash(_uri.value())) { pstr = pstr + "/"; }
      Log.info("ImportHDFS processing (" + pstr + ")");
      JsonArray succ = new JsonArray();
      JsonArray fail = new JsonArray();
      try {
        PersistHdfs.addFolder(new Path(pstr), succ, fail);
      } catch( IOException e ) {
        return Response.error(e);
      }
      DKV.write_barrier();
      JsonArray respArray = new JsonArray();
      JsonObject tmpObj = new JsonObject();
      JsonObject tmpObj2 = new JsonObject();
      String k = "";
      for (int i=0;i<succ.size();i++){
        tmpObj = (JsonObject) succ.get(i);
        if (i==0) k = tmpObj.get("key").getAsString();
        tmpObj2 = new JsonObject();
        tmpObj2.add("uri", tmpObj.get("key"));
        tmpObj2.add("size", tmpObj.get("value_size_bytes"));
        respArray.add(tmpObj2);
      }


      JsonObject json = new JsonObject();
      json.add("uris", respArray);
      //json.add("dst", new JsonPrimitive(Key.make().toString()));
      String fileName = k.toString().substring( k.toString().lastIndexOf('/')+1, k.toString().length() );
      String fileNameWithoutExtn = fileName.substring(0, fileName.lastIndexOf('.'));
      json.add("dst", new JsonPrimitive(fileNameWithoutExtn+dstTime+".hex"));
      Response r = Response.custom(json);
      return r;
  }


  boolean isBareS3NBucketWithoutTrailingSlash(String s) {
    Pattern p = Pattern.compile("s3n://[^/]*");
    Matcher m = p.matcher(s);
    boolean b = m.matches();
    return b;
  }

  private Response uploadPath(){
    ArrayList<String> afiles = new ArrayList();
    ArrayList<String> akeys  = new ArrayList();
    ArrayList<String> afails = new ArrayList();
    ArrayList<String> adels  = new ArrayList();
    FileIntegrityChecker.check(new File(_uri.value()),false).syncDirectory(afiles,akeys,afails,adels);
    JsonObject json = new JsonObject();
    JsonArray urisArray = new JsonArray();
    String k = "";
    for (int i=0;i<afiles.size();i++){
      JsonObject jObject = new JsonObject();
      if(i==0) k=akeys.get(i);
      jObject.add("uri", new JsonPrimitive(akeys.get(i)));
      jObject.add("size", new JsonPrimitive(0));
      urisArray.add(jObject);
    }
    json.add("uris", urisArray);
    //json.add("dst", new JsonPrimitive(Key.make().toString()));
    String fileName = k.toString().substring( k.toString().lastIndexOf('/')+1, k.toString().length() );
    String fileNameWithoutExtn = fileName.substring(0, fileName.lastIndexOf('.'));
    json.add("dst", new JsonPrimitive(fileNameWithoutExtn+dstTime+".hex"));
    return Response.custom(json);
  }

  private Response uploadS3(){
    String bucket = _uri.value();
    if (bucket.startsWith("s3://"))
      bucket = bucket.substring(5);
    Log.info("ImportS3 processing (" + bucket + ")");
    JsonObject json = new JsonObject();
    JsonArray succ = new JsonArray();
    JsonArray fail = new JsonArray();
    AmazonS3 s3 = PersistS3.getClient();
    ObjectListing currentList = s3.listObjects(bucket);
    processListing(currentList, succ, fail);
    while(currentList.isTruncated()){
      currentList = s3.listNextBatchOfObjects(currentList);
      processListing(currentList, succ, fail);
    }
    json.add("uris", succ);
    //json.add("dst", new JsonPrimitive(Key.make().toString()));
    String k = ((JsonObject)succ.get(0)).get("uri").toString();
    String fileName = k.toString().substring( k.toString().lastIndexOf('/')+1, k.toString().length() );
    String fileNameWithoutExtn = fileName.substring(0, fileName.lastIndexOf('.'));
    json.add("dst", new JsonPrimitive(fileNameWithoutExtn+dstTime+".hex"));
    DKV.write_barrier();
    Response r = Response.custom(json);
    //r.setBuilder(SUCCEEDED + "." + KEY, new KeyCellBuilder());
    return r;

  }

  public void processListing(ObjectListing listing, JsonArray succ, JsonArray fail){
    for( S3ObjectSummary obj : listing.getObjectSummaries() ) {
      try {
        Key k = PersistS3.loadKey(obj);
        JsonObject o = new JsonObject();
        o.addProperty("uri", k.toString());
        //o.addProperty("uri", obj.getKey());
        o.addProperty(VALUE_SIZE, obj.getSize());
        succ.add(o);
      } catch( IOException e ) {
        JsonObject o = new JsonObject();
        o.addProperty(FILE, obj.getKey());
        o.addProperty(ERROR, e.getMessage());
        fail.add(o);
      }
    }
  }
}
