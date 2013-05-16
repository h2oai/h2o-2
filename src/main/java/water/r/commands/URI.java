package water.r.commands;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import water.*;
import water.store.s3.PersistS3;
import water.util.FileIntegrityChecker;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * WORK IN PROGRESS.
 *
 * The URI class represents a file name that we can import. We support a bunch and do a modicum of
 * sanity checking. We could do more.
 *
 * THIS CODE IS PROBABLY WRONG AND WILL CHANGE
 */
public abstract class URI {

  String path;

  public static URI make(String s) throws FormatError {
    URI res = null;
    if( s.startsWith("file://") ) {
      String p = s.replaceFirst("file://", "");
      res = new File(p);
    } else if( s.startsWith("hdfs://") ) {
      String p = s.replaceFirst("hdfs://", "");
      res = new S3(p);
    } else if( s.startsWith("s3://") ) {
      String p = s.replaceFirst("s3://", "");
      res = new S3(p);
    } else if( s.startsWith("s3n://") ) {
      String p = s.replaceFirst("s3n://", "");
      res = new S3n(p);
    } else if( s.startsWith("http://") ) {
      String p = s.replaceFirst("http://", "");
      res = new Http(p);
    } else if( !s.contains("://") ) {
      res = new File(s);
    }
    if( res == null ) throw new FormatError("Not a valid URI:" + s);
    else return res;
  }

  public static class FormatError extends Error {
    FormatError(String s) {
      super(s);
    }
  }

  public abstract ValueArray get() throws IOException;

}

class File extends URI {

  File(String p) throws FormatError {
    path = p;
  }

  public ValueArray get() {
    FileIntegrityChecker c = FileIntegrityChecker.check(new java.io.File(path));
    Futures fs = new Futures();
    Key k = c.importFile(0, fs);
    fs.blockForPending();
    return DKV.get(k).<ValueArray> get();
  }
  public String toString() { return path; }
}

class S3 extends URI {
  String bucket;

  S3(String p) throws FormatError {
    if( !p.contains("/") ) throw new FormatError("This is not a valid S3 name: " + p);
    String[] split = p.split("/", 2);
    assert split.length == 2;
    bucket = split[0];
    path = split[1];
  }

  public ValueArray get() throws IOException {
    AmazonS3 s3 = PersistS3.getClient();
    List<S3ObjectSummary> l = s3.listObjects(bucket, path).getObjectSummaries();
    assert l.size() == 1;
    S3ObjectSummary obj = l.get(0);
    Key k = PersistS3.loadKey(obj);
    return  DKV.get(k).<ValueArray> get();
  }
  public String toString() { return "s3://"+bucket+"/"+path; }

}

class S3n extends S3 {
  S3n(String p) throws FormatError {
    super(p);
  }
  public String toString() { return "s3n://"+bucket+"/"+path; }

}

class Http extends URI {

  Http(String p) throws FormatError {
    path = p;
  }

  public ValueArray get() throws IOException {
    String p = "http://" + path;
    URL url = new URL(p);
    Key k = Key.make(p);
    InputStream s = url.openStream();
    if( s == null ) throw new FormatError("argh");
    ValueArray.readPut(k, s);
    return  DKV.get(k).<ValueArray> get();
  }

  public String toString() { return "http://"+path; }
}
