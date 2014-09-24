
package water.api;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import water.persist.PersistHdfs;
import water.persist.PersistS3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.google.common.base.Strings;
import dontweave.gson.JsonArray;
import dontweave.gson.JsonPrimitive;

public class TypeaheadFileRequest extends TypeaheadRequest {

  public TypeaheadFileRequest() {
    super("Provides a simple JSON array of filtered local files.","");
  }

  protected JsonArray serveFile(String filter, int limit){
    File base = null;
    String filterPrefix = "";
    if( !filter.isEmpty() ) {
      File file = new File(filter);
      if( file.isDirectory() ) {
        base = file;
      } else {
        base = file.getParentFile();
        filterPrefix = file.getName().toLowerCase();
      }
    }
    if( base == null ) base = new File(".");

    JsonArray array = new JsonArray();
    File[] files = base.listFiles();
    if( files == null ) return array;
    for( File file : files ) {
      if( file.isHidden() ) continue;
      if( file.getName().toLowerCase().startsWith(filterPrefix) )
        array.add(new JsonPrimitive(file.getPath()));
      if( array.size() == limit) break;
    }
    return array;
  }
  protected JsonArray serveHdfs(String filter, int limit){
    JsonArray array = new JsonArray();
    Configuration conf = PersistHdfs.CONF;
    if( conf == null ) return array;
    try {
      Path p = new Path(filter);
      Path expand = p;
      if( !filter.endsWith("/") ) expand = p.getParent();
      FileSystem fs = FileSystem.get(p.toUri(), conf);
      for( FileStatus file : fs.listStatus(expand) ) {
        Path fp = file.getPath();
        if( fp.toString().startsWith(p.toString()) ) {
          array.add(new JsonPrimitive(fp.toString()));
        }
        if( array.size() == limit) break;
      }
    } catch( Throwable xe ) { }
    return array;
  }
  protected JsonArray serveS3(String filter, int limit){
    JsonArray array = new JsonArray();
    try {
      AmazonS3 s3 = PersistS3.getClient();
      filter = Strings.nullToEmpty(filter);
      for( Bucket b : s3.listBuckets() ) {
        if( b.getName().startsWith(filter) )
          array.add(new JsonPrimitive(b.getName()));
        if( array.size() == limit) break;
      }
    } catch( IllegalArgumentException xe ) { }
    return array;
  }

  @Override final protected JsonArray serve(String filter, int limit) {
    final String lcaseFilter = filter.toLowerCase();
    if(lcaseFilter.startsWith("hdfs://") || lcaseFilter.startsWith("s3n://"))return serveHdfs(filter, limit);
    if(lcaseFilter.startsWith("s3://")) return serveS3(filter.substring(5), limit);
    return serveFile(filter,limit);
  }

}
