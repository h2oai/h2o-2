
package water.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import water.persist.PersistHdfs;

import dontweave.gson.JsonArray;
import dontweave.gson.JsonPrimitive;

public class TypeaheadHdfsPathRequest extends TypeaheadRequest {

  public TypeaheadHdfsPathRequest() {
    super("Provides a simple JSON array of HDFS Buckets.","");
  }

  @Override
  protected JsonArray serve(String filter, int limit) {
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

}
