
package water.api;

import java.io.File;

import com.google.gson.*;

public class TypeaheadFileRequest extends TypeaheadRequest {

  public TypeaheadFileRequest() {
    super("Provides a simple JSON array of filtered local files.","");
  }

  @Override
  protected JsonArray serve(String filter, int limit) {
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
}
