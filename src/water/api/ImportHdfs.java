package water.api;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import water.DKV;
import water.Key;
import water.hdfs.HdfsLoader;
import water.hdfs.PersistHdfs;
import water.store.s3.PersistS3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.gson.*;

public class ImportHdfs extends Request {
  public class PathArg extends TypeaheadInputText<String> {
    public PathArg(String name) {
      super(TypeaheadHdfsPathRequest.class, name, true);
    }

    @Override
    protected String parse(String input) throws IllegalArgumentException {
      return input;
    }

    @Override
    protected String queryDescription() {
      return "existing HDFS path";
    }

    @Override
    protected String defaultValue() {
      return null;
    }
  }

  protected final PathArg _path = new PathArg("path");

  public ImportHdfs() {
    _requestHelp = "Imports the given HDFS path.  All nodes in the "
        + "cloud must have permission to access the HDFS path.";
    _path._requestHelp = "HDFS path to import.";
  }

  @Override
  protected Response serve() {
    JsonArray succ = new JsonArray();
    JsonArray fail = new JsonArray();
    try {
      PersistHdfs.addFolder(new Path(_path.value()), succ, fail);
    } catch( IOException e ) {
      return Response.error(e.getMessage());
    }
    DKV.write_barrier();

    JsonObject json = new JsonObject();
    json.add(SUCCEEDED, succ);
    json.add(FAILED, fail);
    Response r = Response.done(json);
    r.setBuilder(SUCCEEDED + "." + KEY, new KeyCellBuilder());
    return r;
  }
}
