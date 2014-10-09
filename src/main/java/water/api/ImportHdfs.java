package water.api;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import water.DKV;
import water.persist.PersistHdfs;
import water.util.Log;

import dontweave.gson.*;
import dontweave.gson.internal.Streams;

public class ImportHdfs extends Request {
  public class PathArg extends TypeaheadInputText<String> {
    public PathArg(String name) {
      super(TypeaheadHdfsPathRequest.class, name, true);
    }
    @Override protected String parse(String input) throws IllegalArgumentException {
      return input;
    }
    @Override protected String queryDescription() { return "existing HDFS path"; }
    @Override protected String defaultValue() { return null; }
  }

  protected final PathArg _path = new PathArg(PATH);

  public ImportHdfs() {
    _requestHelp = "Imports the given HDFS path.  All nodes in the "
        + "cloud must have permission to access the HDFS path.";
    _path._requestHelp = "HDFS path to import.";
  }

  boolean isBareS3NBucketWithoutTrailingSlash(String s) {
    Pattern p = Pattern.compile("s3n://[^/]*");
    Matcher m = p.matcher(s);
    boolean b = m.matches();
    return b;
  }

  @Override
  protected Response serve() {
    String pstr = _path.value();
    if (isBareS3NBucketWithoutTrailingSlash(_path.value())) { pstr = pstr + "/"; }
    Log.info("ImportHDFS processing (" + pstr + ")");
    JsonArray succ = new JsonArray();
    JsonArray fail = new JsonArray();
    try {
      PersistHdfs.addFolder(new Path(pstr), succ, fail);
    } catch( IOException e ) {
      return Response.error(e);
    }
    DKV.write_barrier();
    JsonObject json = new JsonObject();
    json.add(NUM_SUCCEEDED, new JsonPrimitive(succ.size()));
    json.add(SUCCEEDED, succ);
    json.add(NUM_FAILED, new JsonPrimitive(fail.size()));
    json.add(FAILED, fail);
    Response r = Response.done(json);
    r.setBuilder(SUCCEEDED + "." + KEY, new KeyCellBuilder());
    // Add quick link
    if (succ.size() > 1)
      r.addHeader("<div class='alert'>" //
          + Parse.link("*"+pstr+"*", "Parse all into hex format") + " </div>");
    return r;
  }
}
