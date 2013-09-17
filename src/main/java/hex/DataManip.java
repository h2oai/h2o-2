package hex;

import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import com.google.gson.JsonObject;

import water.*;
import water.Job.FrameJob;
import water.api.*;
import water.fvec.Frame;
import water.fvec.Vec;

public class DataManip extends FrameJob {
  public DataManip() {
    super("Data Manip", null);
  }

  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Basic manipulation of 2 data frames.";
  public enum Operation { cbind, rbind };

  @API(help="Data Frame 2", required=true, filter=FrameKey.class)
  Frame source2;

  @API(help="Columns to Bind", required=true, filter=DataMultiVecSelect.class)
  int[] cols = new int[] {};
  class DataMultiVecSelect extends MultiVecSelect { DataMultiVecSelect() { super("source2");} }

  @API(help="Operation", filter=operationFilter.class)
  public Operation operation = Operation.cbind;
  class operationFilter extends EnumArgument<Operation> { public operationFilter() { super(Operation.cbind); } }

  @Override protected Response serve() {
    if(operation == Operation.cbind) {
      Vec[] v2 = source2.vecs();
      String[] n2 = source2.names();
      Set<String> n1 = new HashSet<String>(Arrays.asList(source.names()));

      String[] nadd = new String[cols.length];
      Vec[] vadd = new Vec[cols.length];
      for(int i = 0; i < cols.length; i++) {
        vadd[i] = v2[cols[i]];
        nadd[i] = n2[cols[i]];

        // Ensure column name is unique
        int count = 0;
        while(n1.contains(nadd[i])) {
          nadd[i] += "_DF2";
          if(count > 0) nadd[i] += "_" + count;
          count++;
        }
      }
      if(source.vecs()[0].length() != vadd[0].length())
        throw new IllegalArgumentException("Data frames must have same number of rows!");

      String[] names = (String[]) ArrayUtils.addAll(source.names(), nadd);
      Vec[] vecs = (Vec[]) ArrayUtils.addAll(source.vecs(), vadd);
      Frame fr = new Frame(names, vecs);
      DKV.put(dest(), fr);
    } else if(operation == Operation.rbind) {
      throw H2O.unimpl();
    }

    JsonObject res = new JsonObject();
    res.addProperty(RequestStatics.DEST_KEY, destination_key.toString());
    JsonObject redir = new JsonObject();
    redir.addProperty("src_key", destination_key.toString());
    return Response.redirect(res, Inspect2.class, redir);
  }

}