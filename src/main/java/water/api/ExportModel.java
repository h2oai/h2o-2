package water.api;

import water.Model;
import water.Request2;

public class ExportModel extends Request2 {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "Exports a model as JSON";

  @API(help = "The model to export", json = true, required = true, filter = Default.class)
  public Model model;

  @Override protected Response serve() {
    return Response.done(this);
  }
}
