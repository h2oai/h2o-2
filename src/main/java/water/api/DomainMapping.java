package water.api;


import water.Request2;
import water.fvec.*;
import java.util.Arrays;

public class DomainMapping extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Get the domain mapping of String in a Vec";
  static final String NA = ""; // not available information

  @API(help="An existing H2O Frame key.", required=true, filter=Default.class, gridable=false)
  Frame src_key;

  @API(help="A string whose domain mapping should be returned.", required=true, filter=Default.class, gridable = false)
  String str;

  @API(help="The domain mapping") long map;


  // Just validate the frame, and fill in the summary bits
  @Override protected Response serve() {
    if( src_key == null ) return RequestServer._http404.serve();
    Vec v = src_key.anyVec();
    if (v.isEnum()) {
      map = Arrays.asList(v.domain()).indexOf(str);
    } else if (v.masterVec() != null && v.masterVec().isEnum()) {
      map = Arrays.asList(v.masterVec().domain()).indexOf(str);
    } else {
      map = -1;
    }
    return Response.done(this);
  }
}
