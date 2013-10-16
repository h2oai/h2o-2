package water.api;

import water.util.Log;
import water.*;
import water.exec.*;
import water.fvec.*;

public class Exec2 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
  // This Request supports the HTML 'GET' command, and this is the help text for GET.
  static final String DOC_GET = "Executes a string.";
  @API(help="String to execute", required=true, filter=Default.class)
  String str;

  @API(help="Parsing error, if any") String error;
  @API(help="Result key"           ) Key key;
  @API(help="Rows in result"       ) long num_rows;
  @API(help="Columns in result"    ) int  num_cols;
  @API(help="Scalar result"        ) double scalar;
  @API(help="Function result"      ) String funstr;
 
  @API(help="Array of Column Summaries.") Inspect2.ColSummary cols[];

  @Override protected Response serve() {
    if( str == null ) return RequestServer._http404.serve();
    Exception e;
    try {
      Env env = water.exec.Exec2.exec(str);
      if( env == null ) throw new IllegalArgumentException("Null return from Exec2?");
      key = Key.make(".Last.value");
      UKV.remove(key);
      if( env.sp() == 0 ) {      // Empty stack
      } else if( env.isFrame() ) { 
        Frame fr = env.popFrame();
        UKV.put(key,fr);
        num_rows = fr.numRows();
        num_cols = fr.numCols();
        cols = new Inspect2.ColSummary[num_cols];
        for( int i=0; i<num_cols; i++ )
          cols[i] = new Inspect2.ColSummary(fr._names[i],fr.vecs()[i]);
      } else if( env.isFun() ) funstr = env.popFun().toString();
        else scalar = env.popDbl();
      env.remove();
      return new Response(Response.Status.done, this, -1, -1, null);
    } 
    catch( IllegalArgumentException pe ) { e=pe;} // No logging user typo's
    catch( Exception e2 ) { Log.err(e=e2); }
    return Response.error(e.getMessage());
  }
}
