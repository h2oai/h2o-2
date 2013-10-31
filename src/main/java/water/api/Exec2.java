package water.api;

import water.util.Log;
import water.*;
import water.exec.*;
import water.fvec.*;

import java.util.Properties;

public class Exec2 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
  // This Request supports the HTML 'GET' command, and this is the help text for GET.
  static final String DOC_GET = "Executes a string in H2O's R-like language.";
  @API(help="String to execute", required=true, filter=Default.class)
  String str;

  @API(help="Parsing error, if any") String error;
  @API(help="Result key"           ) Key key;
  @API(help="Rows in Frame result" ) long num_rows;
  @API(help="Columns in Frame result" ) int  num_cols;
  @API(help="Scalar result"        ) double scalar;
  @API(help="Function result"      ) String funstr;
  // Pretty-print of result.  For Frames, first 10 rows.  For scalars, just the
  // value.  For functions, the pretty-printed AST.
  @API(help="String result"        ) String result;
 
  @API(help="Array of Column Summaries.") Inspect2.ColSummary cols[];

  @Override protected Response serve() {
    if( str == null ) return RequestServer._http404.serve();
    Exception e;
    try {
      Env env = water.exec.Exec2.exec(str);
      if( env == null ) throw new IllegalArgumentException("Null return from Exec2?");
      if( env.sp() == 0 ) {      // Empty stack
      } else if( env.isFrame() ) { 
        Frame fr = env.popFrame();
        num_rows = fr.numRows();
        num_cols = fr.numCols();
        cols = new Inspect2.ColSummary[num_cols];
        for( int i=0; i<num_cols; i++ )
          cols[i] = new Inspect2.ColSummary(fr._names[i],fr.vecs()[i]);
        // Now the first few rows.
        StringBuilder sb = new StringBuilder();
        String[] fs = fr.toStringHdr(sb);
        for( int i=0; i<Math.min(6,fr.numRows()); i++ )
          fr.toString(sb,fs,i);
        result=sb.toString();
        // Nuke the result
        env.subRef(fr);
      } else if( env.isFun() ) {
        ASTOp op = env.popFun();
        funstr = op.toString();
        result = op.toString(true); // Verbose function
        env.subRef(op);
      } else {
        scalar = env.popDbl();
        result = Double.toString(scalar);
      }
      env.remove();
      return new Response(Response.Status.done, this, -1, -1, null);
    } 
    catch( IllegalArgumentException pe ) { e=pe;} // No logging user typo's
    catch( Exception e2 ) { Log.err(e=e2); }
    return Response.error(e.getMessage());
  }
  @Override protected NanoHTTPD.Response serveGrid(NanoHTTPD server, Properties parms, RequestType type) {
    return superServeGrid(server, parms, type);
  }
}
