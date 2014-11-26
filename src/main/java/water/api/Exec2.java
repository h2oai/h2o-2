package water.api;

import water.util.Log;
import water.*;
import water.exec.*;
import water.fvec.*;

import java.util.Arrays;
import java.util.Properties;

public class Exec2 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
  // This Request supports the HTML 'GET' command, and this is the help text for GET.
  static final String DOC_GET = "Executes a string in H2O's R-like language.";
  @API(help="String to execute", required=true, filter=Default.class)
  String str;

  @API(help="Warning message,ifany") String[] warning;
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
    Throwable e;
    Env env = null;
    try {
      env = water.exec.Exec2.exec(str);
      StringBuilder sb = env._sb;
      if( sb.length()!=0 ) sb.append("\n");
      if( env == null ) throw new IllegalArgumentException("Null return from Exec2?");
      if( env.sp() == 0 ) {      // Empty stack
      } else if( env.isAry() ) {
        Frame fr = env.peekAry();
        String skey = env.peekKey();
        num_rows = fr.numRows();
        num_cols = fr.numCols();
        cols = new Inspect2.ColSummary[num_cols];
        for( int i=0; i<num_cols; i++ )
          cols[i] = new Inspect2.ColSummary(fr._names[i],fr.vecs()[i]);
        // Now the first few rows.
        String[] fs = fr.toStringHdr(sb);
        if(fr.numCols() < 1000)
          for( int i=0; i<Math.min(6,fr.numRows()); i++ )
            fr.toString(sb,fs,i);
        // Nuke the result
        env.pop();
      } else if( env.isFcn() ) {
        ASTOp op = env.peekFcn();
        funstr = op.toString();
        sb.append(op.toString(true)); // Verbose function
        env.pop();
      } else {
        scalar = env.popDbl();
        sb.append(Double.toString(scalar));
      }
      if (env.warnings().length != 0) { sb.append(Arrays.toString(env.warnings())); }
      result=sb.toString();
//      num_cols = num_rows == 0 ? 0 : num_cols;
      return Response.done(this);
    }
    catch( IllegalArgumentException pe ) { e=pe;} // No logging user typo's
    catch( Throwable e2 ) { Log.err(e=e2); }
    finally {
      if (env != null) {
        try { env.remove_and_unlock(); }
        catch (Exception xe) { Log.err("env.remove_and_unlock() failed", xe); }
      }
    }
    return Response.error(e);
  }
  @Override protected NanoHTTPD.Response serveGrid(NanoHTTPD server, Properties parms, RequestType type) {
    return superServeGrid(server, parms, type);
  }
}
