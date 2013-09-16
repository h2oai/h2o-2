package water.api;

import water.*;
import water.fvec.*;
import water.util.*;

/**
 *  Compare two categorical columns, reporting a grid of co-occurrences.
 *  @author cliffc
 */
public class ConfusionMatrix extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "", required = true, filter = actualFilter.class)
  public Frame actual;
  class actualFilter extends FrameKey { public actualFilter() { super("actual"); } }

  @API(help="Column of the actual results (will display vertically)", required=true, filter=actualVecSelect.class)
  Vec vactual;
  class actualVecSelect extends VecClassSelect { actualVecSelect() { super("actual"); } }

  @API(help = "", required = true, filter = predictFilter.class)
  public Frame predict;
  class predictFilter extends FrameKey { public predictFilter() { super("predict"); } }

  @API(help="Column of the predicted results (will display horizontally)", required=true, filter=predictVecSelect.class)
  Vec vpredict;
  class predictVecSelect extends VecClassSelect { predictVecSelect() { super("predict"); } }

  @API(help="Confusion Matrix (or co-occurrence matrix)")
  long cm[][];

  //public static String link(Key k, String content) {
  //  RString rs = new RString("<a href='ConfusionMatrix.query?model=%$key'>%content</a>");
  //  rs.replace("key", k.toString());
  //  rs.replace("content", content);
  //  return rs.toString();
  //}

  @Override protected Response serve() {
    try {
      if( vactual==null || vpredict==null )
        throw new IllegalArgumentException("Missing actual or predict?");
      if( !vactual .isEnum() ) vactual .asEnum();
      if( !vpredict.isEnum() ) vpredict.asEnum();
      cm = new CM().doAll(vactual,vpredict)._cm;
      return new Response(Response.Status.done,this,-1,-1,null);
    } catch (Throwable t) {
      Log.err(t);
      return Response.error(t.getMessage());
    }
  }

  // Compute the co-occurence matrix
  private static class CM extends MRTask2<CM> {
    long _cm[][];
    @Override public void map( Chunk ca, Chunk cp ) {
      int ca_len=ca._vec.domain().length;
      int cp_len=cp._vec.domain().length;
      _cm = new long[ca_len+1][cp_len+1];
      int len=Math.min(ca._len,cp._len);
      for( int i=0; i < len; i++ ) {
        int a=ca.isNA0(i) ? ca_len : (int)ca.at80(i);
        int p=cp.isNA0(i) ? cp_len : (int)cp.at80(i);
        _cm[a][p]++;
      }
      if( len < ca._len )
        for( int i=len; i < ca._len; i++ )
          _cm[ca.isNA0(i) ? ca_len : (int)ca.at80(i)][cp_len]++;
      if( len < cp._len )
        for( int i=len; i < cp._len; i++ )
          _cm[ca_len][cp.isNA0(i) ? cp_len : (int)cp.at80(i)]++;
    }
    @Override public void reduce( CM cm ) { Utils.add(_cm,cm._cm); }
  }

  private static String[] show( long xs[], String ds[] ) {
    String ss[] = new String[xs.length];
    for( int i=0; i<ds.length; i++ )
      if( xs[i] > 0 || (ds[i] != null && ds[i].length() > 0) && !Integer.toString(i).equals(ds[i]) )
        ss[i] = ds[i];
    if( xs[xs.length-1] > 0 ) 
      ss[xs.length-1] = "NA";
    return ss;
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    DocGen.HTML.title(sb,"Confusion Matrix");
    if( cm == null ) return true;
    // Sum up predicted & actuals
    long acts [] = new long[cm   .length];
    long preds[] = new long[cm[0].length];
    for( int a=0; a<cm.length; a++ ) {
      long sum=0;
      for( int p=0; p<cm[a].length; p++ ) { sum += cm[a][p]; preds[p] += cm[a][p]; }
      acts[a] = sum;
    }

    String adomain[] = show(acts ,vactual .domain());
    String pdomain[] = show(preds,vpredict.domain());

    DocGen.HTML.arrayHead(sb);
    // Top row of CM
    sb.append("<tr class='warning'>");
    sb.append("<th>Actual / Predicted</th>"); // Row header
    for( int p=0; p<pdomain.length; p++ )
      if( pdomain[p] != null )
        sb.append("<th>").append(pdomain[p]).append("</th>");
    sb.append("<th>Error</th>");
    sb.append("</tr>");
    
    // Main CM Body
    long terr=0;
    for( int a=0; a<cm.length; a++ ) { // Actual loop
      if( adomain[a] == null ) continue;
      sb.append("<tr>");
      sb.append("<th>").append(adomain[a]).append("</th>");// Row header
      long correct=-1;
      for( int p=0; p<pdomain.length; p++ ) { // Predicted loop
        if( pdomain[p] == null ) continue;
        boolean onDiag = adomain[a].equals(pdomain[p]);
        if( onDiag ) correct = cm[a][p];
        sb.append(onDiag ? "<td style='background-color:LightGreen'>":"<td>").append(cm[a][p]).append("</td>");
      }
      long err = acts[a]-correct;
      terr += err;              // Bump totals
      sb.append(String.format("<th>%5.3f = %d / %d</th>", (double)err/acts[a], err, acts[a]));
      sb.append("</tr>");
    }
    
    // Last row of CM
    sb.append("<tr>");
    sb.append("<th>Totals</th>");// Row header
    for( int p=0; p<pdomain.length; p++ ) { // Predicted loop
      if( pdomain[p] == null ) continue;
      sb.append("<td>").append(preds[p]).append("</td>");
    }
    sb.append(String.format("<th>%5.3f = %d / %d</th>", (double)terr/vactual.length(), terr, vactual.length()));
    sb.append("</tr>");

    DocGen.HTML.arrayTail(sb);
    return true;
  }
}
