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

  @API(help = "", required = true, filter = Default.class)
  public Frame actual;

  @API(help="Column of the actual results (will display vertically)", required=true, filter=actualVecSelect.class)
  public Vec vactual;
  class actualVecSelect extends VecClassSelect { actualVecSelect() { super("actual"); } }

  @API(help = "", required = true, filter = Default.class)
  public Frame predict;

  @API(help="Column of the predicted results (will display horizontally)", required=true, filter=predictVecSelect.class)
  public Vec vpredict;
  class predictVecSelect extends VecClassSelect { predictVecSelect() { super("predict"); } }

  @API(help="Confusion Matrix (or co-occurrence matrix)")
  public long cm[][];

  //public static String link(Key k, String content) {
  //  RString rs = new RString("<a href='ConfusionMatrix.query?model=%$key'>%content</a>");
  //  rs.replace("key", k.toString());
  //  rs.replace("content", content);
  //  return rs.toString();
  //}

  @Override public Response serve() {
    Vec va = null, vp = null;
    try {
      if( vactual==null || vpredict==null )
        throw new IllegalArgumentException("Missing actual or predict?");
      // Create a new vectors - it is cheap since vector are only adaptation vectors
      va = vactual .toEnum();
      vp = vpredict.toEnum();
      cm = new CM(va.domain().length, vp.domain().length).doAll(vactual,vpredict)._cm;
      return new Response(Response.Status.done,this,-1,-1,null);
    } catch (Throwable t) {
      Log.err(t);
      return Response.error(t.getMessage());
    } finally {       // Delete adaptation vectors
      if (va!=null) UKV.remove(va._key);
      if (vp!=null) UKV.remove(vp._key);
    }
  }

  // Compute the co-occurence matrix
  private static class CM extends MRTask2<CM> {
    /* @IN */ final int _ca_len;
    /* @IN */ final int _cp_len;
    /* @OUT */ long _cm[][];
    CM(int ca_len, int cp_len) { _ca_len = ca_len; _cp_len = cp_len; }
    @Override public void map( Chunk ca, Chunk cp ) {
      _cm = new long[_ca_len+1][_cp_len+1];
      int len=Math.min(ca._len,cp._len);
      for( int i=0; i < len; i++ ) {
        int a=ca.isNA0(i) ? _ca_len : (int)ca.at80(i);
        int p=cp.isNA0(i) ? _cp_len : (int)cp.at80(i);
        _cm[a][p]++;
      }
      if( len < ca._len )
        for( int i=len; i < ca._len; i++ )
          _cm[ca.isNA0(i) ? _ca_len : (int)ca.at80(i)][_cp_len]++;
      if( len < cp._len )
        for( int i=len; i < cp._len; i++ )
          _cm[_ca_len][cp.isNA0(i) ? _cp_len : (int)cp.at80(i)]++;
    }
    @Override public void reduce( CM cm ) { Utils.add(_cm,cm._cm); }
  }

  public static String[] show( long xs[], String ds[] ) {
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
      long correct=0;
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
