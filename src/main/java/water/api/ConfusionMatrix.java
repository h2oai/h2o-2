package water.api;

import water.*;
import water.fvec.*;
import water.util.Log;
import water.util.Utils;

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

  @API(help="domain of the actual response")
  String [] response_domain;
  @API(help="domain of the predicted response")
  String [] predicted_domain;
  @API(help="Confusion Matrix (or co-occurrence matrix)")
  public long cm[][];

  //public static String link(Key k, String content) {
  //  RString rs = new RString("<a href='ConfusionMatrix.query?model=%$key'>%content</a>");
  //  rs.replace("key", k.toString());
  //  rs.replace("content", content);
  //  return rs.toString();
  //}

  @Override public Response serve() {
    Vec va = null,vp = null, avp = null;
    // Input handling
    if( vactual==null || vpredict==null )
      return Response.error("Missing actual or predict?");
    if ( !vactual.isInt() || !vpredict.isInt())
      return Response.error("Cannot provide confusion matrix for float vectors.");

    try {
      // Create a new vectors - it is cheap since vector are only adaptation vectors
      va = vactual .toEnum();
      response_domain = va._domain;
      vp = vpredict.toEnum();
      predicted_domain = vp._domain;
      // The vectors are from different groups => align them, but properly delete it after computation
      if (!va.group().equals(vp.group())) {
        avp = vp;
        vp = va.align(vp);
      }
      cm = new CM(va.domain().length, vp.domain().length).doAll(va,vp)._cm;
      return Response.done(this);
    } catch (Throwable t) {
      return Response.error(t);
    } finally {       // Delete adaptation vectors
      if (va!=null) UKV.remove(va._key);
      if (vp!=null) UKV.remove(vp._key);
      if (avp!=null) UKV.remove(avp._key);
    }
  }

  // Compute the co-occurrence matrix
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

    String adomain[] = show(acts ,response_domain);
    String pdomain[] = show(preds,predicted_domain);

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

  public double toASCII( StringBuilder sb ) {
    assert(cm != null);
    long acts [] = new long[cm   .length];
    long preds[] = new long[cm[0].length];
    for( int a=0; a<cm.length; a++ ) {
      long sum=0;
      for( int p=0; p<cm[a].length; p++ ) { sum += cm[a][p]; preds[p] += cm[a][p]; }
      acts[a] = sum;
    }
    Vec vaE = null, vpE = null, avp = null;
    String adomain[] = null;
    String pdomain[] = null;
    try {
      vaE = vactual.toEnum();
      vpE = vpredict.toEnum();
      // The vectors are from different groups => align them, but properly delete it after computation
      if (!vaE.group().equals(vpE.group())) {
        avp = vpE;
        vpE = vaE.align(vpE);
      }
      adomain = ConfusionMatrix.show(acts ,vaE.domain());
      pdomain = ConfusionMatrix.show(preds,vpE.domain());
    } catch (Throwable t) {
      Log.err(t);
      return Double.NaN;
    } finally {
      if (vaE!=null)  UKV.remove(vaE._key);
      if (vpE!=null)  UKV.remove(vpE._key);
      if (avp!=null)  UKV.remove(avp._key);
    }

    // determine max length of each space-padded field
    int maxlen = 0;
    for( String s : pdomain ) if( s != null ) maxlen = Math.max(maxlen, s.length());
    long sum = 0;
    for( int a=0; a<cm.length; a++ ) {
      if( adomain[a] == null ) continue;
      for( int p=0; p<pdomain.length; p++ ) { if( pdomain[p] == null ) continue; sum += cm[a][p]; }
    }
    maxlen = Math.max(8, Math.max(maxlen, String.valueOf(sum).length()) + 2);
    final String fmt  = "%" + maxlen + "d";
    final String fmtS = "%" + maxlen + "s";

    sb.append(String.format(fmtS, "Act/Prd"));
    for( String s : pdomain ) if( s != null ) sb.append(String.format(fmtS, s));
    sb.append("   " + String.format(fmtS, "Error\n"));

    long terr=0;
    for( int a=0; a<cm.length; a++ ) {
      if( adomain[a] == null ) continue;
      sb.append(String.format(fmtS,adomain[a]));
      long correct=0;
      for( int p=0; p<pdomain.length; p++ ) {
        if( pdomain[p] == null ) continue;
        if( adomain[a].equals(pdomain[p]) ) correct = cm[a][p];
        sb.append(String.format(fmt,cm[a][p]));
      }
      long err = acts[a]-correct;
      terr += err;            // Bump totals
      sb.append("   " + String.format("%5.3f = %d / %d\n", (double)err/acts[a], err, acts[a]));
    }
    sb.append(String.format(fmtS, "Totals"));
    for( int p=0; p<pdomain.length; p++ )
      if( pdomain[p] != null )
        sb.append(String.format(fmt, preds[p]));
    double total_err_rate = (double)terr/vactual.length();
    sb.append("   " + String.format("%5.3f = %d / %d\n", total_err_rate, terr, vactual.length()));

    return total_err_rate;
  }
}
