package water.api;

import water.MRTask2;
import water.Model;
import water.Request2;
import water.UKV;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.TransfVec;
import water.fvec.Vec;
import water.util.Utils;

import java.util.Arrays;

/**
 *  Compare two categorical columns, reporting a grid of co-occurrences.
 *
 *  <p>The semantics follows R-approach - see R code:
 *  <pre>
 *  > l = c("A", "B", "C")
 *  > a = factor(c("A", "B", "C"), levels=l)
 *  > b = factor(c("A", "B", "A"), levels=l)
 *  > confusionMatrix(a,b)
 *
 *            Reference
 * Prediction A B C
 *          A 1 0 0
 *          B 0 1 0
 *          C 1 0 0
 *  </pre></p>
 *
 *  <p>Note: By default we report zero rows and columns.</p>
 *
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
  String [] actual_domain;
  @API(help="domain of the predicted response")
  String [] predicted_domain;
  @API(help="union of domains")
  String [] domain;
  @API(help="Confusion Matrix (or co-occurrence matrix)")
  public long cm[][];

  @API(help="Mean Squared Error")
  public double mse = Double.NaN;

  private boolean classification;

  @Override public Response serve() {
    Vec va = null,vp = null, avp = null;
    classification = vactual.isInt() && vpredict.isInt();
    // Input handling
    if( vactual==null || vpredict==null )
      throw new IllegalArgumentException("Missing actual or predict!");
    if (vactual.length() != vpredict.length())
      throw new IllegalArgumentException("Both arguments must have the same length!");

    try {
      if (classification) {
        // Create a new vectors - it is cheap since vector are only adaptation vectors
        va = vactual.toEnum(); // always returns TransfVec
        actual_domain = va._domain;
        vp = vpredict.toEnum(); // always returns TransfVec
        predicted_domain = vp._domain;
        if (!Arrays.equals(actual_domain, predicted_domain)) {
          domain = Utils.union(actual_domain, predicted_domain);
          int[][] vamap = Model.getDomainMapping(domain, actual_domain, true);
          va = TransfVec.compose( (TransfVec) va, vamap, domain, false ); // delete original va
          int[][] vpmap = Model.getDomainMapping(domain, predicted_domain, true);
          vp = TransfVec.compose( (TransfVec) vp, vpmap, domain, false ); // delete original vp
        } else domain = actual_domain;
        // The vectors are from different groups => align them, but properly delete it after computation
        if (!va.group().equals(vp.group())) {
          avp = vp;
          vp = va.align(vp);
        }
        cm = new CM(domain.length).doAll(va,vp)._cm;
      } else {
        if (vactual.isEnum())
          throw new IllegalArgumentException("Actual vector cannot be categorical for regression scoring.");
        if (vpredict.isEnum())
          throw new IllegalArgumentException("Predicted vector cannot be categorical for regression scoring.");
        mse = new CM(1).doAll(vactual,vpredict).mse();
      }
      return Response.done(this);
    } catch( Throwable t ) {
      return Response.error(t);
    } finally {       // Delete adaptation vectors
      if (va!=null) UKV.remove(va._key);
      if (vp!=null) UKV.remove(vp._key);
      if (avp!=null) UKV.remove(avp._key);
    }
  }

  // Compute the co-occurrence matrix
  private static class CM extends MRTask2<CM> {
    /* @IN */ final int _c_len;
    /* @OUT Classification */ long _cm[][];
    /* @OUT Regression */ public double mse() { return _count > 0 ? _mse/_count : Double.POSITIVE_INFINITY; }
    /* @OUT Regression Helper */ private double _mse;
    /* @OUT Regression Helper */ private long _count;
    CM(int c_len) { _c_len = c_len;  }
    @Override public void map( Chunk ca, Chunk cp ) {
      //classification
      if (_c_len > 1) {
        _cm = new long[_c_len+1][_c_len+1];
        int len = Math.min(ca._len,cp._len); // handle different lenghts, but the vectors should have been rejected already
        for( int i=0; i < len; i++ ) {
          int a=ca.isNA0(i) ? _c_len : (int)ca.at80(i);
          int p=cp.isNA0(i) ? _c_len : (int)cp.at80(i);
          _cm[a][p]++;
        }
        if( len < ca._len )
          for( int i=len; i < ca._len; i++ )
            _cm[ca.isNA0(i) ? _c_len : (int)ca.at80(i)][_c_len]++;
        if( len < cp._len )
          for( int i=len; i < cp._len; i++ )
            _cm[_c_len][cp.isNA0(i) ? _c_len : (int)cp.at80(i)]++;
      } else {
        _cm = null;
        _mse = 0;
        assert(ca._len == cp._len);
        int len = ca._len;
        for( int i=0; i < len; i++ ) {
          if (ca.isNA0(i) || cp.isNA0(i)) continue; //TODO: Improve
          final double a=ca.at0(i);
          final double p=cp.at0(i);
          _mse += (p-a)*(p-a);
          _count++;
        }
      }
    }

    @Override public void reduce( CM cm ) {
      if (_cm != null && cm._cm != null) {
        Utils.add(_cm,cm._cm);
      } else {
        assert(_mse != Double.NaN && cm._mse != Double.NaN);
        assert(_cm == null && cm._cm == null);
        _mse += cm._mse;
        _count += cm._count;
      }
    }
  }

  public static String[] show( long xs[], String ds[] ) {
    String ss[] = new String[xs.length]; // the same length
    for( int i=0; i<ds.length; i++ )
      if( xs[i] >= 0 || (ds[i] != null && ds[i].length() > 0) && !Integer.toString(i).equals(ds[i]) )
        ss[i] = ds[i];
    if( xs[xs.length-1] > 0 )
      ss[xs.length-1] = "NA";
    return ss;
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    if (classification) {
      DocGen.HTML.section(sb,"Confusion Matrix");
      if( cm == null ) return true;
    }
    else {
      DocGen.HTML.section(sb,"Mean Squared Error");
      if( mse == Double.NaN ) return true;
    }

    DocGen.HTML.arrayHead(sb);
    if (classification) {
      // Sum up predicted & actuals
      long acts [] = new long[cm   .length];
      long preds[] = new long[cm[0].length];
      for( int a=0; a<cm.length; a++ ) {
        long sum=0;
        for( int p=0; p<cm[a].length; p++ ) { sum += cm[a][p]; preds[p] += cm[a][p]; }
        acts[a] = sum;
      }

      String adomain[] = show(acts , domain);
      String pdomain[] = show(preds, domain);
      assert adomain.length == pdomain.length : "The confusion matrix should have the same length for both directions.";

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
    } else{
      // Regression
      sb.append("<tr class='warning'><td>" + mse + "</td></tr>"); // Row header
    }
    DocGen.HTML.arrayTail(sb);
    return true;
  }

  public double toASCII( StringBuilder sb ) {
    if( cm == null && classification) return 1.0;
    if( !classification) {
      sb.append("MSE: " + mse);
      return mse;
    }

    // Sum up predicted & actuals
    long acts [] = new long[cm   .length];
    long preds[] = new long[cm[0].length];
    for( int a=0; a<cm.length; a++ ) {
      long sum=0;
      for( int p=0; p<cm[a].length; p++ ) { sum += cm[a][p]; preds[p] += cm[a][p]; }
      acts[a] = sum;
    }

    String adomain[] = show(acts , domain);
    String pdomain[] = show(preds, domain);

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
        boolean onDiag = adomain[a].equals(pdomain[p]);
        if( onDiag ) correct = cm[a][p];
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
