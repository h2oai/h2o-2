package hex;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import water.Iced;
import water.api.Request.API;

import java.util.Arrays;

import static water.api.DocGen.FieldDoc;
import static water.api.DocGen.HTML;

public class ConfusionMatrix extends Iced {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
  @API(help="Confusion matrix (Actual/Predicted)")
  public long[][] _arr; // [actual][predicted]
  @API(help = "Prediction error by class")
  public final double[] _classErr;
  @API(help = "Prediction error")
  public final double _predErr;

  @Override public ConfusionMatrix clone() {
    ConfusionMatrix res = new ConfusionMatrix(0);
    res._arr = _arr.clone();
    for( int i = 0; i < _arr.length; ++i )
      res._arr[i] = _arr[i].clone();
    return res;
  }

  public enum ErrMetric {
    MAXC, SUMC, TOTAL;

    public double computeErr(ConfusionMatrix cm) {
      double[] cerr = cm.classErr();
      double res = 0;
      switch( this ) {
        case MAXC:
          res = cerr[0];
          for( double d : cerr )
            if( d > res )
              res = d;
          break;
        case SUMC:
          for( double d : cerr )
            res += d;
          break;
        case TOTAL:
          res = cm.err();
          break;
        default:
          throw new RuntimeException("unexpected err metric " + this);
      }
      return res;
    }

  }

  public ConfusionMatrix(int n) {
    _arr = new long[n][n];
    _classErr = classErr();
    _predErr = err();
  }

  public ConfusionMatrix(long[][] value) {
    _arr = value;
    _classErr = classErr();
    _predErr = err();
  }

  public synchronized void add(int i, int j) {
    _arr[i][j]++;
  }

  public double[] classErr() {
    double[] res = new double[_arr.length];
    for( int i = 0; i < res.length; ++i )
      res[i] = classErr(i);
    return res;
  }

  public final int size() {
    return _arr.length;
  }

  public final double classErr(int c) {
    long s = 0;
    for( long x : _arr[c] )
      s += x;
    if( s == 0 )
      return 0.0;    // Either 0 or NaN, but 0 is nicer
    return (double) (s - _arr[c][c]) / s;
  }
  public long totalRows() {
    long n = 0;
    for( int a = 0; a < _arr.length; ++a )
      for( int p = 0; p < _arr[a].length; ++p )
        n += _arr[a][p];
    return n;
  }

  public double err() {
    long n = totalRows();
    long err = n;
    for( int d = 0; d < _arr.length; ++d )
      err -= _arr[d][d];
    return (double) err / n;
  }

  public synchronized void add(ConfusionMatrix other) {
    water.util.Utils.add(_arr, other._arr);
  }

  public double precisionAndRecall() {
    return precisionAndRecall(_arr);
  }

  /**
   * Returns the F-measure which combines precision and recall. <br>
   * C.f. end of http://en.wikipedia.org/wiki/Precision_and_recall.
   */
  public static double precisionAndRecall(long[][] cm) {
    assert cm.length == 2 && cm[0].length == 2 && cm[1].length == 2;
    double tp = cm[0][0];
    double fp = cm[1][0];
    double fn = cm[0][1];
    double precision = tp / (tp + fp);
    double recall = tp / (tp + fn);
    double f = 2 * (precision * recall) / (precision + recall);
    if (Double.isNaN(f)) return 0;
    return f;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    for( long[] r : _arr )
      sb.append(Arrays.toString(r) + "\n");
    return sb.toString();
  }

  public JsonArray toJson() {
    JsonArray res = new JsonArray();
    JsonArray header = new JsonArray();
    header.add(new JsonPrimitive("Actual / Predicted"));
    for( int i = 0; i < _arr.length; ++i )
      header.add(new JsonPrimitive("class " + i));
    header.add(new JsonPrimitive("Error"));
    res.add(header);
    for( int i = 0; i < _arr.length; ++i ) {
      JsonArray row = new JsonArray();
      row.add(new JsonPrimitive("class " + i));
      long s = 0;
      for( int j = 0; j < _arr.length; ++j ) {
        s += _arr[i][j];
        row.add(new JsonPrimitive(_arr[i][j]));
      }
      double err = s - _arr[i][i];
      err /= s;
      row.add(new JsonPrimitive(err));
      res.add(row);
    }
    JsonArray totals = new JsonArray();
    totals.add(new JsonPrimitive("Totals"));
    long S = 0;
    long DS = 0;
    for( int i = 0; i < _arr.length; ++i ) {
      long s = 0;
      for( int j = 0; j < _arr.length; ++j )
        s += _arr[j][i];
      totals.add(new JsonPrimitive(s));
      S += s;
      DS += _arr[i][i];
    }
    double err = (S - DS) / (double) S;
    totals.add(new JsonPrimitive(err));
    res.add(totals);
    return res;
  }

  public void toHTMLbasic(StringBuilder sb, String[] labels) {
    String[] lab = labels;
    if (lab == null||lab.length!=2) {
      lab = new String[]{"false","true"};
    }
    sb.append("<table class='table table-bordered table-condensed'>");
    sb.append("<tr><th>Actual / Predicted</th><th>"+lab[0]+"</th><th>"+lab[1]+"</th></tr>");
    sb.append("<tr><th>"+lab[0]+"</th><td id='TN'>" + _arr[0][0] + "</td><td id='FN'>" + _arr[0][1] + "</td></tr>");
    sb.append("<tr><th>"+lab[1]+"</th><td id='FP'>" + _arr[1][0] + "</td><td id='TP'>" + _arr[1][1] + "</td></tr>");
    sb.append("</table>");
  }

  public void toHTML(StringBuilder sb, String[] labels) {
    String[] lab = labels;
    if (lab == null||lab.length!=2) {
      lab = new String[]{"false","true"};
    }
    long[][] cm = _arr;
    HTML.arrayHead(sb);
    // Sum up predicted & actuals
    long acts [] = new long[cm   .length];
    long preds[] = new long[cm[0].length];
    for( int a=0; a<cm.length; a++ ) {
      long sum=0;
      for( int p=0; p<cm[a].length; p++ ) { sum += cm[a][p]; preds[p] += cm[a][p]; }
      acts[a] = sum;
    }

    String adomain[] = lab;
    String pdomain[] = lab;

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
        String id = "";
        // this is required to change values via JS - but then the sums and error rates won't change -> leave for now.
//        if (cm.length == 2) {
//          if (a == 0 && p == 0) id = "id='TN'";
//          if (a == 0 && p == 1) id = "id='FP'";
//          if (a == 1 && p == 0) id = "id='FN'";
//          if (a == 1 && p == 1) id = "id='TP'";
//        }
        sb.append(onDiag ? "<td style='background-color:LightGreen' "+id+">":"<td "+id+">").append(cm[a][p]).append("</td>");
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
    sb.append(String.format("<th>%5.3f = %d / %d</th>", err(), terr, totalRows()));
    sb.append("</tr>");
    HTML.arrayTail(sb);
  }
}
