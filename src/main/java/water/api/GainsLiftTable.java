package water.api;

import water.Func;
import water.MRTask2;
import water.UKV;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;
import water.util.Utils;

/* Compute the Gains and Lift Table for binary classifier */
public class GainsLiftTable extends Func {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
  public static final String DOC_GET = "Gains/Lift Table";

  @API(help = "", required = true, filter = Default.class, json=true)
  public Frame actual;

  @API(help="Column of the actual results", required=true, filter=actualVecSelect.class, json=true)
  public Vec vactual;
  class actualVecSelect extends VecClassSelect { actualVecSelect() { super("actual"); } }

  @API(help = "", required = true, filter = Default.class, json=true)
  public Frame predict;

  @API(help="Column of the predicted results", required=true, filter=predictVecSelect.class, json=true)
  public Vec vpredict;
  class predictVecSelect extends VecClassSelect { predictVecSelect() { super("predict"); } }

  @API(help = "The number of rows in the gains table", required = false, filter = Default.class, json = true)
  public int groups = 10;

  // helper - contains the probability thresholds for each of the groups
  double[] thresholds;

  // Results (Output)
  @API(help="Response rates", json=true)
  public float[] response_rates;
  @API(help="Average response rate", json=true)
  public float avg_response_rate;
  @API(help="Positive Responses Per Group", json=true)
  public long[] positive_responses;

  @Override protected void init() throws IllegalArgumentException {
    // Input handling
    if( vactual==null || vpredict==null )
      throw new IllegalArgumentException("Missing vactual or vpredict!");
    if (vactual.length() != vpredict.length())
      throw new IllegalArgumentException("Both arguments must have the same length ("+vactual.length()+"!="+vpredict.length()+")!");
    if (!vactual.isInt())
      throw new IllegalArgumentException("Actual column must be integer class labels!");
    if (vactual.cardinality() != -1 && vactual.cardinality() != 2)
      throw new IllegalArgumentException("Actual column must contain binary class labels, but found cardinality " + vactual.cardinality() + "!");
    if (vpredict.isEnum())
      throw new IllegalArgumentException("vpredict cannot be class labels, expect probabilities.");
  }

  public GainsLiftTable() {}

  public GainsLiftTable(float[] response_rates, float avg_response_rate) {
    this.response_rates = response_rates;
    this.avg_response_rate = avg_response_rate;
  }

  @Override protected void execImpl() {
    Vec va = null, vp;
    try {
      va = vactual.toEnum(); // always returns TransfVec
      vp = vpredict;
      // The vectors are from different groups => align them, but properly delete it after computation
      if (!va.group().equals(vp.group())) {
        vp = va.align(vp);
      }

      // compute thresholds for each quantile
      {
        thresholds = new double[groups];
        for (int i=0; i<groups; ++i) {
          QuantilesPage q = new QuantilesPage();
          q.source_key = predict;
          q.column = vpredict;
          q.quantile = (groups-i-1.) / groups;
          q.invoke();
          thresholds[i] = q.result;
        }

        if (Utils.minValue(thresholds) < 0) throw new IllegalArgumentException("Minimum propability cannot be negative.");
        if (Utils.maxValue(thresholds) > 1) throw new IllegalArgumentException("Maximum probability cannot be greater than 1.");

        // Now compute the GainsTask
        GainsTask gt = new GainsTask(thresholds, va.length());
        gt.doAll(va, vp);
        response_rates = gt.response_rates();
        avg_response_rate = gt.avg_response_rate();
        positive_responses = gt.responses();
      }
    } catch (Throwable t) {
      // do nothing
    } finally {       // Delete adaptation vectors
      if (va!=null) UKV.remove(va._key);
    }
    StringBuilder sb = new StringBuilder();
    toASCII(sb);
    Log.info(sb);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    if (response_rates == null) return false;

    DocGen.HTML.arrayHead(sb);
    sb.append("<a href=\"http://books.google.com/books?id=-JwptfFItaoC&pg=PA318&lpg=PA319&source=bl&ots=_S6fJI5Wds&sig=Uvff-MosTE7CR4e8LdE8TdJvo44&hl=en&sa=X&ei=b3EcVMnHB6T2iwK3koC4Cw&ved=0CF0Q6AEwBw#v=onepage&q&f=false\">"
    + "Gains/Lift Table Reference</a></h4>");
    // Sum up predicted & actuals
    sb.append("<tr class='warning' style='min-width:60px'>");
    sb.append("<th>Quantile</th><th>Response rate</th><th>Lift</th><th>Cumulative lift</th>");
    sb.append("</tr>");

    float cumulativelift = 0;
    for( int i=0; i<groups; i++ ) {
      sb.append("<tr>");
      sb.append("<td>").append(Utils.formatPct((i + 1.) / groups)).append("</td>");
      sb.append("<td>").append(Utils.formatPct(response_rates[i])).append("</td>");
      final float lift = response_rates[i]/ avg_response_rate;
      cumulativelift += lift/groups;
      sb.append("<td>").append(lift).append("</td>");
      sb.append("<td>").append(Utils.formatPct(cumulativelift)).append("</td>");
    }

    sb.append("<tr style='min-width:60px'><th>Total</th>");
    sb.append("<td>").append(Utils.formatPct(avg_response_rate)).append("</td>");
    sb.append("<td>").append(1.0).append("</td>");
    sb.append("<td></td>");
    DocGen.HTML.arrayTail(sb);
    return true;
  }

  public void toASCII( StringBuilder sb ) {
    if (response_rates == null) return;
    // Sum up predicted & actuals
    sb.append("Quantile  Response rate    Lift    Cumulative lift\n");

    float cumulativelift = 0;
    for( int i=0; i<groups; i++ ) {
      sb.append(Utils.formatPct((i + 1.) / groups));
      sb.append("   ").append(Utils.formatPct(response_rates[i])).append("   ");
      final float lift = response_rates[i]/ avg_response_rate;
      cumulativelift += lift/groups;
      sb.append("   ").append(lift).append("     ");
      sb.append("   ").append(Utils.formatPct(cumulativelift)).append("\n");
    }

    sb.append("Total ");
    sb.append("    ").append(Utils.formatPct(avg_response_rate)).append("  ");
    sb.append("    ").append(1.0).append("    ");
    sb.append("         \n");
  }

  // Compute Gains table via MRTask2
  private static class GainsTask extends MRTask2<GainsTask> {
    /* @OUT response_rates */
    public final float[] response_rates() { return _response_rates; }
    public final float avg_response_rate() { return _avg_response_rate; }
    public final long[] responses(){ return _responses; }

    /* @IN total count of events */ final private double[] _thresh;
    final private long _count;

    private long[] _responses;
    private long _avg_response;
    private float _avg_response_rate;
    private float[] _response_rates;

    GainsTask(double[] thresh, long count) {
      _thresh = thresh.clone();
      _count = count;
    }

    @Override public void map( Chunk ca, Chunk cp ) {
      _responses = new long[_thresh.length];
      _avg_response = 0;
      final int len = Math.min(ca._len, cp._len);
      for( int i=0; i < len; i++ ) {
        if (ca.isNA0(i)) continue;
        final int a = (int)ca.at80(i);
        if (a != 0 && a != 1) throw new IllegalArgumentException("Invalid values in vactual: must be binary (0 or 1).");
        if (cp.isNA0(i)) continue;
        final double pr = cp.at0(i);
        for( int t=0; t < _thresh.length; t++ ) {
          // count number of positive responses in bucket given by two thresholds
          if (pr >= _thresh[t] && (t == 0 || pr < _thresh[t-1]) && a == 1) _responses[t]++;
        }
        if (a == 1) _avg_response++;
      }
    }

    @Override public void reduce( GainsTask other ) {
      for( int i=0; i<_responses.length; ++i) {
        _responses[i] += other._responses[i];
      }
      _avg_response += other._avg_response;
    }

    @Override public void postGlobal(){
      _response_rates = new float[_thresh.length];
      for (int i=0; i<_response_rates.length; ++i) {
        _response_rates[i] = (float) _responses[i];
      }
      Utils.div(_response_rates, (float)_count/_thresh.length);
      for (int i=0; i<_response_rates.length; ++i) {
        // spill over to next bucket - needed due to tie breaking in quantiles
        if(_response_rates[i] > 1) {
          _response_rates[i+1] += (_response_rates[i]-1);
          _response_rates[i] -= (_response_rates[i]-1);
        }
      }
      _avg_response_rate = (float)_avg_response / _count;
    }
  }
}
