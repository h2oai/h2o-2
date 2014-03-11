package water.api;

import hex.Quantiles;
import water.*;
import water.util.RString;
import water.util.Log;
import water.fvec.*;

public class QuantilesPage extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Returns a summary of a fluid-vec frame";

  @API(help="An existing H2O Frame key.", required=true, filter=Default.class)
  public Frame source_key;

  @API(help="Column to calculate quantile for", required=true, filter=responseFilter.class)
  public Vec column;
  class responseFilter extends VecClassSelect { responseFilter() { super("source_key"); } }

  @API(help = "Quantile desired (0.0-1.0). Median is 0.5. 0 and 1 are min/max", filter = Default.class, dmin = 0, dmax = 1)
  public double quantile = 0.5;

  @API(help = "Number of bins used (1-1000000). 1000 recommended", filter = Default.class, lmin = 1, lmax = 1000000)
  public int max_qbins = 1000;

  @API(help = "1: Exact result (iterate max 16). 0: One pass approx. 2: Provide both results", filter = Default.class, lmin = 0, lmax = 2)
  public int multiple_pass  = 1;

  @API(help = "Interpolation between rows. Type 2 (mean) or 7 (linear).", filter = Default.class)
  public int interpolation_type = 7;

  @API(help = "Column name.")
  String column_name;

  @API(help = "Quantile requested.")
  double quantile_requested;

  @API(help = "Interpolation type used.")
  int interpolation_type_used;

  @API(help = "False if an exact result is provided, True if the answer is interpolated.")
  boolean interpolated;

  @API(help = "Number of iterations actually performed.")
  int iterations;

  @API(help = "Result.")
  double result;

  @API(help = "Single pass Result.")
  double result_single;

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='QuantilesPage.query?source=%$key'>"+content+"</a>");
    rs.replace("key", k.toString());
    return rs.toString();
  }

  @Override protected Response serve() {
    if( source_key == null ) return RequestServer._http404.serve();
    if( column == null ) return RequestServer._http404.serve();
    if (column.isEnum()) {
      throw new IllegalArgumentException("Column is an enum");
    }
    if (! ((interpolation_type == 2) || (interpolation_type == 7))) {
      throw new IllegalArgumentException("Unsupported interpolation type. Currently only allow 2 or 7");
    }

    Vec[] vecs = new Vec[1];
    String[] names = new String[1];
    vecs[0] = column;
    names[0] = source_key.names()[source_key.find(column)];
    Frame fr = new Frame(names, vecs);

    Futures fs = new Futures();
    for( Vec vec : vecs) {
        vec.rollupStats(fs);
        // just to see, move to using these rather than the min/max/mean from basicStats
        double vmax = vec.max();
        double vmin = vec.min();
        double vmean = vec.mean();
        double vsigma = vec.sigma();
        long vnaCnt = vec.naCnt();
        boolean visInt = vec.isInt();
    }
    fs.blockForPending();

    // not used on the single pass approx. will use on multipass iterations
    double valStart = vecs[0].min();
    double valEnd = vecs[0].max();
    boolean multiPass;
    Quantiles[] qbins;

    double approxResult;
    double exactResult;
    result_single = Double.NaN; 
    result = Double.NaN; 
    boolean done = false;
    // approx (fully independent from the multipass)
    if ( multiple_pass == 0 || multiple_pass == 2 ) {
      multiPass = false;
      result_single = Double.NaN; 
      if ( multiple_pass == 0) result = Double.NaN;

      qbins = new Quantiles.BinTask2(quantile, max_qbins, valStart, valEnd, 
        multiPass, interpolation_type).doAll(fr)._qbins;
      // can we just overwrite it with a new one?
      Log.info("Q_ for approx. valStart: "+valStart+" valEnd: "+valEnd);

      // Have to get this internal state, and copy this state for the next iteration
      // in order to multipass
      // I guess forward as params to next iteration
      // while ( (iteration <= maxIterations) && !done ) {
      //  valStart   = newValStart;
      //  valEnd     = newValEnd;

      // These 3 are available for viewing, but not necessary to iterate
      //  valRange   = newValRange;
      //  valBinSize = newValBinSize;
      //  valLowCnt  = newValLowCnt;

      if (qbins != null) { // if it's enum it will be null?
        qbins[0].finishUp(vecs[0]);
        column_name = qbins[0].colname;
        quantile_requested = qbins[0].QUANTILES_TO_DO[0];
        iterations = 1;
        done = qbins[0]._done;
        approxResult = qbins[0]._pctile[0];
        interpolated = qbins[0]._interpolated;
        interpolation_type_used = qbins[0]._interpolationType;
      }
      else {
        column_name = "";
        quantile_requested = quantile;
        iterations = 0;
        done = false;
        approxResult = Double.NaN;
        interpolated = false;
        interpolation_type_used = interpolation_type;
      }

      result_single = approxResult;
      // only the best result if we only ran the approx
      if ( multiple_pass == 0) result = approxResult;

      // if max_qbins is set to 2? hmm. we won't resolve if max_qbins = 1
      // interesting to see how we resolve (should we disallow < 1000? (accuracy issues) but good for test)
      // done with that!
      qbins = null;
    }
    
    if ( multiple_pass == 1 || multiple_pass == 2 ) {
      final int MAX_ITERATIONS = 16; 
      multiPass = true;
      exactResult = Double.NaN; 
      // try a second pass
      Quantiles[] qbins2;
      int iteration;
      
      qbins2 = null;
      for (int b = 0; b < MAX_ITERATIONS; b++) {
        qbins2 = new Quantiles.BinTask2(quantile, max_qbins, valStart, valEnd, 
          multiPass, interpolation_type).doAll(fr)._qbins;
        iterations = b + 1;
        if ( qbins2 != null ) {
          qbins2[0].finishUp(vecs[0]);
          // for printing?
          double valRange = qbins2[0]._valRange;
          double valBinSize = qbins2[0]._valBinSize;
          Log.info("\nQ_ multipass iteration: "+iterations+" valStart: "+valStart+" valEnd: "+valEnd);
          Log.info("Q_ valBinSize: "+valBinSize);

          valStart = qbins2[0]._newValStart;
          valEnd = qbins2[0]._newValEnd;
          done = qbins2[0]._done;
          if ( done ) break;
        }
      }

      if (qbins2 != null) { // if it's enum it will be null?
        column_name = qbins2[0].colname;
        quantile_requested = qbins2[0].QUANTILES_TO_DO[0];
        done = qbins2[0]._done;
        exactResult = qbins2[0]._pctile[0];
        interpolated = qbins2[0]._interpolated;
        interpolation_type_used = qbins2[0]._interpolationType;
      }
      else {
        // enums must come this way. Right now we don't seem 
        // to create everything for the normal response, if we reject an enum col.
        // should fix that. For now, just hack it to not look for stuff
        column_name = "";
        quantile_requested = quantile;
        iterations = 0;
        done = false;
        exactResult = Double.NaN;
        interpolated = false;
        interpolation_type_used = interpolation_type;
      }

      // all done with it
      qbins2 = null;
      // always the best result if we ran here
      result = exactResult;
    }
    return Response.done(this);
  }
}
