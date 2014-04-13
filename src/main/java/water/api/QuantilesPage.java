package water.api;

import hex.Quantiles;
import water.*;
import water.util.RString;
import water.util.Log;
import water.fvec.*;

public class QuantilesPage extends Func {
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

  // this isn't used yet. column_name is
  // class colsFilter1 extends MultiVecSelect { public colsFilter1() { super("source_key");} }
  // @API(help = "Not supported yet (Select columns)", filter=colsFilter1.class)
  // int[] cols;

  @API(help = "Maximum number of columns to show quantile", filter = Default.class, lmin = 1)
  int max_ncols = 1000;

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
  public double result;

  @API(help = "Single pass Result.")
  double result_single;

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='QuantilesPage.query?source=%$key'>"+content+"</a>");
    rs.replace("key", k.toString());
    return rs.toString();
  }

  @Override protected void init() throws IllegalArgumentException {
    super.init();
    if( source_key == null ) throw new IllegalArgumentException("Source key is missing");
    if( column == null )     throw new IllegalArgumentException("Column is missing");
    if( column.isEnum() )    throw new IllegalArgumentException("Column is an enum");
    if(! ((interpolation_type == 2) || (interpolation_type == 7)) ) {
      throw new IllegalArgumentException("Unsupported interpolation type. Currently only allow 2 or 7");
    }
  }

  @Override protected void execImpl() {
    String[] names = new String[1];

    Futures fs = new Futures();
    column.rollupStats(fs);
    fs.blockForPending();

    boolean multiPass;
    Quantiles[] qbins;

    // just take one here.
    // it's array because summary2 might use with a single pass list
    // and an exec single pass approx could pass a threshold list
    double [] quantiles_to_do = new double[1];
    quantiles_to_do[0] = quantile;

    double approxResult;
    double exactResult;
    result_single = Double.NaN;
    result = Double.NaN;
    boolean done = false;
    // approx (fully independent from the multipass)
    qbins = null;
    if ( multiple_pass == 0 || multiple_pass == 2 ) {
      multiPass = false;
      result_single = Double.NaN;
      if ( multiple_pass == 0) result = Double.NaN;

      // These are used as initial params, and setup for the next iteration
      // be sure to set again if multiple qbins are created
      double valStart = column.min();
      double valEnd = column.max();
      // quantile doesn't matter for the map/reduce binning
      qbins = new Quantiles.BinTask2(max_qbins, valStart, valEnd).doAll(column)._qbins;
      Log.debug("Q_ for approx. valStart: "+valStart+" valEnd: "+valEnd);

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

      interpolation_type_used = interpolation_type;
      quantile_requested = quantiles_to_do[0];
      if ( qbins != null ) { // if it's enum it will be null?
        qbins[0].finishUp(column, quantiles_to_do, interpolation_type, multiPass);
        column_name = names[0]; // the string name, not the param
        iterations = 1;
        done = qbins[0]._done;
        approxResult = qbins[0]._pctile[0];
        interpolated = qbins[0]._interpolated;
      }
      else {
        column_name = "";
        iterations = 0;
        done = false;
        approxResult = Double.NaN;
        interpolated = false;
      }

      result_single = approxResult;
      // only the best result if we only ran the approx
      if ( multiple_pass == 0 ) result = approxResult;

      // if max_qbins is set to 2? hmm. we won't resolve if max_qbins = 1
      // interesting to see how we resolve (should we disallow < 1000? (accuracy issues) but good for test)
    }

    if ( multiple_pass == 1 || multiple_pass == 2 ) {
      final int MAX_ITERATIONS = 16;
      multiPass = true;
      exactResult = Double.NaN;
      double valStart = column.min();
      double valEnd = column.max();

      for (int b = 0; b < MAX_ITERATIONS; b++) {
        // we did an approximation pass above we could reuse it for the first pass here?
        // quantile doesn't matter for the map/reduce binning
        // cleaned up things so no multipass behavior in qbins..all in finishUp:w
        // so can reuse the qbins from the approx pass above (if done)
        if ( !(multiple_pass==2 && b==0) ) {
          qbins = new Quantiles.BinTask2(max_qbins, valStart, valEnd).doAll(column)._qbins;
        }
        iterations = b + 1;
        if ( qbins == null ) break;
        else {
          qbins[0].finishUp(column, quantiles_to_do, interpolation_type, multiPass);
          Log.debug("\nQ_ multipass iteration: "+iterations+" valStart: "+valStart+" valEnd: "+valEnd);
          double valBinSize = qbins[0]._valBinSize;
          Log.debug("Q_ valBinSize: "+valBinSize);

          valStart = qbins[0]._newValStart;
          valEnd = qbins[0]._newValEnd;
          done = qbins[0]._done;
          if ( done ) break;
        }
      }

      interpolation_type_used = interpolation_type;
      quantile_requested = quantiles_to_do[0];
      if ( qbins != null ) { // if it's enum it will be null?
        column_name = names[0]; // string name, not the param
        done = qbins[0]._done;
        exactResult = qbins[0]._pctile[0];
        interpolated = qbins[0]._interpolated;
      }
      else {
        // enums must come this way. Right now we don't seem
        // to create everything for the normal response, if we reject an enum col.
        // should fix that. For now, just hack it to not look for stuff
        column_name = "";
        iterations = 0;
        done = false;
        exactResult = Double.NaN;
        interpolated = false;
      }

      // all done with it
      qbins = null;
      // always the best result if we ran here
      result = exactResult;
    }
  }
}
