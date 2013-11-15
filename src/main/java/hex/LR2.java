package hex;

import water.*;
import water.api.DocGen;
import water.fvec.*;
import water.util.RString;

public class LR2 extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Linear Regression between 2 columns";

  @API(help="Data Frame", required=true, filter=Default.class)
  Frame source;

  @API(help="Column X", required=true, filter=LR2VecSelect.class)
  Vec vec_x;

  @API(help="Column Y", required=true, filter=LR2VecSelect.class)
  Vec vec_y;
  class LR2VecSelect extends VecSelect { LR2VecSelect() { super("source"); } }

  @API(help="Pass 1 msec")     long pass1time;
  @API(help="Pass 2 msec")     long pass2time;
  @API(help="Pass 3 msec")     long pass3time;
  @API(help="nrows")           long nrows;
  @API(help="beta0")           double beta0;
  @API(help="beta1")           double beta1;
  @API(help="r-squared")       double r2;
  @API(help="SSTO")            double ssto;
  @API(help="SSE")             double sse;
  @API(help="SSR")             double ssr;
  @API(help="beta0 Std Error") double beta0stderr;
  @API(help="beta1 Std Error") double beta1stderr;

  @Override public Response serve() {
    // Pass 1: compute sums & sums-of-squares
    long start = System.currentTimeMillis();
    CalcSumsTask lr1 = new CalcSumsTask().doAll(vec_x, vec_y);
    long pass1 = System.currentTimeMillis();
    pass1time = pass1 - start;
    nrows = lr1._n;

    // Pass 2: Compute squared errors
    final double meanX = lr1._sumX/nrows;
    final double meanY = lr1._sumY/nrows;
    CalcSquareErrorsTasks lr2 = new CalcSquareErrorsTasks(meanX, meanY).doAll(vec_x, vec_y);
    long pass2 = System.currentTimeMillis();
    pass2time = pass2 - pass1;
    ssto = lr2._YYbar;

    // Compute the regression
    beta1 = lr2._XYbar / lr2._XXbar;
    beta0 = meanY - beta1 * meanX;
    CalcRegressionTask lr3 = new CalcRegressionTask(beta0, beta1, meanY).doAll(vec_x, vec_y);
    long pass3 = System.currentTimeMillis();
    pass3time = pass3 - pass2;

    long df = nrows - 2;
    r2 = lr3._ssr / lr2._YYbar;
    double svar = lr3._rss / df;
    double svar1 = svar / lr2._XXbar;
    double svar0 = svar/nrows + meanX*meanX*svar1;
    beta0stderr = Math.sqrt(svar0);
    beta1stderr = Math.sqrt(svar1);
    sse = lr3._rss;
    ssr = lr3._ssr;

    return Response.done(this);
  }

  public static class CalcSumsTask extends MRTask2<CalcSumsTask> {
    long _n; // Rows used
    double _sumX,_sumY,_sumX2; // Sum of X's, Y's, X^2's
    @Override public void map( Chunk xs, Chunk ys ) {
      for( int i=0; i<xs._len; i++ ) {
        double X = xs.at0(i);
        double Y = ys.at0(i);
        if( !Double.isNaN(X) && !Double.isNaN(Y)) {
          _sumX += X;
          _sumY += Y;
          _sumX2+= X*X;
          _n++;
        }
      }
    }
    @Override public void reduce( CalcSumsTask lr1 ) {
      _sumX += lr1._sumX ;
      _sumY += lr1._sumY ;
      _sumX2+= lr1._sumX2;
      _n += lr1._n;
    }
  }


  public static class CalcSquareErrorsTasks extends MRTask2<CalcSquareErrorsTasks> {
    final double _meanX, _meanY;
    double _XXbar, _YYbar, _XYbar;
    CalcSquareErrorsTasks( double meanX, double meanY ) { _meanX = meanX; _meanY = meanY; }
    @Override public void map( Chunk xs, Chunk ys ) {
      for( int i=0; i<xs._len; i++ ) {
        double Xa = xs.at0(i);
        double Ya = ys.at0(i);
        if(!Double.isNaN(Xa) && !Double.isNaN(Ya)) {
          Xa -= _meanX;
          Ya -= _meanY;
          _XXbar += Xa*Xa;
          _YYbar += Ya*Ya;
          _XYbar += Xa*Ya;
        }
      }
    }
    @Override public void reduce( CalcSquareErrorsTasks lr2 ) {
      _XXbar += lr2._XXbar;
      _YYbar += lr2._YYbar;
      _XYbar += lr2._XYbar;
    }
  }


  public static class CalcRegressionTask extends MRTask2<CalcRegressionTask> {
    final double _meanY;
    final double _beta0, _beta1;
    double _rss, _ssr;
    CalcRegressionTask(double beta0, double beta1, double meanY) {_beta0=beta0; _beta1=beta1; _meanY=meanY;}
    @Override public void map( Chunk xs, Chunk ys ) {
      for( int i=0; i<xs._len; i++ ) {
        double X = xs.at0(i); double Y = ys.at0(i);
        if( !Double.isNaN(X) && !Double.isNaN(Y) ) {
          double fit = _beta1*X + _beta0;
          double rs = fit-Y;
          _rss += rs*rs;
          double sr = fit-_meanY;
          _ssr += sr*sr;
        }
      }
    }

    @Override public void reduce( CalcRegressionTask lr3 ) {
      _rss += lr3._rss;
      _ssr += lr3._ssr;
    }
  }

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='LR2.query?data_key=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }
}
