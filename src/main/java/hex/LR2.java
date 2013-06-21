package hex;

import com.google.gson.JsonObject;
import water.fvec.*;
import water.*;

public abstract class LR2 {

  public static JsonObject run( Vec X, Vec Y ) {
    // Pass 1: compute sums & sums-of-squares
    long start = System.currentTimeMillis();
    CalcSumsTask lr1 = new CalcSumsTask().invoke(X,Y);
    long pass1 = System.currentTimeMillis();
    final long n = lr1._n;

    // Pass 2: Compute squared errors
    final double meanX = lr1._sumX/n;
    final double meanY = lr1._sumY/n;
    CalcSquareErrorsTasks lr2 = new CalcSquareErrorsTasks(meanX, meanY).invoke(X,Y);
    long pass2 = System.currentTimeMillis();

    // Compute the regression
    double beta1 = lr2._XYbar / lr2._XXbar;
    double beta0 = meanY - beta1 * meanX;
    CalcRegressionTask lr3 = new CalcRegressionTask(beta0,beta1,meanY).invoke(X,Y);
    long pass3 = System.currentTimeMillis();

    long df = n - 2;
    double R2 = lr3._ssr / lr2._YYbar;
    double svar = lr3._rss / df;
    double svar1 = svar / lr2._XXbar;
    double svar0 = svar/n + meanX*meanX*svar1;

    JsonObject res = new JsonObject();
    res.addProperty("Pass1Msecs", pass1 - start);
    res.addProperty("Pass2Msecs", pass2-pass1);
    res.addProperty("Pass3Msecs", pass3-pass2);
    res.addProperty("Rows", n);
    res.addProperty("Beta0", lr3._beta0);
    res.addProperty("Beta1", lr3._beta1);
    res.addProperty("RSquared", R2);
    res.addProperty("Beta0StdErr", Math.sqrt(svar0));
    res.addProperty("Beta1StdErr", Math.sqrt(svar1));
    res.addProperty("SSTO", lr2._YYbar);
    res.addProperty("SSE", lr3._rss);
    res.addProperty("SSR", lr3._ssr);
    return res;
  }

  public static class CalcSumsTask extends MRTask2<CalcSumsTask> {
    long _n; // Rows used
    double _sumX,_sumY,_sumX2; // Sum of X's, Y's, X^2's
    @Override public void map( Chunk xs, Chunk ys ) {
      for( int i=0; i<xs._len; i++ ) {
        double X = xs.getd(i);
        double Y = ys.getd(i);
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
        double Xa = xs.getd(i);
        double Ya = ys.getd(i);
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
        double X = xs.getd(i); double Y = ys.getd(i);
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
}
