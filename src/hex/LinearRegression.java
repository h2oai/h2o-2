package hex;
import com.google.gson.JsonObject;
import water.*;

public abstract class LinearRegression {

  static public JsonObject run( ValueArray ary, int colA, int colB ) {
    // Pass 1: compute sums & sums-of-squares
    long start = System.currentTimeMillis();
    CalcSumsTask lr1 = new CalcSumsTask();
    lr1._arykey = ary._key;
    lr1._colA = colA;
    lr1._colB = colB;
    lr1.invoke(ary._key);
    long pass1 = System.currentTimeMillis();
    final long n = lr1._rows;

    // Pass 2: Compute squared errors
    CalcSquareErrorsTasks lr2 = new CalcSquareErrorsTasks();
    lr2._arykey = ary._key;
    lr2._colA = colA;
    lr2._colB = colB;
    lr2._Xbar = lr1._sumX / n;
    lr2._Ybar = lr1._sumY / n;
    lr2.invoke(ary._key);
    long pass2 = System.currentTimeMillis();

    // Compute the regression
    CalcRegressionTask lr3 = new CalcRegressionTask();
    lr3._arykey = ary._key;
    lr3._colA = colA;
    lr3._colB = colB;
    lr3._beta1 = lr2._XYbar / lr2._XXbar;
    lr3._beta0 = lr2._Ybar - lr3._beta1 * lr2._Xbar;
    lr3._Ybar = lr2._Ybar;
    lr3.invoke(ary._key);
    long pass3 = System.currentTimeMillis();

    long df = n - 2;
    double R2 = lr3._ssr / lr2._YYbar;
    double svar = lr3._rss / df;
    double svar1 = svar / lr2._XXbar;
    double svar0 = svar/n + lr2._Xbar*lr2._Xbar*svar1;

    JsonObject res = new JsonObject();
    res.addProperty("Key", ary._key.toString());
    res.addProperty("ColA", ary._cols[colA]._name);
    res.addProperty("ColB", ary._cols[colB]._name);
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

  public static class CalcSumsTask extends MRTask {
    Key _arykey; // Main ValueArray key
    int _colA, _colB; // Which columns to work on
    long _rows; // Rows used
    double _sumX,_sumY,_sumX2; // Sum of X's, Y's, X^2's

    public void map( Key key ) {
      assert key.home();
      // Get the root ValueArray for the metadata
      ValueArray ary = ValueArray.value(DKV.get(_arykey));
      // Get the raw bits to work on
      AutoBuffer bits = ary.getChunk(key);
      final int rows = bits.remaining()/ary._rowsize;
      // Columns to work on
      ValueArray.Column A = ary._cols[_colA];
      ValueArray.Column B = ary._cols[_colB];

      if( !ary.hasInvalidRows(_colA) && !ary.hasInvalidRows(_colB) ) {
        for( int i=0; i<rows; i++ ) {
          double X = ary.datad(bits,i,A);
          double Y = ary.datad(bits,i,B);
          _sumX += X;
          _sumY += Y;
          _sumX2+= X*X;
        }
        _rows = rows;
      } else {
        int bad = 0;
        for( int i=0; i<rows; i++ ) {
          if( ary.isNA(bits,i,A) || ary.isNA(bits,i,B) ) {
            bad++;
          } else {
            double X = ary.datad(bits,i,A);
            double Y = ary.datad(bits,i,B);
            _sumX += X;
            _sumY += Y;
            _sumX2+= X*X;
          }
        }
        _rows = rows-bad;
      }
    }

    public void reduce( DRemoteTask rt ) {
      CalcSumsTask lr1 = (CalcSumsTask)rt;
      _sumX += lr1._sumX ;
      _sumY += lr1._sumY ;
      _sumX2+= lr1._sumX2;
      _rows += lr1._rows ;
    }
  }


  public static class CalcSquareErrorsTasks extends MRTask {
    Key _arykey; // Main ValueArray key
    int _colA, _colB; // Which columns to work on
    double _Xbar, _Ybar, _XXbar, _YYbar, _XYbar;

    public void map( Key key ) {
      assert key.home();
      // Get the root ValueArray for the metadata
      ValueArray ary = ValueArray.value(DKV.get(_arykey));
      // Get the raw bits to work on
      AutoBuffer bits = ary.getChunk(key);
      final int rows = bits.remaining()/ary._rowsize;
      // Columns to work on
      ValueArray.Column A = ary._cols[_colA];
      ValueArray.Column B = ary._cols[_colB];

      final double Xbar = _Xbar;
      final double Ybar = _Ybar;

      if( !ary.hasInvalidRows(_colA) && !ary.hasInvalidRows(_colB) ) {
        for( int i=0; i<rows; i++ ) {
          double X = ary.datad(bits,i,A);
          double Y = ary.datad(bits,i,B);
          double Xa = (X-Xbar);
          double Ya = (Y-Ybar);
          _XXbar += Xa*Xa;
          _YYbar += Ya*Ya;
          _XYbar += Xa*Ya;
        }
      } else {
        for( int i=0; i<rows; i++ ) {
          if( !ary.isNA(bits,i,A) && !ary.isNA(bits,i,B) ) {
            double X = ary.datad(bits,i,A);
            double Y = ary.datad(bits,i,B);
            double Xa = (X-Xbar);
            double Ya = (Y-Ybar);
            _XXbar += Xa*Xa;
            _YYbar += Ya*Ya;
            _XYbar += Xa*Ya;
          }
        }
      }
    }

    public void reduce( DRemoteTask rt ) {
      CalcSquareErrorsTasks lr2 = (CalcSquareErrorsTasks)rt;
      _XXbar += lr2._XXbar;
      _YYbar += lr2._YYbar;
      _XYbar += lr2._XYbar;
    }
  }


  public static class CalcRegressionTask extends MRTask {
    Key _arykey; // Main ValueArray key
    int _colA, _colB; // Which columns to work on
    double _Ybar;
    double _beta0, _beta1;
    double _rss, _ssr;

    public void map( Key key ) {
      assert key.home();
      // Get the root ValueArray for the metadata
      ValueArray ary = ValueArray.value(DKV.get(_arykey));
      // Get the raw bits to work on
      AutoBuffer bits = ary.getChunk(key);
      final int rows = bits.remaining()/ary._rowsize;
      // Columns to work on
      ValueArray.Column A = ary._cols[_colA];
      ValueArray.Column B = ary._cols[_colB];

      final double beta0 = _beta0;
      final double beta1 = _beta1;
      final double Ybar = _Ybar ;

      if( !ary.hasInvalidRows(_colA) && !ary.hasInvalidRows(_colB) ) {
        for( int i=0; i<rows; i++ ) {
          double X = ary.datad(bits,i,A);
          double Y = ary.datad(bits,i,B);
          final double fit = beta1*X + beta0;
          final double rs = fit-Y;
          _rss += rs*rs;
          final double sr = fit-Ybar;
          _ssr += sr*sr;
        }
      } else {
        for( int i=0; i<rows; i++ ) {
          if( !ary.isNA(bits,i,A) && !ary.isNA(bits,i,B) ) {
            double X = ary.datad(bits,i,A);
            double Y = ary.datad(bits,i,B);
            final double fit = beta1*X + beta0;
            final double rs = fit-Y;
            _rss += rs*rs;
            final double sr = fit-Ybar;
            _ssr += sr*sr;
          }
        }
      }
    }

    public void reduce( DRemoteTask rt ) {
      CalcRegressionTask lr3 = (CalcRegressionTask)rt;
      _rss += lr3._rss;
      _ssr += lr3._ssr;
    }
  }
}
