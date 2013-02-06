package hex;
import water.*;

import com.google.gson.JsonObject;

/**
* Calculate the covariance and correlation of two variables
*/
public abstract class Covariance {

  static public JsonObject run( ValueArray ary, int colA, int colB ) {
    COV_Task cov = new COV_Task();
    cov._arykey = ary._key;
    cov._colA = colA;
    cov._colB = colB;

    // Pass 1: compute sums for averages
    cov._pass = 1;
    long start = System.currentTimeMillis();
    cov.invoke(ary._key);
    long pass1 = System.currentTimeMillis();

    // Pass 2: Compute the product of variance for variance and covariance
    long n = ary._numrows;
    cov._pass = 2;
    cov._Xbar = cov._sumX / n;
    cov._Ybar = cov._sumY / n;
    cov.reinitialize();
    cov.invoke(ary._key);
    long pass2 = System.currentTimeMillis();

    // Compute results
    // We divide by n-1 since we lost a df by using a sample mean
    double varianceX = cov._XXbar / (n - 1);
    double varianceY = cov._YYbar / (n - 1);
    double sdX = Math.sqrt(varianceX);
    double sdY = Math.sqrt(varianceY);
    double covariance = cov._XYbar / (n - 1);
    double correlation = covariance / sdX / sdY;

    JsonObject res = new JsonObject();
    res.addProperty("Key", ary._key.toString());
    res.addProperty("ColA", ary._cols[colA]._name);
    res.addProperty("ColB", ary._cols[colB]._name);
    res.addProperty("Pass1Msecs", pass1 - start);
    res.addProperty("Pass2Msecs", pass2 - start);
    res.addProperty("Covariance", covariance);
    res.addProperty("Correlation", correlation);
    res.addProperty("XMean", cov._Xbar);
    res.addProperty("YMean", cov._Ybar);
    res.addProperty("XStdDev", sdX);
    res.addProperty("YStdDev", sdY);
    res.addProperty("XVariance", varianceX);
    res.addProperty("YVariance", varianceY);
    return res;
  }

  public static class COV_Task extends MRTask {
    Key _arykey; // Main ValueArray key
    int _pass; // Pass 1, or 2.
    int _colA, _colB; // Which columns to work on
    double _sumX,_sumY;
    double _Xbar, _Ybar, _XXbar, _YYbar, _XYbar;

    public void map( Key key ) {
      // Get the root ValueArray for the metadata
      ValueArray ary = ValueArray.value(DKV.get(_arykey));
      // Get the raw bits to work on
      AutoBuffer bits = ary.getChunk(key);
      final int rows = bits.remaining()/ary._rowsize;
      // Columns to work on
      ValueArray.Column A = ary._cols[_colA];
      ValueArray.Column B = ary._cols[_colB];

      // Loop over the data
      switch( _pass ) {
      case 1: // Pass 1
        // Run pass 1
        // Calculate sums for averages
        for( int i=0; i<rows; i++ ) {
          double X = ary.datad(bits,i,A);
          double Y = ary.datad(bits,i,B);
          _sumX += X;
          _sumY += Y;
        }
        break;

      case 2: // Pass 2
        // Run pass 2
        // Calculate the product of de-meaned variables
        for( int i=0; i<rows; i++ ) {
          double X = ary.datad(bits,i,A);
          double Y = ary.datad(bits,i,B);
          double Xa = (X-_Xbar);
          double Ya = (Y-_Ybar);
          _XXbar += Xa*Xa;
          _YYbar += Ya*Ya;
          _XYbar += Xa*Ya;
        }
        break;
      }
    }

    public void reduce( DRemoteTask rt ) {
      COV_Task cov = (COV_Task)rt;
      switch( _pass ) {
      case 1:
        _sumX += cov._sumX ;
        _sumY += cov._sumY ;
        break;
      case 2:
        _XXbar += cov._XXbar;
        _YYbar += cov._YYbar;
        _XYbar += cov._XYbar;
        break;
      }
    }
  }
}

