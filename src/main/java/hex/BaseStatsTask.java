package hex;

import java.util.Arrays;

import water.*;
import water.ValueArray.Column;

/**
 * Class for storing and updating basic column stats (max, min, mean, sigma).
 * @author tomasnykodym
 *
 */
public final class BaseStatsTask extends MRTask {
  private double [] _min;
  private double [] _max;
  private double [] _sumsq;
  private double [] _sum;
  private long   [] _n;
  final ValueArray _ary;

  public BaseStatsTask(ValueArray ary){_ary = ary;}

  @Override public void map(Key key) {
    AutoBuffer bits = new AutoBuffer(DKV.get(key).memOrLoad());
    final int nrows = bits.remaining()/_ary.rowSize();
    final int ncols = _ary._cols.length;
    _min   = new double[ncols]; Arrays.fill(_min, Double.POSITIVE_INFINITY);
    _max   = new double[ncols]; Arrays.fill(_max, Double.NEGATIVE_INFINITY);
    _sum   = new double[ncols];
    _sumsq = new double[ncols];
    _n     = new long  [ncols];
    for(int r = 0; r < nrows; ++r){
      for(int c = 0; c < ncols; ++c){
        if(!_ary.isNA(bits, r,c)){
          double d = _ary.datad(bits, r, c);
          if(_min[c] > d)_min[c] = d;
          if(_max[c] < d)_max[c] = d;
          _sum  [c] += d;
          _sumsq[c] += d*d;
          _n[c]++;
        }
      }
    }
  }
  @Override public void reduce(DRemoteTask drt) {
    BaseStatsTask t = (BaseStatsTask)drt;
    if(_min == null){
      _min = t._min; _max = t._max; _sum = t._sum; _sumsq = t._sumsq; _n = t._n;
    } else {
      for(int c = 0; c < _min.length; ++c){
        if(_min[c] > t._min[c])_min[c] = t._min[c];
        if(_max[c] < t._max[c])_max[c] = t._max[c];
        _sum[c] += t._sum[c];
        _sumsq[c] += t._sumsq[c];
        _n[c] += t._n[c];
      }
    }
  }
  public double min(int c){return _min[c];}
  public double max(int c){return _max[c];}
  public double mean(int c){return _sum[c]/_n[c];}
  public double sigma(int c){
    double norm = 1.0/_n[c];
    double s = _sum[c]*norm;
    return  Math.sqrt(_sumsq[c]*norm - s*s);
  }
  public long nobs(int c){return _n[c];}
  public Column [] getCols(){
    Column [] res = new Column[_min.length];
    for(int i = 0; i < res.length; ++i){
      res[i] = _ary._cols[i].clone();
      res[i]._min = _min[i];
      res[i]._max = _max[i];
      res[i]._mean = mean(i);
      res[i]._sigma = sigma(i);
      res[i]._n = _n[i];
    }
    return res;
  }
}
