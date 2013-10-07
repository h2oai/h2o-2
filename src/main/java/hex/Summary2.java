package hex;

import com.google.common.base.Objects;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import water.MemoryManager;
import water.fvec.Chunk;
import water.fvec.Vec;
import water.util.Utils;

import java.util.Arrays;

/**
 * Summary of a column.
 */
public class Summary2 extends Aggregate<Summary2> {
  public static final int MAX_HIST_SZ = water.parser.Enum.MAX_ENUM_SIZE;
  public static final double [] DEFAULT_PERCENTILES = {0.01,0.05,0.10,0.25,0.33,0.50,0.66,0.75,0.90,0.95,0.99};
  public final String _name;
  final int NMAX = 5;
  final long [] _bins; // bins for histogram
  long _n;
  long _nzero;
  long _n_na;
  final double _start, _end, _binsz, _binszInv;
  double [] _min; // min N elements
  double [] _max; // max N elements
  private double [] _percentileValues;
  final double [] _percentiles;
  final boolean _enum;

  public Summary2(Vec vec, String name, double[] percentiles) {
    super(vec);
    _name = name;
    _enum = vec.isEnum();
    if(vec.min() == vec.max()){ // constant columns pecial case,
    // not really any meaningfull data here, just don't blow up
      _start = vec.min();
      _binsz = _binszInv = 1;
      _end = _start+1;
      _percentiles = null;
      _bins = new long[1];
      _min = new double[]{vec.min()};
      _max = new double[]{vec.max()};
    } else {
      if(_enum){
        _percentiles = null;
        _binsz = _binszInv = 1;
        _bins = MemoryManager.malloc8(vec.domain().length);
        _start = 0;
        _end = _bins.length;
        _min = _max = null;
      } else {
        final long n = Math.max(vec.length(),1);
        _min = MemoryManager.malloc8d(NMAX);
        _max = MemoryManager.malloc8d(NMAX);
        Arrays.fill(_min, Double.POSITIVE_INFINITY);
        Arrays.fill(_max, Double.NEGATIVE_INFINITY);
        if(!vec.isEnum() || vec.domain().length > MAX_HIST_SZ){
          double start, binsz;
          int nbin;
          double a = (vec.max() - vec.min()) / n;
          double b = Math.pow(10, Math.floor(Math.log10(a)));
          // selects among d, 5*d, and 10*d so that the number of
          // partitions go in [start, end] is closest to n
          if (a > 20*b/3)
            b *= 10;
          else if (a > 5*b/3)
            b *= 5;
          start = b * Math.floor(vec.min() / b);
          // guard against improper parse (date type) or zero c._sigma
          binsz = Math.max(1e-4, 3.5 * vec.sigma()/ Math.cbrt(vec.length()));
          // Pick smaller of two for number of bins to avoid blowup of longs
          nbin = Math.max(Math.min(MAX_HIST_SZ,(int)((vec.max() - vec.min()) /
                  binsz)),1);
          _bins = new long[nbin];
          _start = start;
          _binsz = binsz;
          _binszInv = 1.0/binsz;
          _end = start + nbin * binsz;
          _percentiles = Objects.firstNonNull(percentiles, DEFAULT_PERCENTILES);
        } else {
          _start = vec.min();
          _end = vec.max();
          int sz = (int)vec.domain().length;
          _bins = new long[sz];
          _binszInv = _binsz = 1.0;
          _percentiles = Objects.firstNonNull(percentiles, DEFAULT_PERCENTILES);
        }
      }
    }
    assert !Double.isNaN(_start):"_start is NaN!";
  }

  @Override public Summary2 add(Chunk chk) {
    long start = chk._start;

    if (_enum) {
      for (int i = 0; i < chk._len; i++)
        if (chk.isNA(start + i))
          _n_na ++;
    } else {
      int maxmin = 0;
      int minmax = 0;

      for (int i = 0; i < chk._len; i++) {
        double val;
        if (chk.isNA(start + i))
          _n_na++;
        else {
          val = chk.at(start + i);
          if (val == 0.)
            _nzero++;
          // update min/max
          if (val < _min[maxmin]) {
            _min[maxmin] = val;
            for (int k = 0; k < _min.length; k++)
              if (_min[k] > _min[maxmin])
                maxmin = k;
          }
          if (val > _max[minmax]) {
            _max[minmax] = val;
            for (int k = 0; k < _max.length; k++)
              if (_max[k] < _max[minmax])
                minmax = k;
          }
          // update histogram
          int binIdx = (_binsz == 1)
                  ?Math.min((int)(val-_start),_bins.length-1)
                  :Math.min(_bins.length-1,(int)((val - _start) * _binszInv));
          ++_bins[binIdx];
        }
      }

      /* sort min and max */
      Arrays.sort(_min);
      Arrays.sort(_max);
    }
    _n += chk._len;
    return this;
  }

  @Override public Summary2 add(Summary2 other) {
    _n += other._n;
    _nzero += other._nzero;
    _n_na += other._n_na;

    Utils.add(_bins, other._bins);

    if (_enum)
      return this;

    double[] min = _min.clone();
    double[] max = _max.clone();

    int i = 0, j = 0;
    for (int k = 0; k < min.length; k++)
      min[k++] = _min[i] < other._min[j] ? _min[i++] : _min[j++];

    i = j = _max.length - 1;
    for (int k = max.length - 1; k >= 0; k--)
      max[k--] = _max[i] > other._max[j] ? _max[i--] : _max[j--];

    _min = min;
    _max = max;

    return this;
  }

  public double binValue(int b){
    if(_binsz != 1)
      return _start + Math.max(0,(b-1))*_binsz + _binsz*0.5;
    else
      return _start + b;
  }
  public long binCount(int b){return _bins[b];}
  public double binPercent(int b){return 100*(double)_bins[b]/_n;}

  public final double [] percentiles(){return _percentiles;}

  public long getEnumCardinality(){
    if (_enum)
      return _bins.length;
    else
      throw new IllegalArgumentException("summary: non enums don't have enum cardinality");
  }

  private void computePercentiles(){
    _percentileValues = new double [_percentiles.length];
    if( _bins.length == 0 ) return;
    int k = 0;
    long s = 0;
    for(int j = 0; j < _percentiles.length; ++j){
      final double s1 = _percentiles[j]*_n_na;
      long bc = 0;
      while(s1 > s+(bc = binCount(k))){
        s  += bc;
        k++;
      }
      _percentileValues[j] = _min[0] + k*_binsz + ((_binsz > 1)?0.5*_binsz:0);
    }
  }

  public double percentileValue(double threshold){
    if(_percentiles == null) throw new Error("Percentiles not available for enums!");
    int idx = Arrays.binarySearch(_percentiles, threshold);
    if(idx < 0) throw new Error("don't have requested percentile");
    if(_percentileValues == null)computePercentiles();
    return _percentileValues[idx];
  }

  public String toString(){
    StringBuilder res = new StringBuilder("ColumnSummary[" + _start + ":" + _end +", binsz=" + _binsz+"]");
    if(_percentiles != null) {
      for(double d:_percentiles)
        res.append(", p("+(int)(100*d)+"%)=" + percentileValue(d));
    }
    return res.toString();
  }

  public JsonObject toJson(){
    JsonObject res = new JsonObject();
    res.addProperty("type", _enum?"enum":"number");
    res.addProperty("name", _name);
    if (_enum)
      res.addProperty("enumCardinality", getEnumCardinality());
    if(!_enum){
      JsonArray min = new JsonArray();
      for(double d:_min){
        if(Double.isInfinite(d))break;
        min.add(new JsonPrimitive(d));
      }
      res.add("min", min);
      JsonArray max = new JsonArray();

      for(double d:_max){
        if(Double.isInfinite(d))break;
        max.add(new JsonPrimitive(d));
      }
      res.add("max", max);
      res.addProperty("mean", _vec.mean());
      res.addProperty("sigma", _vec.sigma());
      res.addProperty("zeros", _nzero);
    }
    res.addProperty("N", _n);
    res.addProperty("na", _n_na);
    JsonObject histo = new JsonObject();
    histo.addProperty("bin_size", _binsz);
    histo.addProperty("nbins", _bins.length);
    JsonArray ary = new JsonArray();
    JsonArray binNames = new JsonArray();
    if(_enum){
      for(int i = 0; i < _vec.domain().length; ++i){
        if(_bins[i] != 0){
          ary.add(new JsonPrimitive(_bins[i]));
          binNames.add(new JsonPrimitive(_vec.domain()[i]));
        }
      }
    } else {
      double x = _min[0];
      if(_binsz != 1)x += _binsz*0.5;
      for(int i = 0; i < _bins.length; ++i){
        if(_bins[i] != 0){
          ary.add(new JsonPrimitive(_bins[i]));
          binNames.add(new JsonPrimitive(Utils.p2d(x + i*_binsz)));
        }
      }
    }
    histo.add("bin_names", binNames);
    histo.add("bins", ary);
    res.add("histogram", histo);
    if(!_enum && _percentiles != null){
      if(_percentileValues == null)computePercentiles();
      JsonObject percentiles = new JsonObject();
      JsonArray thresholds = new JsonArray();
      JsonArray values = new JsonArray();
      for(int i = 0; i < _percentiles.length; ++i){
        thresholds.add(new JsonPrimitive(_percentiles[i]));
        values.add(new JsonPrimitive(_percentileValues[i]));
      }
      percentiles.add("thresholds", thresholds);
      percentiles.add("values", values);
      res.add("percentiles", percentiles);
    }
    return res;
  }
}
