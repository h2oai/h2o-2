package hex;

import java.util.Arrays;

import water.*;
import water.ValueArray.Column;
import water.util.Utils;

import com.google.common.base.Objects;
import com.google.gson.*;

public class Summary extends Iced {
  public static final int MAX_HIST_SZ = water.parser.Enum.MAX_ENUM_SIZE;
  public final static class ColSummary extends Iced {
    public transient Summary _summary;
    public final int _colId;
    final int NMAX = 5;
    public static final double [] DEFAULT_PERCENTILES = {0.01,0.05,0.10,0.25,0.33,0.50,0.66,0.75,0.90,0.95,0.99};
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

    ColSummary(Summary s, int colId) {
      this(s,colId,null);
    }

    ColSummary(Summary s, int colId, double [] percentiles) {
      _summary = s;
      _colId = colId;
      Column c = s.ary()._cols[colId];
      _enum = c.isEnum();
      // special case constant columns or columns entirely NA
      if(c._min == c._max || Double.isInfinite(c._min) || Double.isInfinite(c._max)) {
        _start = c._min;
        _binsz = _binszInv = 1;
        _end = _start+1;
        _percentiles = null;
        _bins = new long[1];
        _min = new double[]{c._min};
        _max = new double[]{c._max};
      } else {
        if(_enum){
          _percentiles = null;
          _binsz = _binszInv = 1;
          _bins = MemoryManager.malloc8((int)c.numDomainSize());
          _start = 0;
          _end = _bins.length;
          _min = _max = null;
        } else {
          final long n = Math.max(c._n,1);
          _min = MemoryManager.malloc8d(NMAX);
          _max = MemoryManager.malloc8d(NMAX);
          Arrays.fill(_min, Double.POSITIVE_INFINITY);
          Arrays.fill(_max, Double.NEGATIVE_INFINITY);
          if(c.isFloat() || c.numDomainSize() > MAX_HIST_SZ){
            double start, binsz;
            int nbin;
            double a = (c._max - c._min) / n;
            double b = Math.pow(10, Math.floor(Math.log10(a)));
            // selects among d, 5*d, and 10*d so that the number of
            // partitions go in [start, end] is closest to n
            if (a > 20*b/3)
               b *= 10;
            else if (a > 5*b/3)
               b *= 5;
            start = b * Math.floor(c._min / b);
            // guard against improper parse (date type) or zero c._sigma
            binsz = Math.max(1e-4, 3.5 *  c._sigma/ Math.cbrt(c._n));
            // Pick smaller of two for number of bins to avoid blowup of longs
            nbin = Math.max(Math.min(MAX_HIST_SZ,(int)((c._max - c._min) / binsz)),1);
            _bins = new long[nbin];
            _start = start;
            _binsz = binsz;
            _binszInv = 1.0/binsz;
            _end = start + nbin * binsz;
            _percentiles = Objects.firstNonNull(percentiles, DEFAULT_PERCENTILES);
          } else {
            _start = c._min;
            _end = c._max;
            int sz = (int)c.numDomainSize();
            _bins = new long[sz];
            _binszInv = _binsz = 1.0;
            _percentiles = Objects.firstNonNull(percentiles, DEFAULT_PERCENTILES);
          }
        }
      }
      assert !Double.isNaN(_start):"_start is NaN!";
    }
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
        final double s1 = _percentiles[j]*_n;
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

    void add(ColSummary other) {
      assert _bins.length == other._bins.length;

      // double infinity checks: column that is entirely NA
      assert Math.abs(_start - other._start) < 0.000001 || (Double.isInfinite(_start) && Double.isInfinite(other._start)):"start - other._start = " + (_start - other._start);
      assert Math.abs(_binszInv - other._binszInv) < 0.000000001;
      _n += other._n;
      _nzero += other._nzero;
      _n_na += other._n_na;

      Utils.add(_bins,other._bins);
      if(_min != null){
        int j = 0, k = 0;
        double [] min = _min.clone();
        double [] max = _max.clone();
        for(int i = 0; i < _min.length; ++i){
          if(other._min[k] < _min[j]){
            min[i] = other._min[k++];
          } else if(_min[j] < other._min[k]){
            min[i] = _min[j++];
          } else {
            min[i] = other._min[k];
            ++j; ++k;
          }
        }

        j = k = 0;
        for(int i = 0; i < _max.length; ++i){
          if(other._max[k] > _max[j]){
            max[i] = other._max[k++];
          } else if (_max[j] > other._max[k]){
            max[i] = _max[j++];
          } else {
            max[i] = other._max[k];
            ++j; ++k;
          }
        }
        _min = min;
        _max = max;
      }
    }

    void add(double val) {
      if(!_enum){
        if (val == 0.)
          _nzero++;
        // first update min/max
        if(val < _min[_min.length-1]){
          int j = _min.length-1;
          while(j > 0 && _min[j-1] > val)--j;
          if(j == 0 || _min[j-1] < val){ // skip dups
            for(int k = _min.length-1; k > j; --k)
              _min[k] = _min[k-1];
            _min[j] = val;
          }
        }

        if(val > _max[_max.length-1]){
          int j = _max.length-1;
          while(j > 0 && _max[j-1] < val)--j;
          if(j == 0 || _max[j-1] > val){ // skip dups
            for(int k = _max.length-1; k > j; --k)
              _max[k] = _max[k-1];
            _max[j] = val;
          }

        }
      }
      // update the histogram
      int binIdx = (_binsz == 1)
          ?Math.min((int)(val-_start),_bins.length-1)
          :Math.max(0,Math.min(_bins.length-1,(int)((val - _start) * _binszInv)));
      ++_bins[binIdx];
      ++_n;
    }

    public double binValue(int b){
      if(_binsz != 1)
        return _start + Math.max(0,(b-1))*_binsz + _binsz*0.5;
      else
        return _start + b;
    }
    public long binCount(int b){return _bins[b];}
    public double binPercent(int b){return 100*(double)_bins[b]/_n;}

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
      res.addProperty("name", _summary._ary._cols[_colId]._name);
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
        res.addProperty("mean", _summary._ary._cols[_colId]._mean);
        res.addProperty("sigma", _summary._ary._cols[_colId]._sigma + 0.f  /* elh: get rid of negative zero*/);
        res.addProperty("zeros", _nzero);
      }
      res.addProperty("N", _n);
      res.addProperty("na", _n_na);
      JsonObject histo = new JsonObject();
      histo.addProperty("bin_size", _binsz);
      histo.addProperty("nbins", _bins.length);
      JsonArray ary = new JsonArray();
      JsonArray binNames = new JsonArray();
      if(_summary._ary._cols[_colId].isEnum()){
        for(int i = 0; i < _summary._ary._cols[_colId]._domain.length; ++i){
          if(_bins[i] != 0){
            ary.add(new JsonPrimitive(_bins[i]));
            binNames.add(new JsonPrimitive(_summary._ary._cols[_colId]._domain[i]));
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
  private final ValueArray _ary;

  public ValueArray ary(){
    return _ary;
  }

  ColSummary [] _sums;
  int [] _cols;


  public Summary(ValueArray ary, int [] cols){
    assert ary != null;
    _ary = ary;
    _sums = new ColSummary [cols.length];
    _cols = cols;
    for(int i = 0; i < cols.length; ++i)
      _sums[i] = new ColSummary(this, cols[i]);
  }
  public Summary add(Summary other){
    for(int i = 0; i < _sums.length; ++i)
      _sums[i].add(other._sums[i]);
    return this;
  }
  public JsonObject toJson(){
    JsonObject res = new JsonObject();
    JsonArray sums = new JsonArray();
    for(int i = 0; i < _sums.length; ++i){
      _sums[i]._summary = this;
      sums.add(_sums[i].toJson());
    }
    res.add("columns",sums);
    return res;
  }
}
