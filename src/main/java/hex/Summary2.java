package hex;

import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.*;
import water.exec.Flow;
import water.parser.*;
import water.util.Utils;
import water.util.Log;

import java.util.Arrays;
import java.util.Random;

/**
 * Summary of a column.
 */
public class Summary2 extends Iced {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Returns a summary of a fluid-vec frame";

  public static final int    MAX_HIST_SZ = water.parser.Enum.MAX_ENUM_SIZE;
  public static final int    NMAX = 5;
  public static final int    RESAMPLE_SZ = 1000;
  public static final double DEFAULT_PERCENTILES[] = {0.01,0.05,0.10,0.25,0.33,0.50,0.66,0.75,0.90,0.95,0.99};
  private static final int   T_REAL = 0;
  private static final int   T_INT  = 1;
  private static final int   T_ENUM = 2;
  public BasicStat           _stat0;     /* Basic Vec stats collected by PrePass. */
  public final int           _type;      // 0 - real; 1 - int; 2 - enum
  public double[]            _mins;
  public double[]            _maxs;
  public double[]            _samples;
  long                       _gprows;    // non-empty rows per group

  final transient String[]   _domain;
  final transient double     _start;
  final transient double     _binsz;
  transient int              _len1;      /* Size of filled elements in a chunk. */
  transient double[]         _pctile;


  static abstract class Stats extends Iced {
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields

    @API(help="stats type"   ) public String type;
    Stats(String type) { this.type = type; }
  }
  // An internal JSON-output-only class
  @SuppressWarnings("unused")
  static class EnumStats extends Stats {
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    public EnumStats( int card ) {
      super("Enum");
      this.cardinality = card;
    }
    @API(help="cardinality"  ) public final int     cardinality;
  }

  static class NumStats extends Stats {
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    public NumStats( double mean, double sigma, long zeros, double[] mins, double[] maxs, double[] pctile) {
      super("Numeric");
      this.mean  = mean;
      this.sd    = sigma;
      this.zeros = zeros;
      this.mins  = mins;
      this.maxs  = maxs;
      this.pctile = pctile;
      this.pct   = DEFAULT_PERCENTILES;
    }
    @API(help="mean"        ) public final double   mean;
    @API(help="sd"          ) public final double   sd;
    @API(help="#zeros"      ) public final long     zeros;
    @API(help="min elements") public final double[] mins; // min N elements
    @API(help="max elements") public final double[] maxs; // max N elements
    @API(help="percentile thresholds" ) public final double[] pct;
    @API(help="percentiles" ) public final double[] pctile;
  }
  // OUTPUTS
  // Basic info
  @API(help="name"        ) public String    colname;
  @API(help="type"        ) public String    type;
  // Basic stats
  @API(help="NAs"         ) public long      nacnt;
  @API(help="Base Stats"  ) public Stats     stats;

  @API(help="histogram start")    public double    hstart;
  @API(help="histogram bin step") public double    hstep;
  @API(help="histogram headers" ) public String[]  hbrk;
  @API(help="histogram bin values") public long[]  hcnt;

  public static class BasicStat extends Iced {
    public long _len;   /* length of vec */
    public long _nas;   /* number of NA's */
    public long _nans;   /* number of NaN's */
    public long _pinfs;   /* number of positive infinity's */
    public long _ninfs;   /* number of positive infinity's */
    public long _zeros;   /* number of zeros */
    public double _min1;   /* if there's -Inf, then -Inf, o/w min2 */
    public double _max1;   /* if there's Inf, then Inf, o/w max2 */
    public double _min2;   /* min of the finite numbers. NaN if there's none. */
    public double _max2;   /* max of the finite numbers. NaN if there's none. */
    public BasicStat( ) {
      _len = 0;
      _nas = 0;
      _nans = 0;
      _pinfs = 0;
      _ninfs = 0;
      _zeros = 0;
      _min1 = Double.NaN;
      _max1 = Double.NaN;
      _min2 = Double.NaN;
      _max2 = Double.NaN;
    }
    public BasicStat add(Chunk chk) {
      _len = chk._len;
      for(int i = 0; i < chk._len; i++) {
        double val;
        if (chk.isNA0(i)) { _nas++; continue; }
        if (Double.isNaN(val = chk.at0(i))) { _nans++; continue; }
        if      (val == Double.POSITIVE_INFINITY) _pinfs++;
        else if (val == Double.NEGATIVE_INFINITY) _ninfs++;
        else {
          _min2 = Double.isNaN(_min2)? val : Math.min(_min2,val);
          _max2 = Double.isNaN(_max2)? val : Math.max(_max2,val);
          if (val == .0) _zeros++;
        }
      }
      return this;
    }
    public BasicStat add(BasicStat other) {
      _len += other._len;
      _nas += other._nas;
      _nans += other._nans;
      _pinfs += other._pinfs;
      _ninfs += other._ninfs;
      _zeros += other._zeros;
      if (Double.isNaN(_min2)) _min2 = other._min2;
      else if (!Double.isNaN(other._min2)) _min2 = Math.min(_min2,other._min2);
      if (Double.isNaN(_max2)) _max2 = other._max2;
      else if (!Double.isNaN(other._max2)) _max2 = Math.max(_max2, other._max2);
      return this;
    }
    public BasicStat finishUp() {
      _min1 = _ninfs>0?               Double.NEGATIVE_INFINITY   /* there's -Inf */
              : !Double.isNaN(_min2)? _min2                      /* min is finite */
              : _pinfs>0?             Double.POSITIVE_INFINITY   /* Only Infs exist */
              :                       Double.NaN;                /* All NaN's or NAs */
      _max1 = _pinfs>0?               Double.POSITIVE_INFINITY   /* there's Inf */
              : !Double.isNaN(_max2)? _max2                      /* max is finite */
              : _ninfs>0?             Double.NEGATIVE_INFINITY   /* Only -Infs exist */
              :                       Double.NaN;                /* All NaN's or NAs */
      return this;
    }

    /**
     * @return number of filled elements, excluding NaN's as well.
     */
    public long len1() {
      return _len - _nas - _nans;
    }
    /**
     * Returns whether the fill density is less than the given percent.
     * @param pct target percent.
     * @param nan if true then NaN is counted as missing.
     * @return true if less than {@code pct} of rows are filled. */
    public boolean isSparse(double pct, boolean nan) {
      assert 0 < pct && pct <= 1;
      return (double)(_len - _nas - (nan?_nans:0)) / _len < pct;
    }
  }

  public static class PrePass extends MRTask2<PrePass> {
    public BasicStat _basicStats[];
    @Override public void map(Chunk[] cs) {
      _basicStats = new BasicStat[cs.length];
      for (int c=0; c < cs.length; c++)
        _basicStats[c] = new BasicStat().add(cs[c]);
    }
    @Override public void reduce(PrePass other){
      for (int c = 0; c < _basicStats.length; c++)
        _basicStats[c].add(other._basicStats[c]);
    }
    public PrePass finishUp() {
      for (BasicStat stat : _basicStats) stat.finishUp();
      return this;
    }
  }
  public static class SummaryTask2 extends MRTask2<SummaryTask2> {
    private BasicStat[] _basics;
    public Summary2 _summaries[];
    public SummaryTask2 (BasicStat[] basicStats) { _basics = basicStats; }
    @Override public void map(Chunk[] cs) {
      _summaries = new Summary2[cs.length];
      for (int i = 0; i < cs.length; i++)
        _summaries[i] = new Summary2(_fr.vecs()[i],_fr.names()[i],_basics[i]).add(cs[i]);
    }
    @Override public void reduce(SummaryTask2 other) {
      for (int i = 0; i < _summaries.length; i++)
        _summaries[i].add(other._summaries[i]);
    }
  }

  // Entry point for the Flow passes, to allow easy percentiles on filtered GroupBy
  public static class SummaryPerRow extends Flow.PerRow<SummaryPerRow> {
    public final Frame _fr;
    public final Summary2 _summaries[];
    public SummaryPerRow( Frame fr ) { this(fr,null); }
    private SummaryPerRow( Frame fr, Summary2[] sums ) { _fr = fr; _summaries = sums; }
    @Override public void mapreduce( double ds[] ) { 
      for( int i=0; i<ds.length; i++ )
        _summaries[i].add(ds[i]);
    }
    @Override public void reduce( SummaryPerRow that ) { 
      for (int i = 0; i < _summaries.length; i++)
        _summaries[i].add(that._summaries[i]);
    }
    @Override public SummaryPerRow make() {
      Vec[] vecs = _fr.vecs();
      Summary2 sums[] = new Summary2[vecs.length];
      BasicStat basics[] = new PrePass().doAll(_fr).finishUp()._basicStats;
      for( int i=0; i<vecs.length; i++ )
        sums[i] = new Summary2(vecs[i],_fr._names[i],basics[i]);
      return new SummaryPerRow(_fr,sums);
    }
    @Override public String toString() {
      String s = "";
      for( int i=0; i<_summaries.length; i++ )
        s += _fr._names[i]+" "+_summaries[i]+"\n";
      return s;
    }
    public void finishUp() {
      Vec[] vecs = _fr.vecs();
      for (int i = 0; i < vecs.length; i++) 
        _summaries[i].finishUp(vecs[i]);
    }
  }

  @Override public String toString() {
    String s = "";
    if( stats instanceof NumStats ) {
      double pct   [] = ((NumStats)stats).pct   ;
      double pctile[] = ((NumStats)stats).pctile;
      for( int i=0; i<pct.length; i++ )
        s += ""+(pct[i]*100)+"%="+pctile[i]+", ";
    } else {
      s += "cardinality="+((EnumStats)stats).cardinality;
    }
    return s;
  }

  public void finishUp(Vec vec) {
    nacnt = _stat0._nas;
    if (_type == T_ENUM) {
      // Compute majority items for enum data
      computeMajorities();
    } else {
      _pctile = new double[DEFAULT_PERCENTILES.length];
      if (_samples != null) {
        Arrays.sort(_samples);
        // Compute percentiles for numeric data
        for (int i = 0; i < _pctile.length; i++)
          _pctile[i] = sampleQuantile(_samples,DEFAULT_PERCENTILES[i]);
      } else {
        approxQuantiles(_pctile,DEFAULT_PERCENTILES);
      }
    }

    // remove the trailing NaNs
    for (int i = 0; i < _mins.length; i++) {
      if (Double.isNaN(_mins[i])) {
        _mins = Arrays.copyOf(_mins, i);
        break;
      }
    }
    for (int i = 0; i < _maxs.length; i++) {
      if (Double.isNaN(_maxs[i])) {
        _maxs = Arrays.copyOf(_maxs, i);
        break;
      }
    }
    for (int i = 0; i < _maxs.length>>>1; i++) {
      double t = _maxs[i]; _maxs[i] = _maxs[_maxs.length-1-i]; _maxs[_maxs.length-1-i] = t;
    }
    this.stats = _type==T_ENUM?new EnumStats(vec.domain().length):new NumStats(vec.mean(),vec.sigma(),_stat0._zeros,_mins,_maxs,_pctile);
    if (_type == T_ENUM) {
      this.hstart = 0;
      this.hstep = 1;
      this.hbrk = _domain;
    } else {
      this.hstart = _start;
      this.hstep  = _binsz;
      this.hbrk = new String[hcnt.length];
      for (int i = 0; i < hbrk.length; i++)
        hbrk[i] = Utils.p2d(i==0?_start:binValue(i));
    }
  }

  public Summary2(Vec vec, String name, BasicStat stat0) {
    colname = name;
    _stat0 = stat0;
    _type = vec.isEnum()?2:vec.isInt()?1:0;
    _domain = vec.isEnum() ? vec.domain() : null;
    _gprows = 0;
    double sigma = Double.isNaN(vec.sigma()) ? 0 : vec.sigma();
    if ( _type != T_ENUM ) {
      _mins = MemoryManager.malloc8d((int)Math.min(vec.length(),NMAX));
      _maxs = MemoryManager.malloc8d((int)Math.min(vec.length(),NMAX));
      Arrays.fill(_mins, Double.NaN);
      Arrays.fill(_maxs, Double.NaN);
    } else {
      _mins = MemoryManager.malloc8d(Math.min(_domain.length,NMAX));
      _maxs = MemoryManager.malloc8d(Math.min(_domain.length,NMAX));
    }
    if( vec.isEnum() && _domain.length < MAX_HIST_SZ ) {
      _start = 0;
      _binsz = 1;
      hcnt = new long[_domain.length];
    } else if (!Double.isNaN(stat0._min2)) {
      // guard against improper parse (date type) or zero c._sigma
      long N = _stat0._len - stat0._nas - stat0._nans - stat0._pinfs - stat0._ninfs;
      double b = Math.max(1e-4,3.5 * sigma/ Math.cbrt(N));
      double d = Math.pow(10, Math.floor(Math.log10(b)));
      if (b > 20*d/3)
        d *= 10;
      else if (b > 5*d/3)
        d *= 5;

      // tweak for integers
      if (d < 1. && vec.isInt()) d = 1.;
      _binsz = d;
      _start = _binsz * Math.floor(stat0._min2/_binsz);
      int nbin = (int)(Math.round((stat0._max2 + (vec.isInt()?.5:0) - _start)*1000000.0/_binsz)/1000000L) + 1;
      assert nbin > 0;
      hcnt = new long[nbin];
    } else { // vec does not contain finite numbers
      _start = vec.min();
      _binsz = Double.POSITIVE_INFINITY;
      hcnt = new long[1];
    }
  }

  /**
   * Copy non-empty elements to an array.
   * @param chk Chunk to copy from
   * @return array of non-empty elements
   */
  double[] copy1(Chunk chk) {
    double[] dbls = new double[_len1==0?128:_len1];
    double val;
    int ns = 0;
    for (int i = 0; i < chk._len; i++) if (!chk.isNA0(i))
      if (!Double.isNaN(val = chk.at0(i))) {
        if (ns == dbls.length) dbls = Arrays.copyOf(dbls,dbls.length<<1);
        dbls[ns++] = val;
      }
    if (ns < dbls.length) dbls = Arrays.copyOf(dbls,ns);
    return dbls;
  }

  public double[] resample(Chunk chk) {
    Random r = new Random(chk._start);
    if (_stat0.len1() <= RESAMPLE_SZ) return copy1(chk);
    int ns = (int)(_len1*RESAMPLE_SZ/_stat0.len1()) + 1;
    double[] dbls = new double[ns];
    if (ns<<3 < _len1 && ns<<3 < chk._len) {
      // Chunk pretty dense, sample directly
      int n = 0;
      while (n < ns) {
        double val;
        int i = r.nextInt(chk._len);
        if (chk.isNA0(i)) continue;
        if (Double.isNaN(val = chk.at0(i))) continue;
        dbls[n++] = val;
      }
      return dbls;
    }
    dbls = copy1(chk);
    if (dbls.length <= ns) return dbls;
    for (int i = dbls.length-1; i >= ns; i--)
      dbls[r.nextInt(i+1)] = dbls[i];
    return Arrays.copyOf(dbls,ns);
  }

  public Summary2 add(Chunk chk) {
    for (int i = 0; i < chk._len; i++)
      add(chk.at0(i));
    _samples = resample(chk);
    return this;
  }
  public void add(double val) {
    if( Double.isNaN(val) ) return;
    _len1++; _gprows++;
    if ( _type != T_ENUM ) {
      int index;
      // update min/max
      if (val < _mins[_mins.length-1] || Double.isNaN(_mins[_mins.length-1])) {
        index = Arrays.binarySearch(_mins, val);
        if (index < 0) {
          index = -(index + 1);
          for (int j = _mins.length -1; j > index; j--)
            _mins[j] = _mins[j-1];
          _mins[index] = val;
        }
      }
      boolean hasNan = Double.isNaN(_maxs[_maxs.length-1]);
      if (val > _maxs[0] || hasNan) {
        index = Arrays.binarySearch(_maxs, val);
        if (index < 0) {
          index = -(index + 1);
          if (hasNan) {
            for (int j = _maxs.length -1; j > index; j--)
              _maxs[j] = _maxs[j-1];
            _maxs[index] = val;
          } else {
            for (int j = 0; j < index-1; j++)
              _maxs[j] = _maxs[j+1];
            _maxs[index-1] = val;
          }
        }
      }
    }

    // update histogram
    long binIdx;
    if (hcnt.length == 1) {
      binIdx = 0;
    }
    else if (val == Double.NEGATIVE_INFINITY) {
      binIdx = 0;
    }
    else if (val == Double.POSITIVE_INFINITY) {
      binIdx = hcnt.length-1;
    }
    else {
      binIdx = Math.round(((val - _start) * 1000000.0) / _binsz) / 1000000;
    }

    if ((int)binIdx >= hcnt.length) {
      assert false;
    }

    ++hcnt[(int)binIdx];
  }

  public Summary2 add(Summary2 other) {
    if (hcnt != null)
      Utils.add(hcnt, other.hcnt);
    _gprows += other._gprows;
    // merge samples
    double merged[] = new double[_samples.length+other._samples.length];
    System.arraycopy(_samples,0,merged,0,_samples.length);
    System.arraycopy(other._samples,0,merged,_samples.length,other._samples.length);
    _samples = merged;
    if (_type == T_ENUM) return this;
    double[] ds = MemoryManager.malloc8d(_mins.length);
    int i = 0, j = 0;
    for (int k = 0; k < ds.length; k++)
      if (_mins[i] < other._mins[j])
        ds[k] = _mins[i++];
      else if (Double.isNaN(other._mins[j]))
        ds[k] = _mins[i++];
      else {            // _min[i] >= other._min[j]
        if (_mins[i] == other._mins[j]) i++;
        ds[k] = other._mins[j++];
      }
    System.arraycopy(ds,0,_mins,0,ds.length);

    for (i = _maxs.length - 1; Double.isNaN(_maxs[i]); i--) if (i == 0) {i--; break;}
    for (j = _maxs.length - 1; Double.isNaN(other._maxs[j]); j--) if (j == 0) {j--; break;}

    ds = MemoryManager.malloc8d(i + j + 2);
    // merge two maxs, meanwhile deduplicating
    int k = 0, ii = 0, jj = 0;
    while (ii <= i && jj <= j) {
      if (_maxs[ii] < other._maxs[jj])
        ds[k] = _maxs[ii++];
      else if (_maxs[ii] > other._maxs[jj])
        ds[k] = other._maxs[jj++];
      else { // _maxs[ii] == other.maxs[jj]
        ds[k] = _maxs[ii++];
        jj++;
      }
      k++;
    }
    while (ii <= i) ds[k++] = _maxs[ii++];
    while (jj <= j) ds[k++] = other._maxs[jj++];

    System.arraycopy(ds,Math.max(0, k - _maxs.length),_maxs,0,Math.min(k,_maxs.length));
    for (int t = k; t < _maxs.length; t++) _maxs[t] = Double.NaN;
    return this;
  }

  // _start of each bin
  public double binValue(int b) { return _start + b*_binsz; }

  private double sampleQuantile(final double[] samples, final double threshold) {
    assert .0 <= threshold && threshold <= 1.0;
    int ix = (int)(samples.length * threshold);
    return ix<samples.length?samples[ix]:Double.NaN;
  }
  private int htot() {
    int cnt = 0;
    for (int i = 0; i < hcnt.length; i++) cnt+=hcnt[i];
    return cnt;
  }
  private void approxQuantiles(double[] qtiles, double[] thres){
    if( hcnt.length == 0 ) return;
    int k = 0;
    long s = 0;
    assert _gprows==htot();
    for(int j = 0; j < thres.length; ++j) {
      final double s1 = thres[j]*_gprows;
      long bc;
      while(s1 > s+(bc = hcnt[k])){
        s  += bc;
        k++;
      }
      qtiles[j] = _mins[0] + k*_binsz + ((_binsz > 1)?0.5*_binsz:0);
    }
  }
  // Compute majority categories for enums only
  public void computeMajorities() {
    if ( _type != T_ENUM ) return;
    for (int i = 0; i < _mins.length; i++) _mins[i] = i;
    for (int i = 0; i < _maxs.length; i++) _maxs[i] = i;
    int mini = 0, maxi = 0;
    for( int i = 0; i < hcnt.length; i++ ) {
      if (hcnt[i] < hcnt[(int)_mins[mini]]) {
        _mins[mini] = i;
        for (int j = 0; j < _mins.length; j++)
          if (hcnt[(int)_mins[j]] > hcnt[(int)_mins[mini]]) mini = j;
      }
      if (hcnt[i] > hcnt[(int)_maxs[maxi]]) {
        _maxs[maxi] = i;
        for (int j = 0; j < _maxs.length; j++)
          if (hcnt[(int)_maxs[j]] < hcnt[(int)_maxs[maxi]]) maxi = j;
      }
    }
    for (int i = 0; i < _mins.length - 1; i++)
      for (int j = 0; j < i; j++) {
        if (hcnt[(int)_mins[j]] > hcnt[(int)_mins[j+1]]) {
          double t = _mins[j]; _mins[j] = _mins[j+1]; _mins[j+1] = t;
        }
      }
    for (int i = 0; i < _maxs.length - 1; i++)
      for (int j = 0; j < i; j++)
        if (hcnt[(int)_maxs[j]] < hcnt[(int)_maxs[j+1]]) {
          double t = _maxs[j]; _maxs[j] = _maxs[j+1]; _maxs[j+1] = t;
        }
  }

  public double percentileValue(int idx) {
    if( _type == T_ENUM ) return Double.NaN;
     return _pctile[idx];
  }

  public void toHTML( Vec vec, String cname, StringBuilder sb ) {
    sb.append("<div class='table' id='col_" + cname + "' style='width:90%;heigth:90%;border-top-style:solid;'>" +
    "<div class='alert-success'><h4>Column: " + cname + " (type: " + type + ")</h4></div>\n");
    if ( _stat0._len == _stat0._nas ) {
      sb.append("<div class='alert'>Empty column, no summary!</div></div>\n");
      return;
    }
    // Base stats
    if( _type != T_ENUM ) {
      NumStats stats = (NumStats)this.stats;
      sb.append("<div style='width:100%;'><table class='table-bordered'>");
      sb.append("<tr><th colspan='"+20+"' style='text-align:center;'>Base Stats</th></tr>");
      sb.append("<tr>");
      sb.append("<th>NAs</th>  <td>" + nacnt + "</td>");
      sb.append("<th>mean</th><td>" + Utils.p2d(stats.mean)+"</td>");
      sb.append("<th>sd</th><td>" + Utils.p2d(stats.sd) + "</td>");
      sb.append("<th>zeros</th><td>" + stats.zeros + "</td>");
      sb.append("<th>min[" + stats.mins.length + "]</th>");
      for( double min : stats.mins ) {
        sb.append("<td>" + Utils.p2d(min) + "</td>");
      }
      sb.append("<th>max[" + stats.maxs.length + "]</th>");
      for( double max : stats.maxs ) {
        sb.append("<td>" + Utils.p2d(max) + "</td>");
      }
      // End of base stats
      sb.append("</tr> </table>");
      sb.append("</div>");
    } else {                    // Enums
      sb.append("<div style='width:100%'><table class='table-bordered'>");
      sb.append("<tr><th colspan='" + 4 + "' style='text-align:center;'>Base Stats</th></tr>");
      sb.append("<tr><th>NAs</th>  <td>" + nacnt + "</td>");
      sb.append("<th>cardinality</th>  <td>" + vec.domain().length + "</td></tr>");
      sb.append("</table></div>");
    }
    // Histogram
    final int MAX_HISTO_BINS_DISPLAYED = 1000;
    int len = Math.min(hcnt.length,MAX_HISTO_BINS_DISPLAYED);
    sb.append("<div style='width:100%;overflow-x:auto;'><table class='table-bordered'>");
    sb.append("<tr> <th colspan="+len+" style='text-align:center'>Histogram</th></tr>");
    sb.append("<tr>");
    if ( _type == T_ENUM )
       for( int i=0; i<len; i++ ) sb.append("<th>" + vec.domain(i) + "</th>");
    else
       for( int i=0; i<len; i++ ) sb.append("<th>" + Utils.p2d(i==0?_start:binValue(i)) + "</th>");
    sb.append("</tr>");
    sb.append("<tr>");
    for( int i=0; i<len; i++ ) sb.append("<td>" + hcnt[i] + "</td>");
    sb.append("</tr>");
    sb.append("<tr>");
    for( int i=0; i<len; i++ )
      sb.append(String.format("<td>%.1f%%</td>",(100.0*hcnt[i]/_stat0._len)));
    sb.append("</tr>");
    if( hcnt.length >= MAX_HISTO_BINS_DISPLAYED )
      sb.append("<div class='alert'>Histogram for this column was too big and was truncated to 1000 values!</div>");
    sb.append("</table></div>");

    if (_type != T_ENUM) {
      NumStats stats = (NumStats)this.stats;
      // Percentiles
      sb.append("<div style='width:100%;overflow-x:auto;'><table class='table-bordered'>");
      sb.append("<tr> <th colspan='" + stats.pct.length + "' " +
              "style='text-align:center' " +
              ">Percentiles</th></tr>");
      sb.append("<tr><th>Threshold(%)</th>");
      for (double pc : stats.pct)
        sb.append("<td>" + (int) Math.round(pc * 100) + "</td>");
      sb.append("</tr>");
      sb.append("<tr><th>Value</th>");
      for (double pv : stats.pctile)
        sb.append("<td>" + Utils.p2d(pv) + "</td>");
      sb.append("</tr>");
      sb.append("</table>");
      sb.append("</div>");
    }
    sb.append("</div>\n"); 
  }
}
