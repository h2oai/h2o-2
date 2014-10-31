package hex;

import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.*;
import water.exec.Flow;
import water.util.Utils;
import water.util.Log;

import java.util.Arrays;

/**
 * Summary of a column.
 */
public class Summary2 extends Iced {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Returns a summary of a fluid-vec frame";

  public static final int    MAX_HIST_SZ = H2O.DATA_MAX_FACTOR_LEVELS;
  public static final int    NMAX = 5;
  // updated boundaries to be 0.1% 1%...99%, 99.9% so R code didn't have to change
  // ideally we extend the array here, and just update the R extraction of 25/50/75 percentiles
  // note python tests (junit?) may look at result
  public static final double DEFAULT_PERCENTILES[] = {0.001,0.01,0.10,0.25,0.33,0.50,0.66,0.75,0.90,0.99,0.999};
  private static final int   T_REAL = 0;
  private static final int   T_INT  = 1;
  private static final int   T_ENUM = 2;
  public BasicStat           _stat0;     /* Basic Vec stats collected by PrePass. */
  public final int           _type;      // 0 - real; 1 - int; 2 - enum
  public double[]            _mins;
  public double[]            _maxs;
  long                       _gprows;    // non-empty rows per group

  final transient String[]   _domain;
  final transient double     _start;
  final transient double     _start2;
  final transient double     _binsz;
  final transient double     _binsz2;    // 2nd finer grained histogram used for quantile estimates for numerics
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
  public long[]  hcnt2; // finer histogram. not visible
  public double[]  hcnt2_min; // min actual for each bin
  public double[]  hcnt2_max; // max actual for each bin

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
        if( chk._vec.isUUID() ) continue;
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
      else if (!Double.isNaN(other._min2)) _min2 = Math.min(_min2, other._min2);
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
    private int _max_qbins;
    public Summary2 _summaries[];
    public SummaryTask2 (BasicStat[] basicStats, int max_qbins) { _basics = basicStats; _max_qbins = max_qbins; }
    @Override public void map(Chunk[] cs) {
      _summaries = new Summary2[cs.length];
      for (int i = 0; i < cs.length; i++)
        _summaries[i] = new Summary2(_fr.vecs()[i], _fr.names()[i], _basics[i], _max_qbins).add(cs[i]);
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
        sums[i] = new Summary2(vecs[i], _fr._names[i], basics[i]);
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
      approxQuantiles(_pctile, DEFAULT_PERCENTILES, _stat0._min2, _stat0._max2);
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
      double t = _maxs[i];
      _maxs[i] = _maxs[_maxs.length-1-i];
      _maxs[_maxs.length-1-i] = t;
    }
    this.stats = _type==T_ENUM ?
      new EnumStats(vec.domain().length) :
      new NumStats(vec.mean(), vec.sigma(), _stat0._zeros, _mins, _maxs, _pctile);

    if (_type == T_ENUM) {
      this.hstart = 0;
      this.hstep = 1;
      this.hbrk = _domain;
    } else {
      this.hstart = _start;
      this.hstep  = _binsz;
      this.hbrk = new String[hcnt.length];
      for (int i = 0; i < hbrk.length; i++)
        hbrk[i] = Utils.p2d(i==0 ? _start : binValue(i));
    }
  }

  public Summary2(Vec vec, String name, BasicStat stat0, int max_qbins) {
    colname = name;
    _stat0 = stat0;
    _type = vec.isEnum()?T_ENUM:vec.isInt()?T_INT:T_REAL;
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
      _start2 = 0;
      _binsz = 1;
      _binsz2 = 1;
      // hack for now. if there are no enum values, keep these length 1, for consistency
      // in asserts below
      int dlength = _domain.length==0 ? 1 : _domain.length;
      hcnt = new long[dlength];
      hcnt2 = new long[dlength];
      hcnt2_min = new double[dlength];
      hcnt2_max = new double[dlength];
    }
    else if ( !(Double.isNaN(stat0._min2) || Double.isNaN(stat0._max2)) ) {
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

      // Result from the dynamic bin sizing equations
      double startSuggest = d * Math.floor(stat0._min2 / d);
      double binszSuggest = d;
      int nbinSuggest = (int) Math.ceil((stat0._max2 - startSuggest)/d) + 1;

      // Protect against massive binning. browser doesn't need
      int BROWSER_BIN_TARGET = 100;

      //  _binsz/_start is used in the histogramming.
      // nbin is used in the array declaration. must be big enough.
      // the resulting nbin, could be really large number. We need to cap it.
      // should also be obsessive and check that it's not 0 and force to 1.
      // Since nbin is implied by _binsz, ratio _binsz and recompute nbin
      int binCase = 0; // keep track in case we assert
      double start;
      if ( stat0._max2==stat0._min2) {
        binszSuggest = 0; // fixed next with other 0 cases.
        start = stat0._min2;
        binCase = 1;
      }
      // minimum 2 if min/max different
      else if ( stat0._max2!=stat0._min2 && nbinSuggest<2 ) {
        binszSuggest = (stat0._max2 - stat0._min2) / 2.0;
        start = stat0._min2;
        binCase = 2;
      }
      else if (nbinSuggest<1 || nbinSuggest>BROWSER_BIN_TARGET ) {
        // switch to a static equation with a fixed bin count, and recompute binszSuggest
        // one more bin than necessary for the range (99 exact. causes one extra
        binszSuggest = (stat0._max2 - stat0._min2) / (BROWSER_BIN_TARGET - 1.0);
        start = binszSuggest * Math.floor(stat0._min2 / binszSuggest);
        binCase = 3;
      }
      else {
        // align to binszSuggest boundary. (this is for reals)
        start = binszSuggest * Math.floor(stat0._min2 / binszSuggest);
        binCase = 4;
      }

      // _binsz = 0 means min/max are equal for reals?. Just make it a little number
      // this won't show up in browser display, since bins are labelled by start value

      // Now that we know the best bin size that will fit..Floor the _binsz if integer so visible
      // histogram looks good for integers. This is our final best bin size.
      double binsz = (binszSuggest!=0) ? binszSuggest : (vec.isInt() ? 1 : 1e-13d);
      _binsz = vec.isInt() ? Math.floor(binsz) : binsz;
      // make integers start on an integer too!
      _start = vec.isInt() ? Math.floor(start) : start;

      // This equation creates possibility of some of the first bins being empty
      // also: _binsz means many _binsz2 could be empty at the start if we resused _start there
      // FIX! is this okay if the dynamic range is > 2**32
      // align to bin size?
      int nbin = (int) Math.ceil((stat0._max2 - _start)/_binsz) + 1;
      double impliedBinEnd = _start + (nbin * _binsz);
      String assertMsg = _start+" "+_stat0._min2+" "+_stat0._max2+
        " "+impliedBinEnd+" "+_binsz+" "+nbin+" "+startSuggest+" "+nbinSuggest+" "+binCase;

      // Log.debug("Summary2 bin1. "+assertMsg);
      assert _start <= _stat0._min2 : assertMsg;
      // just in case, make sure it's big enough
      assert nbin > 0: assertMsg;
      // just for double checking we're okay (nothing outside the bin rang)
      assert impliedBinEnd>=_stat0._max2 : assertMsg;

      // create a 2nd finer grained historam for quantile estimates.
      // okay if it is approx. 1000 bins (+-1)
      // update: we allow api to change max_qbins. default 1000. larger = more accuracy
      assert max_qbins > 0 && max_qbins <= 10000000 : "max_qbins must be >0 and <= 10000000";

      // okay if 1 more than max_qbins gets created
      double d2 = (stat0._max2 - stat0._min2) / max_qbins;
      // _binsz2 = 0 means min/max are equal for reals?. Just make it a little number
      // this won't show up in browser display, since bins are labelled by start value
      _binsz2 = (d2!=0) ? d2 : (vec.isInt() ? 1 : 1e-13d);
      _start2 = stat0._min2;
      int nbin2 = (int) Math.ceil((stat0._max2 - _start2)/_binsz2) + 1;
      double impliedBinEnd2 = _start2 + (nbin2 * _binsz2);

      assertMsg = _start2+" "+_stat0._min2+" "+_stat0._max2+
        " "+impliedBinEnd2+" "+_binsz2+" "+nbin2;
      // Log.debug("Summary2 bin2. "+assertMsg);
      assert _start2 <= stat0._min2 : assertMsg;
      assert nbin2 > 0 : assertMsg;
      // can't make any assertion about _start2 vs _start  (either can be smaller due to fp issues)
      assert impliedBinEnd2>=_stat0._max2 : assertMsg;

      hcnt = new long[nbin];
      hcnt2 = new long[nbin2];
      hcnt2_min = new double[nbin2];
      hcnt2_max = new double[nbin2];

      // Log.debug("Finer histogram has "+nbin2+" bins. Visible histogram has "+nbin);
      // Log.debug("Finer histogram starts at "+_start2+" Visible histogram starts at "+_start);
      // Log.debug("stat0._min2 "+stat0._min2+" stat0._max2 "+stat0._max2);

    }
    else { // vec does not contain finite numbers
      Log.debug("Summary2: NaN in stat0._min2: "+stat0._min2+" or stat0._max2: "+stat0._max2);
      // vec.min() wouldn't be any better here. It could be NaN? 4/13/14
      // _start = vec.min();
      // _start2 = vec.min();
      // _binsz = Double.POSITIVE_INFINITY;
      // _binsz2 = Double.POSITIVE_INFINITY;
      _start = Double.NaN;
      _start2 = Double.NaN;
      _binsz = Double.NaN;
      _binsz2 = Double.NaN;
      hcnt = new long[1];
      hcnt2 = new long[1];
      hcnt2_min = new double[1];
      hcnt2_max = new double[1];
    }
  }

  public Summary2(Vec vec, String name, BasicStat stat0) {
    this(vec, name, stat0, 1000);
  }

  public Summary2 add(Chunk chk) {
    if( chk._vec.isUUID() ) {
      // Log.info("Summary2: isUUID() in add");
      return this;
    }
    for (int i = 0; i < chk._len; i++)
      add(chk.at0(i));
    return this;
  }
  public void add(double val) {
    if( Double.isNaN(val) ) return;
    // can get infinity due to bad enum parse to real
    // histogram is sized ok, but the index calc below will be too big
    // just drop them. not sure if something better to do?
    if( val==Double.POSITIVE_INFINITY ) return;
    if( val==Double.NEGATIVE_INFINITY ) return;
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
      // update the finer histogram (used for quantile estimates on numerics)
      long binIdx2;
      if (hcnt2.length==1) {
        binIdx2 = 0; // not used
      }
      else {
        binIdx2 = (int) Math.floor((val - _start2) / _binsz2);
      }

      int binIdx2Int = (int) binIdx2;
      assert (_start2 <= val) : "Why is val < _start2? val:"+val+" _start2:";
      assert (binIdx2Int >= 0 && binIdx2Int < hcnt2.length) :
        "binIdx2Int too big for hcnt2 "+binIdx2Int+" "+hcnt2.length+" "+val+" "+_start2+" "+_binsz2;

      if (hcnt2[binIdx2Int] == 0) {
        // Log.debug("New init: "+val+" for index "+binIdx2Int);
        hcnt2_min[binIdx2Int] = val;
        hcnt2_max[binIdx2Int] = val;
      }
      else {
        if (val < hcnt2_min[binIdx2Int]) {
            // Log.debug("New min: "+val+" for index "+binIdx2Int);
            hcnt2_min[binIdx2Int] = val;
        }
        if (val > hcnt2_max[binIdx2Int]) {
            // if ( binIdx2Int == 500 ) Log.debug("New max: "+val+" for index "+binIdx2Int);
            hcnt2_max[binIdx2Int] = val;
        }
      }
      ++hcnt2[binIdx2Int];
    }

    // update the histogram the browser/json uses
    long binIdx;
    if (hcnt.length == 1) {
      binIdx = 0;
    }
    // interesting. do we really track Infs in the histogram?
    else if (val == Double.NEGATIVE_INFINITY) {
      binIdx = 0;
    }
    else if (val == Double.POSITIVE_INFINITY) {
      binIdx = hcnt.length-1;
    }
    else {
      binIdx = (int) Math.floor((val - _start) / _binsz);
    }

    int binIdxInt = (int) binIdx;
    assert (_start <= val) : "Why is val < _start? val:"+val+" _start:";
    assert (binIdxInt >= 0 && binIdx < hcnt.length) :
        "binIdxInt bad for hcnt2. binIdxInt:"+binIdxInt+" hcnt.length:"+hcnt.length+" val:"+val+" _start:"+_start+" _binsz:"+_binsz;
    ++hcnt[binIdxInt];
  }

  public Summary2 add(Summary2 other) {
    // merge hcnt and hcnt just by adding
    if (hcnt != null)
      Utils.add(hcnt, other.hcnt);

    _gprows += other._gprows;

    if (_type == T_ENUM) return this;

    // merge hcnt2 per-bin mins
    // other must be same length, but use it's length for safety
    // could add assert on lengths?
    for (int k = 0; k < other.hcnt2_min.length; k++) {
      // for now..die on NaNs
      assert !Double.isNaN(other.hcnt2_min[k]) : "NaN in other.hcnt2_min merging";
      assert !Double.isNaN(other.hcnt2[k]) : "NaN in hcnt2_min merging";
      assert !Double.isNaN(hcnt2_min[k]) : "NaN in hcnt2_min merging";
      assert !Double.isNaN(hcnt2[k]) : "NaN in hcnt2_min merging";

      // cover the initial case (relying on initial min = 0 to work is wrong)
      // Only take the new max if it's hcnt2 is non-zero. like a valid bit
      // can hcnt2 ever be null here?
      if (other.hcnt2[k] > 0) {
        if ( hcnt2[k]==0 || ( other.hcnt2_min[k] < hcnt2_min[k] )) {
          hcnt2_min[k] = other.hcnt2_min[k];
        }
      }
    }

    // merge hcnt2 per-bin maxs
    // other must be same length, but use it's length for safety
    for (int k = 0; k < other.hcnt2_max.length; k++) {
      // for now..die on NaNs
      assert !Double.isNaN(other.hcnt2_max[k]) : "NaN in other.hcnt2_max merging";
      assert !Double.isNaN(other.hcnt2[k]) : "NaN in hcnt2_min merging";
      assert !Double.isNaN(hcnt2_max[k]) : "NaN in hcnt2_max merging";
      assert !Double.isNaN(hcnt2[k]) : "NaN in hcnt2_max merging";

      // cover the initial case (relying on initial min = 0 to work is wrong)
      // Only take the new max if it's hcnt2 is non-zero. like a valid bit
      // can hcnt2 ever be null here?
      if (other.hcnt2[k] > 0) {
        if ( hcnt2[k]==0 || ( other.hcnt2_max[k] > hcnt2_max[k] )) {
          hcnt2_max[k] = other.hcnt2_max[k];
        }
      }
    }

    // can hcnt2 ever be null here?. Inc last, so the zero case is detected above
    // seems like everything would fail if hcnt2 doesn't exist here
    if (hcnt2 != null)
      Utils.add(hcnt2, other.hcnt2);

    // merge hcnt mins
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
    // merge hcnt maxs, also deduplicating against mins?
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

  // _start of each hcnt bin
  public double binValue(int b) { return _start + b*_binsz; }
  // can we assert against something here?
  // assert _gprows==htot2(0, 0) : "_gprows: "+_gprows+" htot2(): "+htot2(0, 0);

  // need to count >4B rows
  private long htot2(long low, long high) {
    long cnt = 0;
    for (int i = 0; i < hcnt2.length; i++) cnt+=hcnt2[i];
    // add the stuff outside the bins, 0,0 for single pass
    cnt = cnt + low + high;
    return cnt;
  }


  //******************************************************************************
  // NOTE: only works on a backfilled hcnt2, unlike Quantiles. eliminates nextK search
  // The backfill is not done here, so it's only done once (because 10 calls here)
  private double approxLikeInQuantiles(double threshold, double valStart, double valEnd) {
    // Code is lifted from Quantiles.java, with only a little jiggering
    // on the branches around forceBestApprox/interpolation type, and use of globals
    // that have different names. Need to merge sometime.
    // the 'intent' is to be the same as the single pass Quantiles approx, interpolation_type==-1

    // max_qbins was the goal for sizing.
    // nbins2 was what was used for size, after various calcs
    // just assume hcnt2 is the right length!
    // Don't need at least two bins..since we'll always have 'some' answer
    // are we being called on constant 0?
    int maxBinCnt = hcnt2.length;

    // Find the row count we want to hit, within some bin.
    long currentCnt = 0;
    double targetCntFull = threshold * (_gprows-1);  //  zero based indexing
    long targetCntInt = (long) Math.floor(targetCntFull);
    double targetCntFract = targetCntFull  - (double) targetCntInt;
    assert (targetCntFract>=0) && (targetCntFract<=1);
    // Log.debug("QS_ targetCntInt: "+targetCntInt+" targetCntFract: "+targetCntFract);

    // walk thru and find out what bin to look inside
    int k = 0;
    while(k!=maxBinCnt && ((currentCnt + hcnt2[k]) <= targetCntInt)) {
      // Log.debug("Q_ Looping for k: "+threshold+" "+k+" "+maxBinCnt+" "+currentCnt+" "+targetCntInt+
      //   " "+hcnt2[k]+" "+hcnt2_min[k]+" "+hcnt2_max[k]);
      currentCnt += hcnt2[k];
      ++k;
      // Note the loop condition covers the breakout condition:
      // (currentCnt==targetCntInt && (hcnt2[k]!=0)
      // also: don't go pass array bounds
    }


    assert hcnt2[k]!=0;
    // Log.debug("QS_ Found k (approx): "+threshold+" "+k+" "+currentCnt+" "+targetCntInt+
    //  " "+_gprows+" "+hcnt2[k]+" "+hcnt2_min[k]+" "+hcnt2_max[k]);

    assert (currentCnt + hcnt2[k]) > targetCntInt : targetCntInt+" "+currentCnt+" "+k+" "+" "+maxBinCnt;
    assert hcnt2[k]!=1 || hcnt2_min[k]==hcnt2_max[k];

    boolean done = false;
    double guess = Double.NaN;
    boolean interpolated = false;
    double dDiff;

    // special cases. If the desired row is the last of equal values in this bin (2 or more)
    // we will need to intepolate with a nextK out-of-bin value
    // we can't iterate, since it won't improve things and the bin-size will be zero!
    // trying to resolve case of binsize=0 for next pass, after this, is flawed thinking.
    // implies the values are not the same..end of bin interpolate to next
    boolean atStartOfBin = hcnt2[k]>=1 && (currentCnt == targetCntInt);
    boolean atEndOfBin = !atStartOfBin && (hcnt2[k]>=2 && ((currentCnt + hcnt2[k] - 1) == targetCntInt));
    boolean inMidOfBin = !atStartOfBin && !atEndOfBin && (hcnt2[k]>=3) && (hcnt2_min[k]==hcnt2_max[k]);

    boolean interpolateEndNeeded = false;
    if ( atEndOfBin ) {
      if ( targetCntFract != 0 ) {
        interpolateEndNeeded = true;
      }
      else {
        guess = hcnt2_max[k];
        done = true;
        // Log.debug("QS_ Guess M "+guess);
      }
    }
    else if ( inMidOfBin ) {
      // if we know there is something before and after us with same value,
      // we never need to interpolate (only allowed when min=max
      guess = hcnt2_min[k];
      done = true;
      // Log.debug("QS_ Guess N "+guess);
    }

    if ( !done && atStartOfBin ) {
      // no interpolation needed
      if ( hcnt2[k]>2 && (hcnt2_min[k]==hcnt2_max[k]) ) {
        guess = hcnt2_min[k];
        done = true;
        // Log.debug("QS_ Guess A "+guess);
      }
      // min/max can be equal or not equal here
      else if ( hcnt2[k]==2 ) { // interpolate between min/max for the two value bin
        // type 7 (linear interpolation)
        // Unlike mean, which just depends on two adjacent values, this adjustment
        // adds possible errors related to the arithmetic on the total # of rows.
        dDiff = hcnt2_max[k] - hcnt2_min[k]; // two adjacent..as if sorted!
        // targetCntFract is fraction of total rows
        guess = hcnt2_min[k] + (targetCntFract * dDiff);

        done = true;
        interpolated = true;
        // Log.debug("QS_ Guess B "+guess+" targetCntFract: "+targetCntFract);
      }
      // no interpolation needed
      else if ( (hcnt2[k]==1) && (targetCntFract==0) ) {
        assert hcnt2_min[k]==hcnt2_max[k];
        guess = hcnt2_min[k];
        done = true;
        // Log.debug("QS_ Guess C "+guess);
      }
    }

    // interpolate into a nextK value
    // all the qualification is so we don't set done when we're not, for multipass
    // interpolate from single bin, end of two entry bin, or for approx
    boolean stillCanGetIt = atStartOfBin && hcnt2[k]==1 && targetCntFract!=0;
    if ( !done ) {
      if ( hcnt2[k]==1 ) {
        assert hcnt2_min[k]==hcnt2_max[k];
        // Log.debug("QS_ Single value in this bin, but fractional means we need to interpolate to next non-zero");
      }
      if ( interpolateEndNeeded ) {
        // Log.debug("QS_ Interpolating off the end of a bin!");
      }

      double nextVal;
      int nextK;
      // if we're at the end
      assert k < maxBinCnt : k+" "+maxBinCnt;
      if ( (k+1)==maxBinCnt) {
        // Log.debug("QS_ Using valEnd for approx interpolate: "+valEnd);
        nextVal = valEnd; // just in case the binning didn't max in a bin before the last
      }
      else {
        nextK = k + 1;
        nextVal = hcnt2_min[nextK];
        // Log.debug("QS_ Using nextK for interpolate: "+nextK+" "+hcnt2_min[nextK]);
        // hcnt2[nextK] may be zero here if we backfilled
      }

      // can still get an exact interpolation, when hcnt2[k]=2
      if ( stillCanGetIt ) {
        dDiff = nextVal - hcnt2_max[k]; // two adjacent, as if sorted!
        // targetCntFract is fraction of total rows
        guess = hcnt2_max[k] + (targetCntFract * dDiff);
        interpolated = true;
        done = true; //  has to be one above us when needed. (or we're at end)
        // Log.debug("QS_ Guess D "+guess+" "+nextVal+" "+hcnt2_min[k]+" "+hcnt2_max[k]+" "+hcnt2[k]+" "+nextVal+
        //   " targetCntFull: "+targetCntFull+" targetCntFract: "+targetCntFract+
        //   " _gprows: " + _gprows+" "+stillCanGetIt);

      }
      else { // single pass approx..with unresolved bin
        assert hcnt2[k]!=0 : hcnt2[k]+" "+k;
        // use max within this bin, to stay within the guaranteed error bounds
        dDiff = (hcnt2_max[k] - hcnt2_min[k]) / hcnt2[k];
        guess = hcnt2_min[k] + (targetCntFull-currentCnt) * dDiff;
        interpolated = true;
        done = true; //  has to be one above us when needed. (or we're at end)
        // Log.debug("QS_ Guess E "+guess+" "+nextVal+" "+hcnt2_min[k]+" "+hcnt2_max[k]+" "+hcnt2[k]+" "+nextVal+
        //   " targetCntFull: "+targetCntFull+" targetCntFract: "+targetCntFract+
        //   " _gprows: " + _gprows);
      }
    }
    assert !Double.isNaN(guess); // covers positive/negative inf also (if we divide by 0)
    return guess;
  }
  //******************************************************************************
  private void approxQuantiles(double[] qtiles, double[] thres, double valStart, double valEnd){
    // not called for enums
    assert _type != T_ENUM;

    // hcnt2 may have been sized differently than the max_qbins goal
    int maxBinCnt = hcnt2.length;
    if ( maxBinCnt==0 ) return;
    // this would imply we didn't get anything correctly. Maybe real col with all NA?
    if ( (maxBinCnt==1) && (hcnt2[0]==0) )  return;

    // Perf hack that is currently different than Quantiles.java
    // back fill hcnt2_min where it's zero, so we can avoid the nextK search
    // when we need to interpolate. Keep hcnt2[k]=0 so we know not to use it
    // other than for getting nextK without searching. This is powerful
    // because if we're getting 10 quantiles from a histogram, we don't
    // do searches to the end (potentially) for ever nextK find. This
    // makes the Quantiles.java algo work well when reused for multiple quantiles
    // here in Summary2

    // The use of nextK, rather than just our bin, improves accuracy for various cases.
    // (mirroring what Quantiles does for perfect answers)

    // start at the end. don't need to fill the 0 case ever, but should for consistency
    double backfill = valEnd;
    for (int b=(maxBinCnt-1); b>=0; --b) {
      if ( hcnt2[b] == 0 ) {
        hcnt2_min[b] = backfill;
        // Log.debug("QS_ backfilling "+b+" "+backfill);
      }
      else {
        backfill = hcnt2_min[b];
      }
    }

    for(int j = 0; j < thres.length; ++j) {
      // 0 okay for threshold?
      assert 0 <= thres[j] && thres[j] <= 1;
      qtiles[j] = approxLikeInQuantiles(thres[j], valStart, valEnd);
    }
  }

  //******************************************************************************
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
    // should be a better way/place to decode this back to string.
    String typeStr;
    if ( _type == T_REAL) typeStr = "Real";
    else if ( _type == T_INT) typeStr = "Int";
    else if ( _type == T_ENUM) typeStr = "Enum";
    else typeStr = "Undefined";

    sb.append("<div class='table' id='col_" + cname + "' style='width:90%;heigth:90%;border-top-style:solid;'>" +
    "<div class='alert-success'><h4>Column: " + cname + " (type: " + typeStr + ")</h4></div>\n");
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
      sb.append("<tr>");
      sb.append("<th>min[" + stats.mins.length + "]</th>");
      for( double min : stats.mins ) {
        sb.append("<td>" + Utils.p2d(min) + "</td>");
      }
      sb.append("<tr>");
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
        sb.append("<td>" + Utils.p2d(pc * 100.0) + "</td>");
        // sb.append("<td>" + (int) Math.round(pc * 100) + "</td>");
      sb.append("</tr>");
      sb.append("<tr><th>Value</th>");
      for (double pv : stats.pctile)
        sb.append("<td>" + pv + "</td>");
      sb.append("</tr>");
      sb.append("</table>");
      sb.append("</div>");
    }
    sb.append("</div>\n");
  }
}
