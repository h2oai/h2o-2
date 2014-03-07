package hex;

import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.*;
import water.util.Utils;

/**
 * Quantile of a column.
 */
public class Quantiles extends Iced {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Returns a quantile of a fluid-vec frame";

  public static final int    MAX_ENUM_SIZE = water.parser.Enum.MAX_ENUM_SIZE;
  // updated boundaries to be 0.1% 1%...99%, 99.9% so R code didn't have to change
  // ideally we extend the array here, and just update the R extraction of 25/50/75 percentiles
  // note python tests (junit?) may look at result
  public final double QUANTILES_TO_DO[];
  private static final int   T_REAL = 0;
  private static final int   T_INT  = 1;
  private static final int   T_ENUM = 2;
  public BasicStat           _stat0;     /* Basic Vec stats collected by PrePass. */
  public final int           _type;      // 0 - real; 1 - int; 2 - enum
  long                       _gprows;    // non-empty rows per group

  final transient String[]   _domain;
  final transient double     _start2;
  final transient double     _binsz2;    // 2nd finer grained histogram used for quantile estimates for numerics
  public transient double[]  _pctile;


  static abstract class Stats extends Iced {
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields

    @API(help="stats type"   ) public String type;
    Stats(String type) { this.type = type; }
  }
  static class NumStats extends Stats {
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    public NumStats( double mean, double sigma, long zeros, double[] pctile, double[] pct) {
      super("Numeric");
      this.mean  = mean;
      this.sd    = sigma;
      this.zeros = zeros;
      this.pctile = pctile;
      this.pct   = pct;
    }
    @API(help="mean"        ) public final double   mean;
    @API(help="sd"          ) public final double   sd;
    @API(help="#zeros"      ) public final long     zeros;
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

  public long[]  hcnt2; // finer histogram. not visible
  public double[]  hcnt2_min; // min actual for each bin
  public double[]  hcnt2_max; // max actual for each bin

  public static class BasicStat extends Iced {
    public long _len;   /* length of vec */
    /// FIX! can NAs vs NaNs be distinguished? Do we care for quantile?
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
    private double _quantile;
    private int _max_qbins;
    public Quantiles _summaries[];
    public SummaryTask2 (BasicStat[] basicStats, double quantile, int max_qbins) 
      { _basics = basicStats; _quantile = quantile; _max_qbins = max_qbins; }
    @Override public void map(Chunk[] cs) {
      _summaries = new Quantiles[cs.length];
      for (int i = 0; i < cs.length; i++)
        _summaries[i] = new Quantiles(_fr.vecs()[i], _fr.names()[i], 
          _basics[i], _quantile, _max_qbins).add(cs[i]);
    }
    @Override public void reduce(SummaryTask2 other) {
      for (int i = 0; i < _summaries.length; i++)
        _summaries[i].add(other._summaries[i]);
    }
  }

  // FIX! do we need this?
  @Override public String toString() {
    String s = "";
    if( stats instanceof NumStats ) {
      double pct   [] = ((NumStats)stats).pct   ;
      double pctile[] = ((NumStats)stats).pctile;
      for( int i=0; i<pct.length; i++ )
        s += ""+(pct[i]*100)+"%="+pctile[i]+", ";
    } else {
      s = "enums no longer here?";
    }
    return s;
  }

  public void finishUp(Vec vec) {
    nacnt = _stat0._nas;
    // below, we force it to ignore length and only do [0]
    // need to figure out if we need to do a list and how that's returned
    _pctile = new double[QUANTILES_TO_DO.length];

    if (_type == T_ENUM) {
      ;
    } else {
      approxQuantiles1Pass(_pctile,QUANTILES_TO_DO);
      this.stats = 
        new NumStats(vec.mean(), vec.sigma(), _stat0._zeros, _pctile, QUANTILES_TO_DO);
    }
  }

  public Quantiles(Vec vec, String name, BasicStat stat0, double quantile, int max_qbins) {
    colname = name;
    _stat0 = stat0;
    _type = vec.isEnum()?2:vec.isInt()?1:0;
    _domain = vec.isEnum() ? vec.domain() : null;
    _gprows = 0;
    QUANTILES_TO_DO = new double[1];
    QUANTILES_TO_DO[0] = quantile;
    double sigma = Double.isNaN(vec.sigma()) ? 0 : vec.sigma();

    if( vec.isEnum() && _domain.length < MAX_ENUM_SIZE ) {
      _start2 = 0;
      _binsz2 = 1;
      hcnt2 = new long[_domain.length];
      hcnt2_min = new double[_domain.length];
      hcnt2_max = new double[_domain.length];
    } else if (!Double.isNaN(stat0._min2)) {

      // okay if 1 more than max_qbins gets created
      // _binsz2 = _binsz / (max_qbins / nbin);
      assert max_qbins > 0 && max_qbins <= 1000000 : "max_qbins must be >0 and <= 1000000";
      _binsz2 = (stat0._max2 + (vec.isInt()?.5:0) - stat0._min2) / max_qbins;
      _start2 = _binsz2 * Math.floor(stat0._min2/_binsz2);
      int nbin2 = (int)(Math.round((stat0._max2 + (vec.isInt()?.5:0) - _start2)*1000000.0/_binsz2)/1000000L) + 1;

      // Log.info("Finer histogram has "+nbin2+" bins. Visible histogram has "+nbin);
      // Log.info("Finer histogram starts at "+_start2+" Visible histogram starts at "+_start);
      // Log.info("stat0._min2 "+stat0._min2+" stat0._max2 "+stat0._max2);

      // can't make any assertion about _start2 vs _start  (either can be smaller due to fp issues)
      assert nbin2 > 0;

      hcnt2 = new long[nbin2];
      hcnt2_min = new double[nbin2];
      hcnt2_max = new double[nbin2];
    } else { // vec does not contain finite numbers
      _start2 = vec.min();
      _binsz2 = Double.POSITIVE_INFINITY;
      hcnt2 = new long[1];
      hcnt2_min = new double[1];
      hcnt2_max = new double[1];
    }
  }

  public Quantiles(Vec vec, String name, BasicStat stat0) {
    this(vec, name, stat0, 0.0, 1000);
  }

  public Quantiles add(Chunk chk) {
    for (int i = 0; i < chk._len; i++)
      add(chk.at0(i));
    // FIX! should eventually get rid of this since unused?
    return this;
  }
  public void add(double val) {
    if( Double.isNaN(val) ) return;
    _gprows++;
    if ( _type == T_ENUM ) return;

    int index;
    long binIdx2;
    if (hcnt2.length==1) {
      binIdx2 = 0; // not used
    }
    else {
      binIdx2 = Math.round(((val - _start2) * 1000000.0) / _binsz2) / 1000000;
    }

    int binIdx2Int = (int) binIdx2;
    assert (binIdx2Int >= 0 && binIdx2Int < hcnt2.length) : 
      "binIdx2Int too big for hcnt2 "+binIdx2Int+" "+hcnt2.length;

    if (hcnt2[binIdx2Int] == 0) {
      // Log.info("New init: "+val+" for index "+binIdx2Int);
      hcnt2_min[binIdx2Int] = val;
      hcnt2_max[binIdx2Int] = val;
    }
    else {
      if (val < hcnt2_min[binIdx2Int]) {
          // Log.info("New min: "+val+" for index "+binIdx2Int);
          hcnt2_min[binIdx2Int] = val;
      }
      if (val > hcnt2_max[binIdx2Int]) {
          // if ( binIdx2Int == 500 ) Log.info("New max: "+val+" for index "+binIdx2Int);
          hcnt2_max[binIdx2Int] = val;
      }
    }
    ++hcnt2[binIdx2Int];
  }

  public Quantiles add(Quantiles other) {
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
    if (hcnt2 != null) Utils.add(hcnt2, other.hcnt2);
    return this;
  }

  // need to count >4B rows
  private long htot2() { // same but for the finer histogram
    long cnt = 0;
    for (int i = 0; i < hcnt2.length; i++) cnt+=hcnt2[i];
    return cnt;
  }

  private void approxQuantiles1Pass(double[] qtiles, double[] thres){
    // not called for enums
    assert _type != T_ENUM;
    if( hcnt2.length == 0 ) return;

    int k = 0;
    long s = 0;
    double guess = 0;
    double actualBinWidth = 0;
    assert _gprows==htot2() : "_gprows: "+_gprows+" htot2(): "+htot2();

    // A 'perfect' quantile definition, for comparison. 
    // Given a set of N ordered values {v[1], v[2], ...} and a requirement to 
    // calculate the pth percentile, do the following:
    // Calculate l = p(N-1) + 1
    // Split l into integer and decimal components i.e. l = k + d
    // Compute the required value as V = v[k] + d(v[k+1] - v[k])

    // we do zero-indexed list, so slightly different eqns.

    // walk up until we're at the bin that starts with the threshold, or right before
    // only do thres[0]. how do we make a list of thresholds work?
    // for(int j = 0; j < thres.length; ++j) {
     for(int j = 0; j <=0; ++j) {
      // 0 okay for threshold?
      assert 0 <= thres[j] && thres[j] <= 1;
      double s1 = Math.floor(thres[j] * (double) _gprows); 
      if ( s1 == 0 ) {
        s1 = 1; // always need at least one row
      }
      // what if _gprows is 0?. just return above?. Is it NAs?
      // assert _gprows > 0 : _gprows;
      if( _gprows == 0 ) return;
      assert 1 <= s1 && s1 <= _gprows : s1+" "+_gprows;
      // how come first bins can be 0? Fixed. problem was _start. Needed _start2. still can get some
      while( (s+hcnt2[k]) < s1) { // important to be < here. case: 100 rows, getting 50% right.
        s += hcnt2[k];
        k++;
      }
      // Log.info("Found k: "+k+" "+s+" "+s1+" "+_gprows+" "+hcnt2[k]+" "+hcnt2_min[k]+" "+hcnt2_max[k]);

      // All possible bin boundary issues 
      if ( s==s1 || hcnt2[k]==0 ) {
        if ( hcnt2[k]!=0 ) {
          guess = hcnt2_min[k];
          // Log.info("Guess A: "+guess+" "+s+" "+s1);
        }
        else {
          if ( k==0 ) { 
            assert hcnt2[k+1]!=0 : "Unexpected state of starting hcnt2 bins";
            guess = hcnt2_min[k+1];
            // Log.info("Guess B: "+guess+" "+s+" "+s1);
          }
          else {
            if ( hcnt2[k-1]!=0 ) {
              guess = hcnt2_max[k-1];
              // Log.info("Guess C: "+guess+" "+s+" "+s1);
            }
            else {
              assert false : "Unexpected state of adjacent hcnt2 bins";
            }
          }
        }
      }
      else {
        // nonzero hcnt2[k] guarantees these are valid
        actualBinWidth = hcnt2_max[k] - hcnt2_min[k];

        // interpolate within the populated bin, assuming linear distribution
        // since we have the actual min/max within a bin, we can be more accurate
        // compared to using the bin boundaries
        // Note actualBinWidth is 0 when all values are the same in a bin
        // Interesting how we have a gap that we jump between max of one bin, and min of another.
        guess = hcnt2_min[k] + actualBinWidth * ((s1 - s)/ hcnt2[k]);
        // Log.info("Guess D: "+guess+" "+k+" "+hcnt2_min[k]+" "+actualBinWidth+" "+s+" "+s1+" "+hcnt2[k]);
      }

      qtiles[j] = guess;

      // might have fp tolerance issues here? but fp numbers should be exactly same?
      // Log.info(]: hcnt2[k]: "+hcnt2[k]+" hcnt2_min[k]: "+hcnt2_min[k]+
      //  " hcnt2_max[k]: "+hcnt2_max[k]+" _binsz2: "+_binsz2+" guess: "+guess+" k: "+k+"\n");
    }
  }

}
