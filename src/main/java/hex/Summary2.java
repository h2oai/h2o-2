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

/**
 * Summary of a column.
 */
public class Summary2 extends Iced {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Returns a summary of a fluid-vec frame";

  public static final int MAX_HIST_SZ = water.parser.Enum.MAX_ENUM_SIZE;
  public static final double [] DEFAULT_PERCENTILES = {0.01,0.05,0.10,0.25,0.33,0.50,0.66,0.75,0.90,0.95,0.99};
  public static final int NMAX = 5;
  private static final int T_REAL = 0;
  private static final int T_INT  = 1;
  private static final int T_ENUM = 2;
  
  // INPUTS
  final           long     _nrow;
  final           int      _type; // 0 - real; 1 - int; 2 - enum
                  double[] _mins;
                  double[] _maxs;
                  long     _zeros;
                  long     _rows;
  final transient double   _min;
  final transient double   _max;
  final transient String[] _domain;
  final transient double   _start;
  final transient double   _binsz;
        transient double[] _pctile;

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

  public static class BasicSummaryTask extends MRTask2<BasicSummaryTask> {
    public double _finite_min[];
    public double _finite_max[];
    @Override public void map(Chunk[] cs) {
      _finite_min = new double[cs.length];
      _finite_max = new double[cs.length];
      for (int c = 0; c < cs.length; c++) {
        _finite_min[c] = Double.NaN;
        _finite_max[c] = Double.NaN;
        for (int i = 0; i < cs[c]._len; i++) {
          if (!cs[c].isNA0(i)) {
            double v = cs[c].at0(i);
            if (!Double.isInfinite(v)) {
              _finite_min[c] = Double.isNaN(_finite_min[c]) ? v : Math.min(v, _finite_min[c]);
              _finite_max[c] = Double.isNaN(_finite_max[c]) ? v : Math.max(v, _finite_max[c]);
            }
          }
        }
      }
    }
    @Override public void reduce(BasicSummaryTask other){
      for (int c = 0; c < _finite_min.length; c++) {
        double min = _finite_min[c];
        double max = _finite_max[c];
        _finite_min[c] = Double.isNaN(min) ? other._finite_min[c] : Math.min(min, other._finite_min[c]);
        _finite_max[c] = Double.isNaN(max) ? other._finite_max[c] : Math.max(max, other._finite_max[c]);
      }
    }
  }
  public static class SummaryTask2 extends MRTask2<SummaryTask2> {
    private double[] _mins, _maxs;
    public Summary2 _summaries[];
    public SummaryTask2 (double[] mins, double[] maxs) { _mins=mins; _maxs = maxs;}
    @Override public void map(Chunk[] cs) {
      _summaries = new Summary2[cs.length];
      for (int i = 0; i < cs.length; i++) {
        (_summaries[i]=new Summary2(_fr.vecs()[i], _fr.names()[i], _mins[i], _maxs[i])).add(cs[i]);
      }
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
      for( int i=0; i<vecs.length; i++ )
        sums[i] = new Summary2(vecs[i],_fr._names[i], vecs[i].min(), vecs[i].max());
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
    computePercentiles();
    computeMajorities();
    // dedup mins and maxs, only happens when all are infinite values
    for (int i = 0; i < _mins.length-1; i++) {
      if (Double.isNaN(_mins[i])) {
        _mins = Arrays.copyOf(_mins, i);
        break;
      }
    }
    for (int i = 0; i < _maxs.length-1; i++) {
      if (Double.isNaN(_maxs[i])) {
        _maxs = Arrays.copyOf(_maxs, i);
        break;
      }
    }
    for (int i = 0; i < _maxs.length>>>1; i++) {
      double t = _maxs[i]; _maxs[i] = _maxs[_maxs.length-1-i]; _maxs[_maxs.length-1-i] = t;
    }
    this.stats = _type==T_ENUM?new EnumStats(vec.domain().length):new NumStats(vec.mean(),vec.sigma(),_zeros,_mins,_maxs,_pctile);
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

  public Summary2(Vec vec, String name, double finite_min, double finite_max) {
    this.colname = name;
    this._type = vec.isEnum()?2:vec.isInt()?1:0;
    this.nacnt = vec.naCnt();
    this._domain = vec.isEnum() ? vec.domain() : null;
    this._nrow = vec.length() - vec.naCnt();
    double sigma = Double.isNaN(vec.sigma()) ? 0 : vec.sigma(); 
    if ( _type != T_ENUM ) {
      this._mins = MemoryManager.malloc8d((int)Math.min(vec.length(),NMAX));
      this._maxs = MemoryManager.malloc8d((int)Math.min(vec.length(),NMAX));
      Arrays.fill(_mins, Double.NaN);
      Arrays.fill(_maxs, Double.NaN);
    } else {
      _mins = MemoryManager.malloc8d(Math.min(_domain.length,NMAX));
      _maxs = MemoryManager.malloc8d(Math.min(_domain.length,NMAX));
    }
    _min = vec.min();_max = vec.max();
    double span = finite_max - finite_min + 1;
    if( vec.isEnum() && span < MAX_HIST_SZ ) {
      _start = finite_min;
      _binsz = 1;
      hcnt = new long[(int)span];
    } else if (!Double.isNaN(finite_min)) {
      // guard against improper parse (date type) or zero c._sigma
      double b = Math.max(1e-4,3.5 * sigma/ Math.cbrt(_nrow));
      double d = Math.pow(10, Math.floor(Math.log10(b)));
      if (b > 20*d/3)
        d *= 10;
      else if (b > 5*d/3)
        d *= 5;

      // tweak for integers
      if (d < 1. && vec.isInt()) d = 1.;
      _binsz = d;
      _start = _binsz * Math.floor(finite_min/_binsz);
      int nbin = (int)(Math.round((finite_max + (vec.isInt()?.5:0) - _start)*1000000.0/_binsz)/1000000L) + 1;
      assert nbin > 0;
      hcnt = new long[nbin];
    } else { // vec does not contain finite numbers
      _start = vec.min();
      _binsz = Double.POSITIVE_INFINITY;
      hcnt = new long[1];
    }
  }

  public void add(Chunk chk) {
    for (int i = 0; i < chk._len; i++)
      add(chk.at0(i));
  }
  public void add(double val) {
    if( Double.isNaN(val) ) return;
    assert val >= _min : "ERROR: ON COLUMN " + colname + "   VALUE " + val + " < VEC.MIN " + _min;
    assert val <= _max : "ERROR: ON COLUMN " + colname + "   VALUE " + val + " > VEC.MAX " + _max;
    if ( _type != T_ENUM ) {
      if (val == 0.) _zeros++;
      int index;
      // update min/max
      if (Double.isNaN(_mins[_mins.length-1])) {
        index = Arrays.binarySearch(_mins, val);
        if (index < 0) {
          index = -(index + 1);
          for (int j = _mins.length -1; j > index; j--)
            _mins[j] = _mins[j-1];
          _mins[index] = val;
        }
      }
      if (val < _mins[_mins.length-1]) {
        index = Arrays.binarySearch(_mins, val);
        if (index < 0) {
          index = -(index + 1);
          for (int j = _mins.length-1; j > index; j--) _mins[j] = _mins[j-1];
          _mins[index] = val;
        }
      }
      if (Double.isNaN(_maxs[_maxs.length-1])) {
        index = Arrays.binarySearch(_maxs, val);
        if (index < 0) {
          index = -(index + 1);
          for (int j = _maxs.length -1; j > index; j--)
            _maxs[j] = _maxs[j-1];
          _maxs[index] = val;
        }
      }
      else if (val > _maxs[0]) {
        index = Arrays.binarySearch(_maxs, val);
        if (index < 0) {
          index = -(index + 1);
          for (int j = 0; j < index-1; j++) _maxs[j] = _maxs[j+1];
          _maxs[index-1] = val;
        }
      }
    }
    // update histogram
    long binIdx = val == Double.NEGATIVE_INFINITY ? 0
            : val == Double.POSITIVE_INFINITY ? hcnt.length-1
            : Math.round((val-_start)*1000000.0/_binsz)/1000000;
    ++hcnt[(int)binIdx];
    ++_rows;
  }

  public Summary2 add(Summary2 other) {
    _zeros += other._zeros;
    if (hcnt != null)
      Utils.add(hcnt, other.hcnt);
    _rows += other._rows;
    if (_type == T_ENUM) return this;

    double[] ds = MemoryManager.malloc8d(_mins.length);
    int i = 0, j = 0;
    for (int k = 0; k < ds.length; k++)
      ds[k] = _mins[i] < other._mins[j] ? _mins[i++] : Double.isNaN(other._mins[j]) ? _mins[i++] : other._mins[j++];
    System.arraycopy(ds,0,_mins,0,ds.length);

    i = j = _maxs.length - 1;
    for (int k = ds.length - 1; k >= 0; k--)
      ds[k] = _maxs[i] > other._maxs[j] ? _maxs[i--] : Double.isNaN(other._maxs[j]) ? _maxs[i--] : other._maxs[j--];
    System.arraycopy(ds,0,_maxs,0,ds.length);

    return this;
  }

  // _start of each bin
  public double binValue(int b) { return _start + b*_binsz; }

  private void computePercentiles(){
    _pctile = new double [DEFAULT_PERCENTILES.length];
    if( hcnt.length == 0 ) return;
    int k = 0;
    long s = 0;
    for(int j = 0; j < DEFAULT_PERCENTILES.length; ++j) {
      final double s1 = DEFAULT_PERCENTILES[j]*_rows;
      long bc = 0;
      while(s1 > s+(bc = hcnt[k])){
        s  += bc;
        k++;
      }
      _pctile[j] = _mins[0] + k*_binsz + ((_binsz > 1)?0.5*_binsz:0);
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
      for (int j = 0; j < i; j++)
        if (hcnt[(int)_mins[j]] > hcnt[(int)_mins[j+1]]) {
          double t = _mins[j]; _mins[j] = _mins[j+1]; _mins[j+1] = t;
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
    if ( _nrow == 0 ) {
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
      sb.append(String.format("<td>%.1f%%</td>",(100.0*hcnt[i]/_nrow)));
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
