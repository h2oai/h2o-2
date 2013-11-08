package hex;

import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Vec;
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
  // INPUTS
  final transient Vec      _vec;
  final transient double   _min;
  final transient double   _max;
  final transient double[] _mins;
  final transient double[] _maxs;
        transient long     _zeros;
  final transient String[] _domains;
  final transient double   _start;
  final transient double   _binsz;
        transient double[] _percentiles;

  static abstract class Stats extends Iced {}
  // An internal JSON-output-only class
  @SuppressWarnings("unused")
  static class EnumStats extends Stats {
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    public EnumStats( Vec vec ) {
      this.cardinality = vec.domain().length;
    }
    @API(help="cardinality"  ) public final int     cardinality;
   }

  static class NumStats extends Stats {
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    public NumStats( Vec vec, long zeros, double[] mins, double[] maxs, double[] percentiles) {
      this.mean = vec.mean();
      this.sd   = vec.sigma();
      this.zeros = zeros;
      this.mins  = mins;
      this.maxs  = maxs;
      this.percentiles = percentiles;
      this.percentile_thresholds = DEFAULT_PERCENTILES;
    }
    @API(help="mean"        ) public final double   mean;
    @API(help="sd"          ) public final double   sd;
    @API(help="#zeros"      ) public final long     zeros;
    @API(help="min elements") public final double[] mins; // min N elements
    @API(help="max elements") public final double[] maxs; // max N elements
    @API(help="percentile thresholds" ) public final double[] percentile_thresholds;
    @API(help="percentiles" ) public final double[] percentiles;
  }
  // OUTPUTS
  // Basic info
  @API(help="name"        ) public String    colname;
  @API(help="type"        ) public String    type;
  // Basic stats
  @API(help="#rows"       ) public long      nrow;
  @API(help="NAs"         ) public long      naCnt;
  @API(help="Base Stats"  ) public Stats     stats;

  @API(help="bin headers" ) public String[]  bin_hdrs;
  @API(help="bin counts"  ) public long[]    bin_cnts;

  public void finishUp() {
    this.type = _vec.isEnum()?"Enum":_vec.isInt()?"Int":"Real";
    this.naCnt = _vec.naCnt();
    computePercentiles();
    computeMajorities();
    for (int i = 0; i < _maxs.length>>>1; i++) {
      double t = _maxs[i]; _maxs[i] = _maxs[_maxs.length-1-i]; _maxs[_maxs.length-1-i] = t;
    }
    this.stats = _vec.isEnum()?new EnumStats(_vec):new NumStats(_vec,_zeros,_mins,_maxs,_percentiles);
    if (_vec.isEnum()) bin_hdrs = _domains;
    else {
      bin_hdrs = new String[bin_cnts.length];
      for (int i = 0; i < bin_hdrs.length; i++)
        bin_hdrs[i] = Utils.p2d(binValue(i));
    }
  }

  public Summary2(Vec vec, String name) {
    this.colname = name;
    this._vec    = vec;
    this._domains = _vec.isEnum() ? vec.domain() : null;
    this.nrow = vec.length() - vec.naCnt();
    if ( !_vec.isEnum() ) {
      this._mins = MemoryManager.malloc8d((int)Math.min(vec.length(),NMAX));
      this._maxs = MemoryManager.malloc8d((int)Math.min(vec.length(),NMAX));
      Arrays.fill(_mins, Double.POSITIVE_INFINITY);
      Arrays.fill(_maxs, Double.NEGATIVE_INFINITY);
    } else {
      _mins = MemoryManager.malloc8d(Math.min(_domains.length,NMAX));
      _maxs = MemoryManager.malloc8d(Math.min(_domains.length,NMAX));
    }

    _min = vec.min();_max = vec.max();
    double span = vec.max()-vec.min() + 1;
    if( vec.isEnum() && span < MAX_HIST_SZ ) {
      _start = vec.min();
      _binsz = 1;
      bin_cnts = new long[(int)span];
    } else {
      // guard against improper parse (date type) or zero c._sigma
      double sigma = vec.sigma();
      if (Double.isNaN(sigma)) sigma = 0; 
      double b = Math.max(1e-4,3.5 * sigma/ Math.cbrt(nrow));
      double d = Math.pow(10, Math.floor(Math.log10(b)));
      if (b > 20*d/3)
        d *= 10;
      else if (b > 5*d/3)
        d *= 5;

      // tweak for integers
      if (d < 1. && _vec.isInt()) d = 1.;
      _binsz = d;
      _start = _binsz * Math.floor(vec.min()/_binsz);
      int nbin = (int)Math.floor((vec.max() + (_vec.isInt()?.5:0) - _start)/_binsz) + 1;
      assert nbin > 0;
      bin_cnts = new long[nbin];
    }
  }

  public void add(Chunk chk) {
    for (int i = 0; i < chk._len; i++) {
      if( chk.isNA0(i) ) continue;
      double val = chk.at0(i);
      assert val >= _min : "ERROR: ON COLUMN " + colname + "   VALUE " + val + " < VEC.MIN " + _min;
      assert val <= _max : "ERROR: ON COLUMN " + colname + "   VALUE " + val + " > VEC.MAX " + _max;
      if ( !_vec.isEnum() ) {
        if (val == 0.) _zeros++;
        // update min/max
        if (val < _mins[_mins.length-1]) {
          int index = Arrays.binarySearch(_mins, val);
          if (index < 0) {
            index = -(index + 1);
            for (int j = _mins.length-1; j > index; j--) _mins[j] = _mins[j-1];
            _mins[index] = val;
          }
        }
        if (val > _maxs[0]) {
          int index = Arrays.binarySearch(_maxs, val);
          if (index < 0) {
            index = -(index + 1);
            for (int j = 0; j < index-1; j++) _maxs[j] = _maxs[j+1];
            _maxs[index-1] = val;
          }
        }
      }
      // update histogram
      long binIdx = Math.round((val-_start)*1000000.0/_binsz)/1000000;
      ++bin_cnts[(int)binIdx];
    }
  }

  public Summary2 add(Summary2 other) {
    _zeros += other._zeros;
    Utils.add(bin_cnts, other.bin_cnts);
    if (_vec.isEnum()) return this;

    double[] ds = MemoryManager.malloc8d(_mins.length);
    int i = 0, j = 0;
    for (int k = 0; k < ds.length; k++)
      ds[k] = _mins[i] < other._mins[j] ? _mins[i++] : other._mins[j++];
    System.arraycopy(ds,0,_mins,0,ds.length);

    i = j = _maxs.length - 1;
    for (int k = ds.length - 1; k >= 0; k--)
      ds[k] = _maxs[i] > other._maxs[j] ? _maxs[i--] : other._maxs[j--];
    System.arraycopy(ds,0,_maxs,0,ds.length);
    return this;
  }

  // _start of each bin
  public double binValue(int b) { return _start + b*_binsz; }

  private void computePercentiles(){
    _percentiles = new double [DEFAULT_PERCENTILES.length];
    if( bin_cnts.length == 0 ) return;
    int k = 0;
    long s = 0;
    for(int j = 0; j < DEFAULT_PERCENTILES.length; ++j) {
      final double s1 = DEFAULT_PERCENTILES[j]*nrow;
      long bc = 0;
      while(s1 > s+(bc = bin_cnts[k])){
        s  += bc;
        k++;
      }
      _percentiles[j] = _mins[0] + k*_binsz + ((_binsz > 1)?0.5*_binsz:0);
    }
  }
  
  // Compute majority categories for enums only
  public void computeMajorities() {
    if (!_vec.isEnum()) return;
    for (int i = 0; i < _mins.length; i++) _mins[i] = i;
    for (int i = 0; i < _maxs.length; i++) _maxs[i] = i;
    int mini = 0, maxi = 0;
    for( int i = 0; i < bin_cnts.length; i++ ) {
      if (bin_cnts[i] < bin_cnts[(int)_mins[mini]]) {
        _mins[mini] = i;
        for (int j = 0; j < _mins.length; j++)
          if (bin_cnts[(int)_mins[j]] > bin_cnts[(int)_mins[mini]]) mini = j;
      }
      if (bin_cnts[i] > bin_cnts[(int)_maxs[maxi]]) {
        _maxs[maxi] = i;
        for (int j = 0; j < _maxs.length; j++)
          if (bin_cnts[(int)_maxs[j]] < bin_cnts[(int)_maxs[maxi]]) maxi = j;
      }
    }
    for (int i = 0; i < _mins.length - 1; i++)
      for (int j = 0; j < i; j++)
        if (bin_cnts[(int)_mins[j]] > bin_cnts[(int)_mins[j+1]]) {
          double t = _mins[j]; _mins[j] = _mins[j+1]; _mins[j+1] = t;
        }
    for (int i = 0; i < _maxs.length - 1; i++)
      for (int j = 0; j < i; j++)
        if (bin_cnts[(int)_maxs[j]] < bin_cnts[(int)_maxs[j+1]]) {
          double t = _maxs[j]; _maxs[j] = _maxs[j+1]; _maxs[j+1] = t;
        }
  }

  public double percentileValue(int idx) {
    if( _vec.isEnum() ) return Double.NaN;
     return _percentiles[idx];
  }

  public void toHTML( Vec vec, String cname, StringBuilder sb ) {
    sb.append("<div class='table' id='col_" + cname + "' style='width:90%;heigth:90%;border-top-style:solid;'>" +
    "<div class='alert-success'><h4>Column: " + cname + " (type: " + type + ")</h4></div>\n");
    if ( nrow == 0 ) {
      sb.append("<div class='alert'>Empty column, no summary!</div></div>\n");
      return;
    }
    // Base stats
    if( !vec.isEnum() ) {
      NumStats stats = (NumStats)this.stats;
      sb.append("<div style='width:100%;'><table class='table-bordered'>");
      sb.append("<tr><th colspan='"+20+"' style='text-align:center;'>Base Stats</th></tr>");
      sb.append("<tr>");
      sb.append("<th>mean</th><td>" + Utils.p2d(stats.mean)+"</td>");
      sb.append("<th>sd</th><td>" + Utils.p2d(stats.sd) + "</td>");
      sb.append("<th>NAs</th>  <td>" + naCnt + "</td>");
      sb.append("<th>zeros</th><td>" + stats.zeros + "</td>");
      sb.append("<th>min[" + stats.mins.length + "]</th>");
      for( double min : stats.mins ) {
        if (min == Double.POSITIVE_INFINITY) break;
        sb.append("<td>" + Utils.p2d(min) + "</td>");
      }
      sb.append("<th>max[" + stats.maxs.length + "]</th>");
      for( double max : stats.maxs ) {
        if (max == Double.NEGATIVE_INFINITY) continue;
        sb.append("<td>" + Utils.p2d(max) + "</td>");
      }
      // End of base stats
      sb.append("</tr> </table>");
      sb.append("</div>");
    } else {                    // Enums
      sb.append("<div style='width:100%'><table class='table-bordered'>");
      sb.append("<tr><th colspan='" + 4 + "' style='text-align:center;'>Base Stats</th></tr>");
      sb.append("<tr><th>NAs</th>  <td>" + naCnt + "</td>");
      sb.append("<th>cardinality</th>  <td>" + vec.domain().length + "</td></tr>");
      sb.append("</table></div>");
    }
    // Histogram
    final int MAX_HISTO_BINS_DISPLAYED = 1000;
    int len = Math.min(bin_cnts.length,MAX_HISTO_BINS_DISPLAYED);
    sb.append("<div style='width:100%;overflow-x:auto;'><table class='table-bordered'>");
    sb.append("<tr> <th colspan="+len+" style='text-align:center'>Histogram</th></tr>");
    sb.append("<tr>");
    if (_vec.isEnum())
       for( int i=0; i<len; i++ ) sb.append("<th>" + vec.domain(i) + "</th>");
    else
       for( int i=0; i<len; i++ ) sb.append("<th>" + Utils.p2d(binValue(i)) + "</th>");
    sb.append("</tr>");
    sb.append("<tr>");
    for( int i=0; i<len; i++ ) sb.append("<td>" + bin_cnts[i] + "</td>");
    sb.append("</tr>");
    sb.append("<tr>");
    for( int i=0; i<len; i++ )
      sb.append(String.format("<td>%.1f%%</td>",(100.0*bin_cnts[i]/nrow)));
    sb.append("</tr>");
    if( bin_cnts.length >= MAX_HISTO_BINS_DISPLAYED )
      sb.append("<div class='alert'>Histogram for this column was too big and was truncated to 1000 values!</div>");
    sb.append("</table></div>");

    if (!vec.isEnum()) {
      NumStats stats = (NumStats)this.stats;
      // Percentiles
      sb.append("<div style='width:100%;overflow-x:auto;'><table class='table-bordered'>");
      sb.append("<tr> <th colspan='" + stats.percentile_thresholds.length + "' " +
              "style='text-align:center' " +
              ">Percentiles</th></tr>");
      sb.append("<tr><th>Threshold(%)</th>");
      for (double pc : stats.percentile_thresholds)
        sb.append("<td>" + (int) Math.round(pc * 100) + "</td>");
      sb.append("</tr>");
      sb.append("<tr><th>Value</th>");
      for (double pv : stats.percentiles)
        sb.append("<td>" + Utils.p2d(pv) + "</td>");
      sb.append("</tr>");
      sb.append("</table>");
      sb.append("</div>");
    }
    sb.append("</div>\n"); 
  }
}
