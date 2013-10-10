package hex;

import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Vec;
import water.util.Utils;

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
  final transient boolean _enum;
  final transient boolean _isInt;
  final transient long _avail_rows;

  // OUTPUTS
  @API(help="bins start"  ) final double _start;
  @API(help="bin step"    ) final double _binsz;
  @API(help="histogram"   ) final long [] _bins; // bins for histogram
  @API(help="#zeros"      ) long _zeros;
  @API(help="min elements") double [] _min; // min N elements
  @API(help="max elements") double [] _max; // max N elements
  @API(help="percentiles" ) double [] _percentileValues;

  public Summary2(Vec vec) {
    _enum = vec.isEnum();
    _isInt = vec.isInt();
    long len = vec.length();
    _avail_rows = len - vec.naCnt();
    _min = MemoryManager.malloc8d((int)Math.min(len,NMAX));
    _max = MemoryManager.malloc8d((int)Math.min(len,NMAX));
    Arrays.fill(_min, Double.POSITIVE_INFINITY);
    Arrays.fill(_max, Double.NEGATIVE_INFINITY);

    double span = vec.max()-vec.min();
    if( vec.isEnum() && (span+1) < MAX_HIST_SZ ) {
      _start = vec.min();
      _binsz = 1;
      _bins = new long[(int)span+1];
    } else {
      // guard against improper parse (date type) or zero c._sigma
      double b = Math.max(1e-4,3.5 * vec.sigma()/ Math.cbrt(len));
      double d = Math.pow(10, Math.floor(Math.log10(b)));
      if (b > 20*d/3)
        d *= 10;
      else if (b > 5*d/3)
        d *= 5;

      // tweak for integers
      if (d < 1. && _isInt) d = 1.;
      _binsz = d;
      _start = _binsz * Math.floor(vec.min()/_binsz);
      int nbin = (int)Math.floor((vec.max() + (_isInt?.5:0) - _start)/_binsz) + 1;
      _bins = new long[nbin];
    }
  }

  public void add(Chunk chk) {
    int maxmin = 0;
    int minmax = 0;
    for (int i = 0; i < chk._len; i++) {
      if( chk.isNA0(i) ) continue;
      double val = chk.at0(i);
      if (val == 0.) _zeros++;
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

      int binIdx = _isInt ? (int)(((long)val - (long)_start)/(long)_binsz)
              : (int)Math.floor((val - _start)/_binsz);
      ++_bins[binIdx];
    }

    /* sort min and max */
    Arrays.sort(_min);
    Arrays.sort(_max);
  }

  public Summary2 add(Summary2 other) {
    _zeros += other._zeros;
    Utils.add(_bins, other._bins);

    double[] ds = MemoryManager.malloc8d(_min.length);
    int i = 0, j = 0;
    for (int k = 0; k < ds.length; k++)
      ds[k] = _min[i] < other._min[j] ? _min[i++] : other._min[j++];
    System.arraycopy(ds,0,_min,0,ds.length);

    i = j = _max.length - 1;
    for (int k = ds.length - 1; k >= 0; k--)
      ds[k] = _max[i] > other._max[j] ? _max[i--] : other._max[j--];
    System.arraycopy(ds,0,_max,0,ds.length);
    return this;
  }

  // Start of each bin
  public double binValue(int b) { return _start + b*_binsz; }

  private void computePercentiles(){
    _percentileValues = new double [DEFAULT_PERCENTILES.length];
    if( _bins.length == 0 ) return;
    int k = 0;
    long s = 0;
    for(int j = 0; j < DEFAULT_PERCENTILES.length; ++j) {
      final double s1 = DEFAULT_PERCENTILES[j]*_avail_rows;
      long bc = 0;
      while(s1 > s+(bc = _bins[k])){
        s  += bc;
        k++;
      }
      _percentileValues[j] = _min[0] + k*_binsz + ((_binsz > 1)?0.5*_binsz:0);
    }
  }

  public double percentileValue(int idx) {
    if( _enum ) return Double.NaN;
    if(_percentileValues == null) computePercentiles();
    return _percentileValues[idx];
  }

  @Override public String toString(){
    StringBuilder res = new StringBuilder("ColumnSummary[" + _start + ":" + binValue(_bins.length) +", binsz=" + _binsz+"]");
    if( !_enum )
      for( int i=0; i<DEFAULT_PERCENTILES.length; i++ )
        res.append(", p("+(int)(100*DEFAULT_PERCENTILES[i])+"%)=" + percentileValue(i));
    return res.toString();
  }

  public void toHTML( Vec vec, String cname, StringBuilder sb ) {

    sb.append("<div class='table' id='col_" + cname + "' style='width:90%;heigth:90%;border-top-style:solid;'>" +
            "<div class='alert-success'><h4>Column: " + cname + "</h4></div>\n");

    // Base stats
    if( !vec.isEnum() ) {
      sb.append("<div style='width:100%;'><table class='table-bordered'>");
      sb.append("<tr><th colspan='"+20+"' style='text-align:center;'>Base Stats</th></tr>");
      sb.append("<tr>");
      sb.append("<th>avg</th><td>" + Utils.p2d(vec.mean())+"</td>");
      sb.append("<th>sd</th><td>" + Utils.p2d(vec.sigma()) + "</td>");
      sb.append("<th>NAs</th>  <td>" + vec.naCnt() + "</td>");
      sb.append("<th>zeros</th><td>" + _zeros + "</td>");
      sb.append("<th>min[" + _min.length + "]</th>");
      for( double min : _min ) {
        if (min == Double.POSITIVE_INFINITY) break;
        sb.append("<td>" + Utils.p2d(min) + "</td>");
      }
      sb.append("<th>max[" + _max.length + "]</th>");
      for( double max : _max ) {
        if (max == Double.NEGATIVE_INFINITY) continue;
        sb.append("<td>" + Utils.p2d(max) + "</td>");
      }
      // End of base stats
      sb.append("</tr> </table>");
      sb.append("</div>");


    } else {                    // Enums
      sb.append("<div style='width:100%'><table class='table-bordered'>");
      sb.append("<tr><th colspan='" + 4 + "' style='text-align:center;'>Base Stats</th></tr>");
      sb.append("<tr><th>NAs</th>  <td>" + vec.naCnt() + "</td>");
      sb.append("<th>cardinality</th>  <td>" + vec.domain().length + "</td></tr>");
      sb.append("</table></div>");
    }

    // Histogram
    final int MAX_HISTO_BINS_DISPLAYED = 1000;
    int len = Math.min(_bins.length,MAX_HISTO_BINS_DISPLAYED);
    sb.append("<div style='width:100%;overflow-x:auto;'><table class='table-bordered'>");
    sb.append("<tr> <th colspan="+len+" style='text-align:center'>Histogram</th></tr>");
    sb.append("<tr>");
    if (_enum)
       for( int i=0; i<len; i++ ) sb.append("<th>" + vec.domain(i) + "</th>");
    else
       for( int i=0; i<len; i++ ) sb.append("<th>" + Utils.p2d(binValue(i)) + "</th>");
    sb.append("</tr>");
    sb.append("<tr>");
    for( int i=0; i<len; i++ ) sb.append("<td>" + _bins[i] + "</td>");
    sb.append("</tr>");
    sb.append("<tr>");
    for( int i=0; i<len; i++ )
      sb.append(String.format("<td>%.1f%%</td>",(100.0*_bins[i]/_avail_rows)));
    sb.append("</tr>");
    if( _bins.length >= MAX_HISTO_BINS_DISPLAYED )
      sb.append("<div class='alert'>Histogram for this column was too big and was truncated to 1000 values!</div>");
    sb.append("</table></div>");

    if (!vec.isEnum()) {
      // Percentiles
      sb.append("<div style='width:100%;overflow-x:auto;'><table class='table-bordered'>");
      sb.append("<tr> <th colspan='" + DEFAULT_PERCENTILES.length + "' " +
              "style='text-align:center' " +
              ">Percentiles</th></tr>");
      sb.append("<tr><th>Threshold(%)</th>");
      for (double pc : DEFAULT_PERCENTILES)
        sb.append("<td>" + (int) Math.round(pc * 100) + "</td>");
      sb.append("</tr>");
      sb.append("<tr><th>Value</th>");
      for (double pv : _percentileValues)
        sb.append("<td>" + Utils.p2d(pv) + "</td>");
      sb.append("</tr>");
      sb.append("</table>");
      sb.append("</div>");
    }
    sb.append("\n</div>\n");
  }
}
