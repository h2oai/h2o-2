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
  @API(help="categories"  ) final String[] domains;
  @API(help="bins start"  ) final double   start;
  @API(help="bin step"    ) final double   binsz;
  @API(help="histogram"   ) final long []  bins; // bins for histogram
  @API(help="#zeros"      ) long           zeros;
  @API(help="min elements") double []      mins; // min N elements
  @API(help="max elements") double []      maxs; // max N elements
  @API(help="percentiles" ) double []      percentileValues;

  public Summary2(Vec vec) {
    _enum = vec.isEnum();
    _isInt = vec.isInt();
    if (_enum) domains = vec.domain(); else domains = null;
    long len = vec.length();
    _avail_rows = len - vec.naCnt();
    if (_enum) {
      mins = MemoryManager.malloc8d(Math.min(domains.length, NMAX));
      maxs = MemoryManager.malloc8d(Math.min(domains.length, NMAX));
    } else {
      mins = MemoryManager.malloc8d((int)Math.min(len,NMAX));
      maxs = MemoryManager.malloc8d((int)Math.min(len,NMAX));
    }
    Arrays.fill(mins, Double.POSITIVE_INFINITY);
    Arrays.fill(maxs, Double.NEGATIVE_INFINITY);

    double span = vec.max()-vec.min() + 1;
    if( vec.isEnum() && span < MAX_HIST_SZ ) {
      start = vec.min();
      binsz = 1;
      bins = new long[(int)span];
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
      binsz = d;
      start = binsz * Math.floor(vec.min()/binsz);
      int nbin = (int)Math.floor((vec.max() + (_isInt?.5:0) - start)/binsz) + 1;
      bins = new long[nbin];
    }
  }

  public void add(Chunk chk) {
    int maxmin = 0;
    int minmax = 0;
    for (int i = 0; i < chk._len; i++) {
      if( chk.isNA0(i) ) continue;
      double val = chk.at0(i);
      if (val == 0.) zeros++;
      // update min/max
      if (val < mins[maxmin]) {
        mins[maxmin] = val;
        for (int k = 0; k < mins.length; k++)
          if (mins[k] > mins[maxmin])
            maxmin = k;
      }
      if (val > maxs[minmax]) {
        maxs[minmax] = val;
        for (int k = 0; k < maxs.length; k++)
          if (maxs[k] < maxs[minmax])
            minmax = k;
      }
      // update histogram

      int binIdx = _isInt ? (int)(((long)val - (long)start)/(long)binsz)
              : (int)Math.floor((val - start)/binsz);
      if (start + (binIdx + 1) * binsz <= val) ++binIdx; // guard against numeric error
      ++bins[binIdx];
    }

    /* sort min and max */
    Arrays.sort(mins);
    Arrays.sort(maxs);
  }

  public Summary2 add(Summary2 other) {
    zeros += other.zeros;
    Utils.add(bins, other.bins);

    double[] ds = MemoryManager.malloc8d(mins.length);
    int i = 0, j = 0;
    for (int k = 0; k < ds.length; k++)
      ds[k] = mins[i] < other.mins[j] ? mins[i++] : other.mins[j++];
    System.arraycopy(ds,0,mins,0,ds.length);

    i = j = maxs.length - 1;
    for (int k = ds.length - 1; k >= 0; k--)
      ds[k] = maxs[i] > other.maxs[j] ? maxs[i--] : other.maxs[j--];
    System.arraycopy(ds,0,maxs,0,ds.length);
    return this;
  }

  // Start of each bin
  public double binValue(int b) { return start + b*binsz; }

  private void computePercentiles(){
    percentileValues = new double [DEFAULT_PERCENTILES.length];
    if( bins.length == 0 ) return;
    int k = 0;
    long s = 0;
    for(int j = 0; j < DEFAULT_PERCENTILES.length; ++j) {
      final double s1 = DEFAULT_PERCENTILES[j]*_avail_rows;
      long bc = 0;
      while(s1 > s+(bc = bins[k])){
        s  += bc;
        k++;
      }
      percentileValues[j] = mins[0] + k*binsz + ((binsz > 1)?0.5*binsz:0);
    }
  }
  
  // Compute majority categories for enums only
  public void computeMajorities() {
    if (!_enum) return;
    for (int i = 0; i < mins.length; i++) mins[i] = i;
    for (int i = 0; i < maxs.length; i++) maxs[i] = i;
    int mini = 0, maxi = 0;
    for( int i = 0; i < bins.length; i++ ) {
      if (bins[i] < bins[(int)mins[mini]]) {
        mins[mini] = i;
        for (int j = 0; j < mins.length; j++) 
          if (bins[(int)mins[j]] > bins[(int)mins[mini]]) mini = j;
      }
      if (bins[i] > bins[(int)maxs[maxi]]) {
        maxs[maxi] = i;
        for (int j = 0; j < maxs.length; j++) 
          if (bins[(int)maxs[j]] < bins[(int)maxs[maxi]]) maxi = j;
      }
    }
    for (int i = 0; i < mins.length - 1; i++)
      for (int j = 0; j < i; j++)
        if (bins[(int)mins[j]] > bins[(int)mins[j+1]]) { 
          double t = mins[j]; mins[j] = mins[j+1]; mins[j+1] = t;
        }
    for (int i = 0; i < maxs.length - 1; i++)
      for (int j = 0; j < i; j++)
        if (bins[(int)maxs[j]] < bins[(int)maxs[j+1]]) { 
          double t = maxs[j]; maxs[j] = maxs[j+1]; maxs[j+1] = t;
        }
  }

  public double percentileValue(int idx) {
    if( _enum ) return Double.NaN;
    if(percentileValues == null) computePercentiles();
    return percentileValues[idx];
  }

  @Override public String toString(){
    StringBuilder res = new StringBuilder("ColumnSummary[" + start + ":" + binValue(bins.length) +", binsz=" + binsz+"]");
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
      sb.append("<th>zeros</th><td>" + zeros + "</td>");
      sb.append("<th>min[" + mins.length + "]</th>");
      for( double min : mins ) {
        if (min == Double.POSITIVE_INFINITY) break;
        sb.append("<td>" + Utils.p2d(min) + "</td>");
      }
      sb.append("<th>max[" + maxs.length + "]</th>");
      for( double max : maxs ) {
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
    int len = Math.min(bins.length,MAX_HISTO_BINS_DISPLAYED);
    sb.append("<div style='width:100%;overflow-x:auto;'><table class='table-bordered'>");
    sb.append("<tr> <th colspan="+len+" style='text-align:center'>Histogram</th></tr>");
    sb.append("<tr>");
    if (_enum)
       for( int i=0; i<len; i++ ) sb.append("<th>" + vec.domain(i) + "</th>");
    else
       for( int i=0; i<len; i++ ) sb.append("<th>" + Utils.p2d(binValue(i)) + "</th>");
    sb.append("</tr>");
    sb.append("<tr>");
    for( int i=0; i<len; i++ ) sb.append("<td>" + bins[i] + "</td>");
    sb.append("</tr>");
    sb.append("<tr>");
    for( int i=0; i<len; i++ )
      sb.append(String.format("<td>%.1f%%</td>",(100.0*bins[i]/_avail_rows)));
    sb.append("</tr>");
    if( bins.length >= MAX_HISTO_BINS_DISPLAYED )
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
      for (double pv : percentileValues)
        sb.append("<td>" + Utils.p2d(pv) + "</td>");
      sb.append("</tr>");
      sb.append("</table>");
      sb.append("</div>");
    }
    sb.append("</div>\n"); 
  }
}
