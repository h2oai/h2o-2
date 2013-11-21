package hex;

import water.Iced;
import water.MRTask2;
import water.api.DocGen;
import water.api.Request;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.util.Utils;

import java.util.*;

/**
 * Pass1 collects group sizes and min/max per group.
 * Pass2 calculates the percentiles for each group.
 * appendPctCol (optional third pass) appends percentiles to the input frame.
 */
public final class GroupedPct {

  /**
   * Information summarized for one group.
   */
  public static class Summary extends Iced {
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    // This Request supports the HTML 'GET' command, and this is the help text
    // for GET.
    static final String DOC_GET = "Returns a summary of a fluid-vec frame";

    @Request.API(help = "Group Id")
    public long   _gid;

    @Request.API(help = "Percentiles")
    public double[] _pctiles;

    private double _min;      // Minimum value in the group
    private double _max;      // Maximum value in the group
    private long   _size;     // Number of elements in the group
    private double _step;     // Histogram bin size
    private long[] _bins;     // Number of bins in the histogram (typically 1000)
    private int[]  _invpct;   // Inverted mapping of the percentiles.  Percentile value of each bin boundary.

    public Summary(long gid) {_gid = gid;_min=Double.MAX_VALUE;_max=Double.MIN_VALUE;_size=0;_step=0;_bins=null;_pctiles=null;}

    /**
     * Merge (reduce) two summaries together.
     */
    public Summary add(Summary other) {
      _min = Math.min(_min, other._min);
      _max = Math.max(_max, other._max);
      _size += other._size;
      if (other._bins != null)
        if (_bins == null) _bins = other._bins;
        else
          for (int i = 0; i < _bins.length; i++) _bins[i] += other._bins[i];
      return this;
    }

    /**
     * Allocate the array of bins for this group.
     * @param min Minimum value in the group
     * @param max Maximum value in the group
     * @return the array
     */
    public Summary allocBins(double min, double max) {
      if (max - min < 1e-10) {
        _bins = new long[1]; _step = 1e-10;
      }
      else {
        int nbin = 1000;
        _step = (max - min)/(double)nbin;
        while ((int)((max - min)/_step) >= nbin++);
        _bins = new long[nbin];
      } 
      return this;
    }

    /**
     * Compute percentile values for this group.
     */
    public void computePctiles() {
      if (_pctiles != null) return;
      int k = 0;
      long s = 0;
      _pctiles = new double[100];
      _invpct  = new int[_bins.length];
      for(int j = 0; j < 100; ++j) {
        final long s1 = _size*j/100;
        long bc = 0;
        while(s1 > s+(bc = _bins[k])){
          _invpct[k] = j;
          s  += bc;
          k++;
        }
        _pctiles[j] = _min + k*_step;
      }
    }

    public boolean toHTML( StringBuilder sb ) {
      sb.append("<div class='table' style='width:90%;heigth:90%;border-top-style:solid;'>" +
              "<div class='alert-success'><h4>GID: " + _gid + "</h4></div>");
      sb.append("<div style='width:100%;overflow-x:auto;'><table class='table-bordered'>");
      sb.append("<tr> <th colspan='" + _pctiles.length + "' " +
              "style='text-align:center' " +
              ">Percentiles</th></tr>");
      sb.append("<tr><th>Threshold(%)</th>");
      for (int i = 0; i < _pctiles.length; i++)
        sb.append("<td>" + i + "</td>");
      sb.append("</tr>");
      sb.append("<tr><th>Value</th>");
      for (double pv : _pctiles)
        sb.append("<td>" + Utils.p2d(pv) + "</td>");
      sb.append("</tr>");
      sb.append("</table>");
      sb.append("</div></div>");
      return true;
    }
  }

  /**
   * This is the main output of the grouped percentile calculation.
   */
  public Summary[] _gsums;

  // Private to the algorithm.
  private Frame     _fr;
  private int       _gcol;
  private int       _vcol;
  private long[]    _gids;
  private Utils.IcedHashMap<Utils.IcedLong, Summary> _gs;   // Hash map to group summaries.

  /**
   * Constructor.
   * Note:  this actually runs the algorithm.
   *
   * @param fr Frame to perform the grouped percentile operation on.  Note that a new column can be added to this frame by the appendPctCol operation.
   * @param gcol GroupId column index.
   * @param vcol Value column index to calculate percentiles on.
   */
  public GroupedPct(Frame fr, int gcol, int vcol) {
    _fr = fr;
    _gcol = gcol;
    _vcol = vcol;

    // Collects simple summary(min, max, size) for each group.
    Utils.IcedHashMap<Utils.IcedLong, Summary> gs = new Pass1(fr, gcol, vcol).doAll(_fr).gs;
    for (Summary s : gs.values())
      s.allocBins(s._min, s._max);

    // Calculate summary for each group as a MapReduce Task.
    gs = new Pass2(fr, gcol, vcol, gs).doAll(fr).gs;

    // Sort output based on groupId and calculate percentiles.
    _gids = new long[gs.size()];
    _gsums = new Summary[gs.size()];
    int i = 0;
    for (Utils.IcedLong key : gs.keySet())
      _gids[i++] = key._val;
    Arrays.sort(_gids);
    i = 0;
    for (long gid : _gids)
      _gsums[i++] = gs.get(new Utils.IcedLong(gid));
    for (Summary s : _gsums)
      s.computePctiles();
    _gs = gs;
  }

  /**
   * Append a column to the frame.
   * @return The same frame.
   */
  public Frame appendPctCol() {
    Frame pctfr = new AppendPct(_fr, _gcol, _vcol, _gs).doAll(1,_fr)
            .outputFrame(new String[]{"Percentile"}, new String[][]{null});
    return _fr.add(pctfr);
  }

  /**
   * Collects group sizes and min/max per group.
   */
  static class Pass1 extends MRTask2<Pass1>{
    final Frame fr;
    final int   gc;
    final int   vc;
    Utils.IcedHashMap<Utils.IcedLong, Summary> gs;
    public Pass1(Frame fr, int gcol, int vcol){
      this.fr = fr;
      this.gc = gcol;
      this.vc = vcol;
      this.gs = null;
    }
    @Override public void map(Chunk[] cs) {
      Utils.IcedLong gid = new Utils.IcedLong(0);
      this.gs = new Utils.IcedHashMap<Utils.IcedLong, Summary>();
      for (int i = 0; i < cs[0]._len; i++) {
        // Ignore rows that have missing elements in the groupId or value column.
        if (cs[gc].isNA0(i) || cs[vc].isNA0(i)) continue;

        // Read a long value from the groupId column as the groupId.
        gid._val = cs[gc].at80(i);

        // The value we are going to add to the summary info.
        double v = cs[vc].at0(i);

        // Find the partial summary object (for this set of chunks cs).
        // Make a new one if it didn't exist before.
        Summary ss = gs.get(gid);
        if (ss == null) {
          gs.put(new Utils.IcedLong(gid._val), (ss = new Summary(gid._val)));
          ss._min = ss._max = v;
        } else {
          ss._min = Math.min(ss._min, v);
          ss._max = Math.max(ss._max, v);
        }
        ss._size ++;
      }
    }

    /**
     * Take two Summary objects and merge them.
     */
    @Override public void reduce(Pass1 that) {
      for (Utils.IcedLong k : that.gs.keySet()) {
        Summary s = this.gs.get(k);
        if (s == null) this.gs.put(k,that.gs.get(k));
        else s.add(that.gs.get(k));
      }
    }
  }

  /**
   * Calculates the percentiles for each group.
   */
  static class Pass2 extends MRTask2<Pass2> {
    final Frame fr;
    final int   gc;
    final int   vc;
    Utils.IcedHashMap<Utils.IcedLong, Summary> gs0;   // Read-only.  Access base stats for each group here.
    Utils.IcedHashMap<Utils.IcedLong, Summary> gs;    // Write calculations into this copy.

    public Pass2(Frame fr, int gcol, int vcol, Utils.IcedHashMap<Utils.IcedLong, Summary> gs){
      this.fr = fr;
      this.gc = gcol;
      this.vc = vcol;
      this.gs0 = gs;
      this.gs = null;
    }

    /**
     * Update partial summaries on this set of chunks.
     */
    @Override public void map(Chunk[] cs) {
      Utils.IcedLong gid = new Utils.IcedLong(0);
      gs = new Utils.IcedHashMap<Utils.IcedLong, Summary>();
      for (int i = 0; i < cs[0]._len; i++) {
        if (cs[gc].isNA0(i) || cs[vc].isNA0(i)) continue;
        gid._val= cs[gc].at80(i);
        double v = cs[vc].at0(i);
        Summary ss = gs.get(gid);
        if (ss == null) gs.put(new Utils.IcedLong(gid._val), (ss = gs0.get(gid)));
        //Log.info("Looking for " + gid._val);
        //for (Utils.IcedLong key : gs.keySet())
        //  Log.info("hash keys : " + key._val);
        int ix = (int)((v - ss._min)/ss._step);
        ss._bins[ix]++;
      }
    }

    /**
     * Reduce summaries.  Use the IcedHashMap to match them up.
     * Reduce histograms.
     */
    @Override public void reduce(Pass2 that) {
      for (Map.Entry<Utils.IcedLong, Summary> e : that.gs.entrySet()) {
        Utils.IcedLong k = e.getKey();
        Summary s = this.gs.get(k);
        if (s == null) { this.gs.put(k, e.getValue()); continue; }
        long[] thatGuysHistogramBins = e.getValue()._bins;
        for (int i = 0; i < s._bins.length; i++)
          s._bins[i] += thatGuysHistogramBins[i];
      }
    }
  }

  /**
   * (optional third pass) appends percentiles to the input frame
   */
  static class AppendPct extends MRTask2<AppendPct> {
    final Frame fr;
    final int   gc;
    final int   vc;
    Utils.IcedHashMap<Utils.IcedLong, Summary> gs;
    public AppendPct(Frame fr, int gcol, int vcol, Utils.IcedHashMap<Utils.IcedLong, Summary> gs){
      this.fr = fr;
      this.gc = gcol;
      this.vc = vcol;
      this.gs = gs;
    }
    @Override public void map(Chunk[] cs, NewChunk nc) {
      Utils.IcedLong gid = new Utils.IcedLong(0);
      for (int i = 0; i < cs[0]._len; i++) {
        if (cs[gc].isNA0(i) || cs[vc].isNA0(i))
        { nc.addNum(Double.NaN); continue; }
        gid._val = cs[gc].at80(i);
        double v = cs[vc].at0(i);
        Summary ss = gs.get(gid);
        int ix = (int)((v - ss._min)/ss._step);
        nc.addNum(ss._invpct[ix]);
      }
    }
  }
}
