package hex;

import water.H2O;
import water.Iced;
import water.MRTask2;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;
import water.util.Log;
import water.util.Utils;

import java.util.*;

public final class GroupedPct {
  public static class Summary extends Iced {
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    // This Request supports the HTML 'GET' command, and this is the help text
    // for GET.
    static final String DOC_GET = "Returns a summary of a fluid-vec frame";

    public double _min;
    public double _max;
    public long   _size;
    public double _step;
    public long[] _bins;
    public int[]  _invpct;
    @API(help = "Group name.")
    public String _name;
    @API(help = "Group ID.")
    public long   _gid;
    @API(help = "Percentiles.")
    public double[] _pctiles;

    public Summary(long gid) {_gid = gid;_min=Double.MAX_VALUE;_max=Double.MIN_VALUE;_size=0;_step=0;
      _bins=null;_pctiles=null;_invpct=null;_name=null;}
    public Summary(Summary other) {_gid = other._gid; _min=other._min; _max=other._max; _size=other._size;_step=other._step;
      _name = other._name;
      _bins=other._bins==null?null:other._bins.clone();
      _invpct=other._invpct==null?null:other._invpct.clone();
      _pctiles=other._pctiles==null?null:other._pctiles.clone();
    }
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
    public void computeInvpct() {
      if (_pctiles != null) return;
      long s = 0;
      _pctiles = new double[101];
      _invpct  = new int[_bins.length];

      for (int i = 0; i < _pctiles.length; i++) _pctiles[i] = _max;
      for(int i = 0; i < _bins.length; i++) {
        _invpct[i] = (int)(s*100/_size);
        s += _bins[i];
      }
    }
    public void cleanUpPct() {
      for (int i = _pctiles.length-2; i >= 0; i--)
        if (_pctiles[i] > _pctiles[i+1])
          _pctiles[i] = _pctiles[i+1];
    }
    public boolean toHTML( StringBuilder sb ) {
      sb.append("<div class='table' style='width:90%;heigth:90%;border-top-style:solid;'>" +
              "<div class='alert-success'><h4>GID: " + _gid + "   " + _name + "</h4></div>");
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
  public abstract static class GID extends Iced {
    int[] ncls;
    abstract public long gid(long[] levels);
    abstract public long[] levels(long gid);
  }

  public static class Grouping extends Iced {
    private Vec[] _gcols;
    private Vec[] _vcols;   /*enum-ized columns*/
    private int[] _ncls;
    public Grouping(Frame fr, int[] gcols) {
      _gcols = new Vec[gcols.length];
      _vcols = new Vec[gcols.length];
      _ncls  = new int[gcols.length];
      int vi = 0;
      for (int i = 0; i < gcols.length; i++) {
        Vec col = fr.vecs()[gcols[i]];
        if (!col.isInt()) throw H2O.unimpl();
        if (!col.isEnum()) {
          col = col.toEnum();
          _vcols[vi++] = col;
        }
        _gcols[i] = col;
        _ncls[i] = col.domain().length;
      }
      _vcols = Arrays.copyOf(_vcols, vi);
      long arity = 1;
      for (int i = 0; i < _ncls.length; i++) {
        arity *= _ncls[i];
        if (arity < 0) throw H2O.unimpl();
      }
    }
    public Vec[] columns() { return _gcols; }
    public void remove() {}

    public GID buildGID() {
      return new GID() {
        {
          ncls = _ncls ;
        }
        @Override public long gid(long[] levels) {
          assert levels.length == ncls.length;
          long id = 0;
          long arity = 1;
          for (int i = 0; i < levels.length; i++) {
            id += arity * levels[i];
            arity *= ncls[i];
          }
          return id;
        }
        public long[] levels(long gid) {
          long[] values = new long[ncls.length];
          long   q = gid;
          for (int i = 0; i < ncls.length; i++) {
            values[i] = q % ncls[i];
            q = q / ncls[i];
          }
          return values;
        }
      };
    }
    //public long gid(long[] levels) {
    //  assert levels.length == _ncls.length;
    //  long id = 0;
    //  long arity = 1;
    //  for (int i = 0; i < levels.length; i++) {
    //    id += arity * levels[i];
    //    arity *= _ncls[i];
    //  }
    //  return id;
    //}
    //public long[] levels(long gid) {
    //  long[] values = new long[_ncls.length];
    //  long   q = gid;
    //  for (int i = 0; i < _ncls.length; i++) {
    //    values[i] = q % _ncls[i];
    //    q = q / _ncls[i];
    //  }
    //  return values;
    //}
  }

  private Frame    _wf;             /*work frame*/
  private Frame    _pctfr;
  private Grouping _grp;
  public int       _vcol;
  public long[]    _gids;
  public Summary[] _gsums;
  private Utils.IcedHashMap<Utils.IcedLong, Summary> _sums;

  public GroupedPct(Frame fr, int[] gcols, int vcol) {
    _vcol = vcol;
    _grp = new Grouping(fr, gcols);

    String[] wf_names = new String[gcols.length+1];
    Vec[]    wf_vecs  = new Vec[gcols.length+1];
    for (int i = 0; i < gcols.length; i++) {
      wf_names[i] = fr.names()[gcols[i]];
      wf_vecs[i] = _grp.columns()[i];
    }
    wf_names[gcols.length] = fr.names()[vcol];
    wf_vecs[gcols.length] = fr.vecs()[vcol];
    _wf = new Frame(wf_names, wf_vecs);   /*work frame*/

    // collects simple summary(min, max, size) for each group
    GID g = _grp.buildGID();
    Utils.IcedHashMap<Utils.IcedLong, Summary> sums = new Pass1(_wf,g).doAll(_wf).sums;
    for (Summary s : sums.values()) {
      s.allocBins(s._min, s._max);
    }
    sums = new Pass2(_wf, g, sums).doAll(_wf).sums;
    _gids = new long[sums.size()];
    _gsums = new Summary[sums.size()];
    int i = 0;
    for (Utils.IcedLong key : sums.keySet())
      _gids[i++] = key._val;
    Arrays.sort(_gids);
    i = 0;
    for (long gid : _gids)
      _gsums[i++] = sums.get(new Utils.IcedLong(gid));
    // finally compute percentiles and put in group names
    for (Summary s : _gsums) {
      s.computeInvpct();
      long[] levels = g.levels(s._gid);
      s._name = new String("");
      for (int k = 0; k < levels.length; k++)
        s._name += (k==0?"":"  ") + "[" + _wf._names[k] + "]" + _wf.vecs()[k].domain(levels[k]);
    }
    _sums = sums;
    _pctfr = makeGroupPctCols();
    for (Summary s : _gsums)
      s.cleanUpPct();
  }

  public Frame getPctCols() { return _pctfr; }
  public Frame makeGroupPctCols() {
    return new Pass3(_wf,_grp.buildGID(),_sums).doAll(2,_wf)
            .outputFrame(new String[]{"GroupID","Percentile"}, new String[][]{null,null});
  }

  static class Pass1 extends MRTask2<Pass1>{
    final Frame fr;
    final GID   g;
    Utils.IcedHashMap<Utils.IcedLong, Summary> sums;
    public Pass1(Frame fr, GID g){
      this.fr = fr;
      this.g  = g;
      this.sums = null; 
    }
    @Override public void map(Chunk[] cs) {
      sums = new Utils.IcedHashMap<Utils.IcedLong, Summary>();
      int vix = cs.length-1;
      Utils.IcedLong gid = new Utils.IcedLong(0);
      long[] levels = new long[vix];
      OUTER:
      for (int i = 0; i < cs[0]._len; i++) {
        if (cs[vix].isNA0(i)) continue;
        for (int k = 0; k < vix; k++) { 
          if (cs[k].isNA0(i)) continue OUTER;
          levels[k] = cs[k].at80(i);
        }
        gid._val = g.gid(levels);
        double v = cs[vix].at0(i);
        Summary ss = sums.get(gid);
        if (ss == null)
          sums.put(new Utils.IcedLong(gid._val), (ss = new Summary(gid._val)));
        ss._min = Math.min(ss._min, v);
        ss._max = Math.max(ss._max, v);
        ss._size ++;
      }
    }
    @Override public void reduce(Pass1 that) {
      for (Map.Entry<Utils.IcedLong, Summary> e : that.sums.entrySet()) {
        Utils.IcedLong k = e.getKey();
        Summary s = this.sums.get(k);
        if (s == null) this.sums.put(k, e.getValue());
        else s.add(e.getValue());
      }
    }
  }

  static class Pass2 extends MRTask2<Pass2> {
    final Frame fr;
    final GID   g;
    Utils.IcedHashMap<Utils.IcedLong, Summary> sums0;
    Utils.IcedHashMap<Utils.IcedLong, Summary> sums;

    public Pass2(Frame fr, GID g, Utils.IcedHashMap<Utils.IcedLong, Summary> sums){
      this.fr = fr;
      this.g  = g;
      this.sums0 = sums;
      this.sums = null;
    }
    @Override public void map(Chunk[] cs) {
      sums = new Utils.IcedHashMap<Utils.IcedLong, Summary>();
      int vix = cs.length-1;
      Utils.IcedLong gid = new Utils.IcedLong(0);
      long[] levels = new long[vix];
      OUTER:
      for (int i = 0; i < cs[0]._len; i++) {
        if (cs[vix].isNA0(i)) continue;
        for (int k = 0; k < vix; k++) {
          if (cs[k].isNA0(i)) continue OUTER;
          levels[k] = cs[k].at80(i);
        }
        gid._val= g.gid(levels);
        double v = cs[vix].at0(i);
        Summary ss = sums.get(gid);
        if (ss==null) sums.put(new Utils.IcedLong(gid._val), (ss = new Summary(sums0.get(gid))));
        //Log.info("Looking for " + gid._val);
        //for (Utils.IcedLong key : sums.keySet())
        //  Log.info("hash keys : " + key._val);
        int ix = (int)((v - ss._min)/ss._step);
        ss._bins[ix]++;
      }
    }
    public void reduce(Pass2 that) {
      for (Map.Entry<Utils.IcedLong, Summary> e : that.sums.entrySet()) {
        Utils.IcedLong k = e.getKey();
        Summary s = this.sums.get(k);
        if (s==null) { sums.put(k, e.getValue()); continue; }
        long[] ob = e.getValue()._bins;
        for (int i = 0; i < s._bins.length; i++)
          s._bins[i] += ob[i];
      }
    }
  }

  static class Pass3 extends MRTask2<Pass3> {
    final Frame fr;
    final GID   g;
    Utils.IcedHashMap<Utils.IcedLong, Summary> sums;
    public Pass3(Frame fr, GID g, Utils.IcedHashMap<Utils.IcedLong, Summary> sums){
      this.fr = fr;
      this.g  = g;
      this.sums = sums;
    }
    @Override public void map(Chunk[] cs, NewChunk[] ncs) {
      int vix = cs.length-1;
      Utils.IcedLong gid = new Utils.IcedLong(0);
      long[] levels = new long[vix];
      OUTER:
      for (int i = 0; i < cs[0]._len; i++) {
        if (cs[vix].isNA0(i)) {
          ncs[0].addNum(Double.NaN);
          ncs[1].addNum(Double.NaN);
          continue;
        }
        for (int k = 0; k < vix; k++) {
          if (cs[k].isNA0(i)) {
            ncs[0].addNum(Double.NaN);
            ncs[1].addNum(Double.NaN);
            continue OUTER;
          }
          levels[k] = cs[k].at80(i);
        }
        gid._val= g.gid(levels);
        ncs[0].addNum(gid._val);
        if (cs[vix].isNA0(i)) {
        ncs[1].addNum(Double.NaN);
          continue;
        }
        double v = cs[vix].at0(i);
        Summary ss = sums.get(gid);
        //Log.info("Looking for " + gid._val);
        //for (Utils.IcedLong key : sums.keySet())
        //  Log.info("hash keys : " + key._val);
        int ix = (int)((v - ss._min)/ss._step);
        int pct = ss._invpct[ix];
        if (v < ss._pctiles[pct]) ss._pctiles[pct] = v; // Racy!
        ncs[1].addNum(ss._invpct[ix]);
      }
    }
  }
}
