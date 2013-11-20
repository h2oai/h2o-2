package hex;

import water.Iced;
import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;
import water.util.Utils;

import java.util.*;

public final class GroupedPct extends MRTask2<GroupedPct> {
  public static class Summary extends Iced {
    public double _min;
    public double _max;
    public long   _size;
    public double _step;
    public long[] _bins;
    public double[] _pctiles;
    public Summary() {_min=Double.MAX_VALUE;_max=Double.MIN_VALUE;_size=0;_step=0;_bins=null;_pctiles=null;}
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
    public double[] getPctiles() {
      if (_pctiles != null) return _pctiles;
      int k = 0;
      long s = 0;
      _pctiles = new double[100];
      for(int j = 0; j < 100; ++j) {
        final long s1 = _size*j/100;
        long bc = 0;
        while(s1 > s+(bc = _bins[k])){
          s  += bc;
          k++;
        }
        _pctiles[j] = _min + k*_step;
      }
      return _pctiles;
    }
  }
  public static class SummaryHashMap extends Utils.IcedHashMap<Utils.IcedLong, Summary>{}

  public static abstract class GroupingTask extends MRTask2<GroupingTask> {
    public Frame    _fr;
    public int[]    _gcols;
    public void map(Chunk[] cs, NewChunk nc) {
      assert false:"Overriding is required.";
    }
    public void reduce(GroupedPct other) {
      assert false:"Overriding is required.";
    }
  }

  public Frame _fr;
  public int   _gcol;
  public int   _vcol;
  public long[] _gids;
  public double[][] _pctiles;

  public GroupedPct(Frame fr, int gcol, int vcol) {
    _fr = fr;
    _gcol = gcol;
    _vcol = vcol;

    // collects simple summary(min, max, size) for each group
    SummaryHashMap gs = new Pass1(fr, gcol, vcol).doAll(_fr).gs;
    for (Summary s : gs.values())
      s.allocBins(s._min, s._max);
    gs = new Pass2(fr, gcol, vcol, gs).doAll(fr).gs;
    _gids = new long[gs.size()];
    int i = 0;
    for (Utils.IcedLong key : gs.keySet())
      _gids[i++] = key._val;
    Arrays.sort(_gids);
    i = 0;
    for (long gid : _gids)
      _pctiles[i++] = gs.get(gid).getPctiles();
  }

  static class Pass1 extends MRTask2<Pass1>{
    final Frame fr;
    final int   gc;
    final int   vc;
    SummaryHashMap gs;
    public Pass1(Frame fr, int gcol, int vcol){
      this.fr = fr;
      this.gc = gcol;
      this.vc = vcol;
      this.gs = new SummaryHashMap();
    }
    @Override public void map(Chunk[] cs) {
      for (int i = 0; i < cs[0]._len; i++) {
        if (cs[gc].isNA0(i) || cs[vc].isNA0(i)) continue;
        long gid = cs[gc].at80(i);
        double v = cs[vc].at0(i);
        Summary ss = gs.get(gid);
        if (ss == null)
          gs.put(gid, (ss = new Summary());
        ss._min = Math.min(ss._min, v);
        ss._max = Math.max(ss._max, v);
        ss._size ++;
      }
    }
    @Override public void reduce(Pass1 other) {
      for (Map.Entry<Utils.IcedLong, Summary> e : other.gs.entrySet()) {
        Utils.IcedLong k = e.getKey();
        Summary s = this.gs.get(k);
        if (s == null) this.gs.put(k, e.getValue());
        else s.add(e.getValue());
      }
    }
  }

  static class Pass2 extends MRTask2<Pass2> {
    final Frame fr;
    final int   gc;
    final int   vc;
    SummaryHashMap gs;
    public Pass2(Frame fr, int gcol, int vcol, SummaryHashMap gs){
      this.fr = fr;
      this.gc = gcol;
      this.vc = vcol;
      this.gs = gs;
    }
    @Override public void map(Chunk[] cs) {
      for (int i = 0; i < cs[0]._len; i++) {
        if (cs[gc].isNA0(i) || cs[vc].isNA0(i)) continue;
        long gid = cs[gc].at80(i);
        double v = cs[vc].at0(i);
        Summary ss = gs.get(gid);
        int ix = (int)((v - ss._min)/ss._step);
        ss._bins[ix]++;
      }
    }
    @Override public void reduce(Pass2 other) {
      for (Map.Entry<Utils.IcedLong, Summary> e : other.gs.entrySet()) {
        Utils.IcedLong k = e.getKey();
        Summary s = this.gs.get(k);
        long[] ob = e.getValue()._bins;
        for (int i = 0; i < s._bins.length; i++)
          s._bins[i] += ob[i];
      }
    }
  }
}
