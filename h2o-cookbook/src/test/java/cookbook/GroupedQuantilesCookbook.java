package cookbook;

import org.junit.Before;
import org.junit.Test;
import water.*;
import water.exec.Flow;
import water.fvec.*;
import water.util.Log;
import water.util.RemoveAllKeysTask;
import water.util.Utils.IcedHashMap;
import water.util.Utils.IcedLong;

public class GroupedQuantilesCookbook extends  TestUtil {
  @Before
  public void removeAllKeys() {
    Log.info("Removing all keys...");
    RemoveAllKeysTask collector = new RemoveAllKeysTask();
    collector.invokeOnAllNodes();
    Log.info("Removed all keys.");
  }

  static class AddSparseGroupNumber extends MRTask2<AddSparseGroupNumber> {
    final int cyl_idx;
    final int sg_idx;
    public AddSparseGroupNumber(int cix, int gix) {cyl_idx = cix; sg_idx= gix;}
    @Override public void map(Chunk[] cs) {
      for (int i = 0; i < cs[0]._len; i++) {
        long cyl = cs[cyl_idx].at80(i);
        cs[sg_idx].set0(i,cyl);
      }
    }
  }

  static class CompactGroupNumber extends MRTask2<CompactGroupNumber> {
    final int sg_idx;
    IcedHashMap<IcedLong, IcedLong> sparse_group_number_set;
    public CompactGroupNumber(int gix) { sg_idx = gix; }
    @Override public void map(Chunk[] cs) {
      sparse_group_number_set = new IcedHashMap<IcedLong, IcedLong>();
      for (int i = 0; i < cs[0]._len; i++) {
        long sgid = cs[sg_idx].at80(i);
        IcedLong sgid_ice = new IcedLong(sgid);
        sparse_group_number_set.put(sgid_ice, null);
      }
    }
    @Override public void reduce( CompactGroupNumber that ) {
      this.sparse_group_number_set.putAll(that.sparse_group_number_set);
    }
  }

  static class AssignCompactGroupNumber extends MRTask2<AssignCompactGroupNumber> {
    final int sg_idx;
    final int dg_idx;
    final IcedHashMap<IcedLong, IcedLong> gid_map;
    public AssignCompactGroupNumber(IcedHashMap<IcedLong, IcedLong> gm, int sgi, int dgi ) {
      gid_map = gm;
      sg_idx  = sgi;
      dg_idx  = dgi;
    }
    @Override public void map(Chunk[] cs) {
      for (int i = 0; i < cs[0]._len; i++) {
        long sgid = cs[sg_idx].at80(i);
        IcedLong sgid_ice = new IcedLong(sgid);
        IcedLong dgid_ice = gid_map.get(sgid_ice);
        if (dgid_ice == null) throw new RuntimeException("GID missing.");
        cs[dg_idx].set0(i, dgid_ice._val);
      }
    }
  }

  static class MyGroupBy extends Flow.GroupBy {
    int dg_idx;
    public MyGroupBy(int gix) {dg_idx = gix;}
    public long groupId(double ds[]) {
      return (long) ds[dg_idx];
    }
  }

  static class BasicSummary extends Flow.PerRow<BasicSummary> {
    double _min;
    double _max;
    long _n;
    int  _val_idx;
    public BasicSummary(int vix) {
      _min = Double.MAX_VALUE;
      _max = Double.MIN_VALUE;
      _n = 0;
      _val_idx = vix;
    }
    @Override public void mapreduce( double ds[] ) {
      _min  = Math.min(_min, ds[_val_idx]);
      _max  = Math.max(_max, ds[_val_idx]);
      _n ++;
    }

    @Override public void reduce( BasicSummary that ) {
      _min = Math.min(_min, that._min);
      _max = Math.max(_max, that._max);
      _n += that._n;
    }

    @Override public BasicSummary make() {
      BasicSummary b = new BasicSummary(_val_idx);
      return b;
    }
  }

  static class Histogram extends Flow.PerRow<Histogram> {
    private final IcedHashMap<IcedLong, BasicSummary> _bs;
    private final int  _dg_idx;
    private final int  _val_idx;
    private final int  _wt_idx;
    private final boolean _weighted;

    long   _n;          // Number of samples in this group.
    double _min;        // Smallest sample value in this group.

    double _step;       // Bin step size.
    double[] _bins;     // Array of bins (note: optionally weighted).
    double[] _values;   // Smallest value in the bin.

    public Histogram(int gix, int vix, int wix, boolean wt, IcedHashMap<IcedLong, BasicSummary> bs) {
      _dg_idx = gix;
      _val_idx = vix;
      _wt_idx = wix;
      _weighted = wt;
      _bs = bs;

      _n = 0;
    }

    public void allocBins(double min, double max) {
      if (max - min < 1e-10) {
        // Case where the column has a constant value.
        // Only need one bucket.
        _bins = new double[1]; _step = 1e-10;
        _values = new double[1];
        _values[0] = Double.NaN;
      }
      else {
        // Normal case where the column has many buckets.
        int nbin = 1000;
        _step = (max - min)/(double)nbin;

        // Guard against off-by-one bucketing due to floating point rounding error.
        while ((int)((max - min)/_step) >= nbin++);

        // Allocate and initialize.
        _bins = new double[nbin];
        _values = new double[nbin];
        for (int i = 0; i < _values.length; i++) {
          _values[i] = Double.NaN;
        }
      }
    }

    @Override public void mapreduce( double ds[] ) {
      if (_bins == null) {
        long gid = (long) ds[_dg_idx];
        BasicSummary bs = _bs.get(new IcedLong(gid));
        _min = bs._min;
        _n   = bs._n;
        allocBins(bs._min, bs._max);
      }
      double v = ds[_val_idx];
      double w;
      int ix = (int) ((v - _min)/_step);
      if (Double.isNaN(_values[ix]))
        _values[ix] = v;
      else if (!Double.isNaN(v))
        _values[ix] = Math.min( _values[ix], v);

      if (_weighted) {
        w = ds[_wt_idx];
        _bins[ix] += w * v;
      } else {
        _bins[ix]++;
      }
    }

    @Override public void reduce( Histogram that ) {
      if (this._bins == null) {
        _min = that._min;
        _n = that._n;
        _step = that._step;
        _bins = that._bins;
        _values = that._values;
      }
      else if (that._bins == null) {
        // do nothing
      } else {
        for (int i = 0; i < _bins.length; i++) {
          _bins[i] += that._bins[i];
          if (Double.isNaN(_values[i])) {
            _values[i] = that._values[i];
          }
          else if (Double.isNaN(that._values[i])) {
            // do nothing
          }
          else {
            _values[i] = Math.min(_values[i], that._values[i]);
          }
        }
      }
    }

    @Override public Histogram make() {
      Histogram h = new Histogram(_dg_idx, _val_idx, _wt_idx, _weighted, _bs);
      return h;
    }
  }

  @Test
  public void testGroupedQuantiles() {
    // final String INPUT_FILE_NAME = "/Users/bai/testdata/year2013.csv";
    final String INPUT_FILE_NAME = "../smalldata/airlines/allyears2k_headers.zip";
    final String KEY_STRING = "airlines.hex";

    final String GROUP_COLUMN_NAME = "CRSDepTime";
    final String VALUE_COLUMN_NAME = "Distance";
    final String WEIGHT_COLUMN_NAME = "";
    Key k = Key.make(KEY_STRING);
    Frame fr = parseFrame(k, INPUT_FILE_NAME);

    Log.info("");
    Log.info("Pass 0, add group number columns to frame");
    Log.info("");

    fr.add("sparse_group_number", fr.anyVec().makeZero());
    fr.add("dense_group_number", fr.anyVec().makeZero());
    Futures fs = new Futures();
    UKV.put(k, fr, fs);
    fs.blockForPending();

    Log.info("");
    Log.info("Pass 1, assign group numbers to rows");
    Log.info("");

    final int cyl_idx = fr.find(GROUP_COLUMN_NAME);
    final int sg_idx  = fr.find("sparse_group_number");
    final int dg_idx  = fr.find("dense_group_number");
    final int val_idx = fr.find(VALUE_COLUMN_NAME);
    final int wt_idx  = fr.find(WEIGHT_COLUMN_NAME);
    new AddSparseGroupNumber(cyl_idx, sg_idx).doAll(fr);


    Log.info("");
    Log.info("Pass 2, compact group numbers");
    Log.info("");

    IcedHashMap<IcedLong, IcedLong> sparse_group_number_set =
            new CompactGroupNumber(sg_idx).doAll(fr).sparse_group_number_set;

    int ng = 1;
    for (IcedLong key : sparse_group_number_set.keySet())
      sparse_group_number_set.put(key, new IcedLong(ng++));

    Log.info("");
    Log.info("Pass 3, assign dense group numbers");
    Log.info("");

    new AssignCompactGroupNumber(sparse_group_number_set, sg_idx, dg_idx).doAll(fr);

    Log.info("");
    Log.info("Pass 4, collect basic stats for each dense group");
    Log.info("");

    IcedHashMap<IcedLong, BasicSummary> basic_summaries =
      fr.with(new MyGroupBy(dg_idx))
      .with(new BasicSummary(val_idx))
      .doit();

    Log.info("");
    Log.info("Pass 5, calculate histograms");
    Log.info("");

    IcedHashMap<IcedLong, Histogram> histograms =
      fr.with(new MyGroupBy(dg_idx))
      .with(new Histogram(dg_idx, val_idx, wt_idx, false, basic_summaries))
      .doit();

    Log.info("");
    Log.info("Debug Step 6 (not a real pass), put histograms into a frame for display");
    Log.info("");

    AppendableVec gidVec = new AppendableVec("GID");
    AppendableVec[] avecs = new AppendableVec[1001];
    Vec[]           vecs  = new Vec[1001];
    NewChunk[] chunks = new NewChunk[1001];
    for (int i = 1; i < 1001; i++) {
      avecs[i] =  new AppendableVec("Bin_" + i);
      chunks[i] = new NewChunk(avecs[i], 0);
    }
    avecs[0] = gidVec;
    chunks[0] = new NewChunk(avecs[0], 0);

    fs = new Futures();
    for (IcedLong key : histograms.keySet()) {
      Histogram hist = histograms.get(key);
      chunks[0].addNum(key._val);
      chunks[0].addNum(key._val);
      double[] bins = hist._bins;
      double[] vals = hist._values;
      for (int i=1; i < 1001; i++) {
        if (i-1 < bins.length) {
          chunks[i].addNum(bins[i-1]);
          chunks[i].addNum(vals[i-1]);
        } else {
          chunks[i].addNum(Double.NaN);
          chunks[i].addNum(Double.NaN);
        }
      }
    }
    for (int i=0; i < chunks.length; i++) {
      chunks[i].close(0, fs);
      vecs[i] = avecs[i].close(fs);
    }
    fs.blockForPending();
    String[] vnames = new String[1001];
    vnames[0] = "GID";
    for (int i = 1; i < 1001; i++) vnames[i] = "Bin_" + (i-1);
    Frame histfr = new Frame(vnames, vecs);
    Key histfr_key = Key.make("histograms.hex");

    fs = new Futures();
    UKV.put(histfr_key, histfr, fs);
    fs.blockForPending();

//    Log.info("");
//    Log.info("Debug Step 7 (not a real pass), sleep for browser testing");
//    Log.info("");
//    try { Thread.sleep(100000000); } catch (Exception e) {}

    UKV.remove(k);
    UKV.remove(histfr_key);
  }
}
