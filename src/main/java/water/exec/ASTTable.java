package water.exec;

import water.Key;
import water.MRTask2;
import water.fvec.*;
import water.util.Utils;

import java.util.Arrays;

public class ASTTable extends ASTOp {
  ASTTable() { super(new String[]{"table", "ary"}, new Type[]{Type.ARY,Type.ARY},
                     OPF_PREFIX,
                     OPP_PREFIX,
                     OPA_RIGHT); }
  @Override String opStr() { return "table"; }
  @Override ASTOp make() { return new ASTTable(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    int ncol;
    Frame fr = env.ary(-1);
    if ((ncol = fr.vecs().length) > 2)
      throw new IllegalArgumentException("table does not apply to more than two cols.");
    for (int i = 0; i < ncol; i++) if (!fr.vecs()[i].isInt())
      throw new IllegalArgumentException("table only applies to integer vectors.");
    String[][] domains = new String[ncol][];  // the domain names to display as row and col names
                                              // if vec does not have original domain, use levels returned by CollectDomain
    long[][] levels = new long[ncol][];
    for (int i = 0; i < ncol; i++) {
      Vec v = fr.vecs()[i];
      levels[i] = new Vec.CollectDomain(v).doAll(new Frame(v)).domain();
      domains[i] = v.domain();
    }
    long[][] counts = new Tabularize(levels).doAll(fr)._counts;
    // Build output vecs
    Key keys[] = Vec.VectorGroup.VG_LEN1.addVecs(counts.length+1);
    Vec[] vecs = new Vec[counts.length+1];
    String[] colnames = new String[counts.length+1];
    AppendableVec v0 = new AppendableVec(keys[0]);
    v0._domain = fr.vecs()[0].domain() == null ? null : fr.vecs()[0].domain().clone();
    NewChunk c0 = new NewChunk(v0,0);
    for( int i=0; i<levels[0].length; i++ ) c0.addNum((double) levels[0][i]);
    c0.close(0,null);
    vecs[0] = v0.close(null);
    colnames[0] = "row.names";
    if (ncol==1) colnames[1] = "Count";
    for (int level1=0; level1 < counts.length; level1++) {
      AppendableVec v = new AppendableVec(keys[level1+1]);
      NewChunk c = new NewChunk(v,0);
      v._domain = null;
      for (int level0=0; level0 < counts[level1].length; level0++)
        c.addNum((double) counts[level1][level0]);
      c.close(0, null);
      vecs[level1+1] = v.close(null);
      if (ncol>1) {
        colnames[level1+1] = domains[1]==null? Long.toString(levels[1][level1]) : domains[1][(int)(levels[1][level1])];
      }
    }
    env.pop(2);
    env.push(new Frame(colnames, vecs));
  }
  public static class Tabularize extends MRTask2<Tabularize> {
    public final long[][]  _domains;
    public long[][] _counts;

    public Tabularize(long[][] dom) { super(); _domains=dom; }
    @Override public void map(Chunk[] cs) {
      assert cs.length == _domains.length;
      _counts = _domains.length==1? new long[1][] : new long[_domains[1].length][];
      for (int i=0; i < _counts.length; i++) _counts[i] = new long[_domains[0].length];
      for (int i=0; i < cs[0]._len; i++) {
        if (cs[0].isNA0(i)) continue;
        long ds[] = _domains[0];
        int level0 = Arrays.binarySearch(ds, cs[0].at80(i));
        assert 0 <= level0 && level0 < ds.length : "l0="+level0+", len0="+ds.length+", min="+ds[0]+", max="+ds[ds.length-1];
        int level1;
        if (cs.length>1) {
          if (cs[1].isNA0(i)) continue; else level1 = Arrays.binarySearch(_domains[1],(int)cs[1].at80(i));
          assert 0 <= level1 && level1 < _domains[1].length;
        } else {
          level1 = 0;
        }
        _counts[level1][level0]++;
      }
    }
    @Override public void reduce(Tabularize that) { Utils.add(_counts, that._counts); }
  }
}
