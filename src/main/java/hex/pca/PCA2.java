package hex.pca;

import hex.gram.Gram;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Future;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.H2O.H2OEmptyCompleter;
import water.Job.*;
import water.fvec.*;

/**
 * Principal Components Analysis
 * This is an algorithm for dimensionality reduction of numerical data.
 * <a href = "http://en.wikipedia.org/wiki/Principal_component_analysis">PCA on Wikipedia</a>
 * @author anqi_fu
 *
 */
public class PCA2 extends ColumnsJob {
  @API(help = "The PCA Model")
  public PCAModel pca_model;

  @API(help = "Maximum number of principal components to return.", filter = Default.class, lmin = 1, lmax = 10000)
  int max_pc = 10000;

  @API(help = "Omit components with std dev <= tol times std dev of first component.", filter = Default.class, lmin = 0, lmax = 1)
  double tolerance = 0;

  @API(help = "If true, data will be standardized on the fly when computing the model.", filter = Default.class)
  boolean standardize = true;

  public PCA2(String desc, Key dest, Frame src, int max_pc, double tol, boolean standardize) {
    description = desc;
    destination_key = dest;
    source = src;
    this.max_pc = max_pc;
    this.tolerance = tol;
    this.standardize = standardize;
  }

  @Override protected Response serve() {
    run(null);
    return PCAProgressPage.redirect(this, self(), dest());
  }

  public Future run(H2OCountedCompleter completer) {
    final H2OCountedCompleter fjt = new H2OEmptyCompleter();
    if(completer != null) fjt.setCompleter(completer);

    start(fjt);
    UKV.remove(dest());
    // _oldModel = new PCAModel(dest(), source, new PCAParams(max_pc, tolerance, standardize), 0, num_pc);
    Vec[] vecs = selectVecs(source);

    // Remove constant cols, non-numeric cols, and cols with too many NAs
    ArrayList<Integer> constantOrNAs = new ArrayList<Integer>();
    for(int i = 0; i < vecs.length; i++) {
      if(vecs[i].min() == vecs[i].max() || vecs[i].naCnt() > vecs[i].length()*0.2 || vecs[i].domain() != null)
        constantOrNAs.add(i);
    }
    if(!constantOrNAs.isEmpty()) {
      int[] cols = new int[constantOrNAs.size()];
      for(int i = 0; i < cols.length; i++)
        cols[i] = constantOrNAs.get(i);
        // Remove from vecs array
    }

    // final PCAScoreJob job = new PCAJob(source, dataKey, destKey, standardize);
    // PCATask tsk = new PCATask(standardize);
    // tsk.doAll(vecs);
    return fjt;
  }

  public static class PCAJob extends ChunkProgressJob {
    public PCAJob(Frame data, Key dataKey, Key destKey, boolean standardize) {
      super(standardize ? 2*data.anyVec().nChunks() : data.anyVec().nChunks());
      description = "PCAScore(" + dataKey.toString() + ")";
      destination_key = destKey;
    }

    public boolean isDone() {
      return DKV.get(self()) == null;
    }

    @Override public float progress() {
      ChunkProgress progress = UKV.get(progressKey());
      return (progress != null ? progress.progress() : 0);
    }
  }

  public class PCATask extends MRTask2<PCATask> {
    Gram _gram;
    Job _job;
    int _nums;          // Number of numerical columns
    int _cats;          // Number of categorical columns
    int[] _catOffsets;
    double[] _normSub;
    double[] _normMul;
    boolean _standardize;

    public PCATask(PCAJob job, Frame fr, boolean standardize) {
      _job = job;
      _nums = fr.numCols();
      _cats = 0;
      _normSub = null;
      _normMul = null;
      _standardize = standardize;
    }

    private int fullSize() {
      return _nums;   // TODO: Change when dealing with categoricals
    }

    @Override public void map(Chunk [] chunks) {
      _gram = new Gram(fullSize(), 0, _nums, 0);  // TODO: Update to deal with categoricals

      if(_job.cancelled()) throw new RuntimeException("Cancelled");
      final int nrows = chunks[0]._len;
      double [] nums = MemoryManager.malloc8d(_nums);
      int    [] cats = MemoryManager.malloc4(_cats);

      OUTER:
      for(int r = 0; r < nrows; r ++) {
        for(Chunk c:chunks) if(c.isNA0(r)) continue OUTER; // skip rows with NAs!
        int i = 0, ncats = 0;
        for(; i < _cats; ++i){
          int c = (int)chunks[i].at80(r);
          if(c != 0) cats[ncats++] = c + _catOffsets[i] - 1;
        }
        for(;i < chunks.length;++i)
          nums[i-_cats] = (chunks[i].at0(r) - _normSub[i-_cats])*_normMul[i-_cats];
        _gram.addRow(nums, 0, cats, 1);
      }
    }

    @Override public void reduce(PCATask tsk) {
      _gram.add(tsk._gram);
    }

    @Override public PCATask dfork(Frame fr) {
      if(_cats == -1 && _nums == -1 ){
        assert _normMul == null;
        assert _normSub == null;
        int i = 0;
        final Vec [] vecs = fr.vecs();
        final int n = vecs.length;
        while(i < n && vecs[i].isEnum())++i;
        _cats = i;
        while(i < n && !vecs[i].isEnum())++i;
        _nums = i-_cats;
        if(_cats != 0)
          throw H2O.unimpl();     // TODO: Categorical PCA not implemented yet
        _normSub = MemoryManager.malloc8d(_nums);
        _normMul = MemoryManager.malloc8d(_nums); Arrays.fill(_normMul, 1);
        if(_standardize) for(i = 0; i < _nums; ++i){
          _normSub[i] = vecs[i+_cats].mean();
          _normMul[i] = 1.0/vecs[i+_cats].sigma();
        }
        _catOffsets = MemoryManager.malloc4(_cats+1);
        int len = _catOffsets[0] = 0;
        for(i = 0; i < _cats; ++i)
          _catOffsets[i+1] = (len += vecs[i].domain().length - 1);
      }
      return super.dfork(fr);
    }
  }
}
