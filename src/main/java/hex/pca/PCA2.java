package hex.pca;

import hex.pca.PCAModel;
import hex.pca.PCAParams;
import hex.gram.Gram;

import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import Jama.Matrix;
import Jama.SingularValueDecomposition;
import water.*;
import water.Job.*;
import water.api.DocGen;
import water.fvec.*;

/**
 * Principal Components Analysis
 * This is an algorithm for dimensionality reduction of numerical data.
 * <a href = "http://en.wikipedia.org/wiki/Principal_component_analysis">PCA on Wikipedia</a>
 * @author anqi_fu
 *
 */
public class PCA2 extends ColumnsJob {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "pca";

  @API(help = "The PCA Model")
  public PCAModel pca_model;

  @API(help = "Maximum number of principal components to return.", filter = Default.class, lmin = 1, lmax = 10000)
  int max_pc = 10000;

  @API(help = "Omit components with std dev <= tol times std dev of first component.", filter = Default.class, lmin = 0, lmax = 1)
  double tolerance = 0;

  @API(help = "If true, data will be standardized on the fly when computing the model.", filter = Default.class)
  boolean standardize = true;

  /*
  public PCA2(String desc, Key dest, Frame src, int max_pc, double tol, boolean standardize) {
    description = desc;
    destination_key = dest;
    source = src;
    this.max_pc = max_pc;
    this.tolerance = tol;
    this.standardize = standardize;
  }
  */

  @Override protected void exec() {
    Frame fr = selectFrame(source);
    Vec[] vecs = fr.vecs();

    // Remove constant cols, non-numeric cols, and cols with too many NAs
    ArrayList<Integer> removeCols = new ArrayList<Integer>();
    for(int i = 0; i < vecs.length; i++) {
      if(vecs[i].min() == vecs[i].max() || vecs[i].naCnt() > vecs[i].length()*0.2 || vecs[i].domain() != null)
        removeCols.add(i);
    }
    if(!removeCols.isEmpty()) {
      int[] cols = new int[removeCols.size()];
      for(int i = 0; i < cols.length; i++)
        cols[i] = removeCols.get(i);
      fr.remove(cols);
    }

    PCATask tsk = new PCATask(this, -1, -1, standardize).doAll(fr);
    PCAModel myModel = buildModel(fr, tsk._gram.getXX());
    UKV.put(destination_key, myModel);
  }

  public PCAModel buildModel(Frame data, double[][] gram) {
    Matrix myGram = new Matrix(gram);   // X'X/n where n = num rows
    SingularValueDecomposition mySVD = myGram.svd();

    // Extract eigenvalues and eigenvectors
    // Note: Singular values ordered in weakly descending order by algorithm
    double[] Sval = mySVD.getSingularValues();
    double[][] eigVec = mySVD.getV().getArray();  // rows = features, cols = principal components
    // DKV.put(EigenvectorMatrix.makeKey(input("source"), destination_key), new EigenvectorMatrix(eigVec));

    // Compute standard deviation
    double[] sdev = new double[Sval.length];
    double totVar = 0;
    double dfcorr = data.numRows()/(data.numRows() - 1.0);
    for(int i = 0; i < Sval.length; i++) {
      // if(standardize)
        Sval[i] = dfcorr*Sval[i];   // Correct since degrees of freedom = n-1
      sdev[i] = Math.sqrt(Sval[i]);
      totVar += Sval[i];
    }

    double[] propVar = new double[Sval.length];    // Proportion of total variance
    double[] cumVar = new double[Sval.length];    // Cumulative proportion of total variance
    for(int i = 0; i < Sval.length; i++) {
      propVar[i] = Sval[i]/totVar;
      cumVar[i] = i == 0 ? propVar[0] : cumVar[i-1] + propVar[i];
    }

    Key dataKey = Key.make(input("source"));
    int ncomp = Math.min(getNumPC(sdev, tolerance), max_pc);
    PCAParams params = new PCAParams(data.names(), max_pc, tolerance, standardize);
    return new PCAModel(destination_key, dataKey, source, sdev, propVar, cumVar, eigVec, mySVD.rank(), ncomp, params);
  }

  static class reverseDouble implements Comparator<Double> {
    @Override public int compare(Double a, Double b) {
        return b.compareTo(a);
      }
    }

  public static int getNumPC(double[] sdev, double tol) {
    if(sdev == null) return 0;
    double cutoff = tol*sdev[0];
    int ind = Arrays.binarySearch(ArrayUtils.toObject(sdev), cutoff, new reverseDouble());
    return Math.abs(ind+1);
  }

  /*@Override public float progress() {
    ChunkProgress progress = UKV.get(progressKey());
    return (progress != null ? progress.progress() : 0);
  }*/

  public static class PCATask extends MRTask2<PCATask> {
    Gram _gram;
    Job _job;
    int _nums;          // Number of numerical columns
    int _cats;          // Number of categorical columns
    int[] _catOffsets;
    double[] _normSub;
    double[] _normMul;
    boolean _standardize;

    public PCATask(Job job, int nums, int cats, boolean standardize) {
      _job = job;
      _nums = nums;
      _cats = cats;
      _catOffsets = null;
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
