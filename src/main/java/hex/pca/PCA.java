package hex.pca;

import hex.gram.Gram.GramTask;

import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import Jama.Matrix;
import Jama.SingularValueDecomposition;
import water.*;
import water.Job.*;
import water.api.DocGen;
import water.fvec.*;
import water.util.RString;

/**
 * Principal Components Analysis
 * This is an algorithm for dimensionality reduction of numerical data.
 * <a href = "http://en.wikipedia.org/wiki/Principal_component_analysis">PCA on Wikipedia</a>
 * @author anqi_fu
 *
 */
public class PCA extends ColumnsJob {
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

  public PCA() {tolerance = 0; standardize = true;}

  public PCA(String desc, Key dest, Frame src, double tolerance, boolean standardize) {
    this(desc, dest, src, 10000, tolerance, standardize);
  }

  public PCA(String desc, Key dest, Frame src, int max_pc, double tolerance, boolean standardize) {
    description = desc;
    destination_key = dest;
    source = src;
    this.max_pc = max_pc;
    this.tolerance = tolerance;
    this.standardize = standardize;
  }

  @Override protected void exec() {
    Frame fr = selectFrame(source);
    Vec[] vecs = fr.vecs();

    // Remove constant cols and cols with too many NAs
    ArrayList<Integer> removeCols = new ArrayList<Integer>();
    for(int i = 0; i < vecs.length; i++) {
      if(vecs[i].min() == vecs[i].max() || vecs[i].naCnt() > vecs[i].length()*0.2)
        removeCols.add(i);
    }
    if(!removeCols.isEmpty()) {
      int[] cols = new int[removeCols.size()];
      for(int i = 0; i < cols.length; i++)
        cols[i] = removeCols.get(i);
      fr.remove(cols);
    }

    GramTask tsk = new GramTask(this, standardize, false).doIt(fr);
    PCAModel myModel = buildModel(fr, tsk._gram.getXX());
    UKV.put(destination_key, myModel);
  }

  @Override protected Response redirect() {
    return PCAProgressPage.redirect(this, self(), dest());
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

  public static String link(Key src_key, String content) {
      RString rs = new RString("<a href='/2/PCA.query?%key_param=%$key'>%content</a>");
      rs.replace("key_param", "source");
      rs.replace("key", src_key.toString());
      rs.replace("content", content);
      return rs.toString();
  }

  /*@Override public float progress() {
    ChunkProgress progress = UKV.get(progressKey());
    return (progress != null ? progress.progress() : 0);
  }*/
}
