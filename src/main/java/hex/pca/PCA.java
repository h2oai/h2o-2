package hex.pca;

import Jama.Matrix;
import Jama.SingularValueDecomposition;
import hex.FrameTask.DataInfo;
import hex.gram.Gram.GramTask;
import water.Job.ColumnsJob;
import water.*;
import water.api.DocGen;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;
import water.util.RString;

import java.util.ArrayList;


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

  static final int MAX_COL = 5000;

  @API(help = "The PCA Model")
  public PCAModel pca_model;

  @API(help = "Maximum number of principal components to return.", filter = Default.class, lmin = 1, lmax = 5000, json=true)
  int max_pc = 5000;

  @API(help = "Omit components with std dev <= tol times std dev of first component.", filter = Default.class, lmin = 0, lmax = 1, json=true)
  double tolerance = 0;

  @API(help = "If true, data will be standardized on the fly when computing the model.", filter = Default.class, json=true)
  boolean standardize = true;

  public PCA() {}

  public PCA(String desc, Key dest, Frame src, double tolerance, boolean standardize) {
    this(desc, dest, src, 5000, tolerance, standardize);
  }

  public PCA(String desc, Key dest, Frame src, int max_pc, double tolerance, boolean standardize) {
    description = desc;
    destination_key = dest;
    source = src;
    this.max_pc = max_pc;
    this.tolerance = tolerance;
    this.standardize = standardize;
  }

  @Override public boolean toHTML(StringBuilder sb) { return makeJsonBox(sb); }

  @Override protected void execImpl() {
    Frame fr = selectFrame(source);
    Vec[] vecs = fr.vecs();

    // Remove constant cols and cols with too many NAs
    ArrayList<Integer> removeCols = new ArrayList<Integer>();
    for(int i = 0; i < vecs.length; i++) {
      if(vecs[i].min() == vecs[i].max() || vecs[i].naCnt() > vecs[i].length()*0.2)
      // if(vecs[i].min() == vecs[i].max() || vecs[i].naCnt() > vecs[i].length()*0.2 || vecs[i].domain() != null)
        removeCols.add(i);
    }
    if(!removeCols.isEmpty()) {
      int[] cols = new int[removeCols.size()];
      for(int i = 0; i < cols.length; i++)
        cols[i] = removeCols.get(i);
      fr.remove(cols);
    }
    if( fr.numCols() < 2 )
      throw new IllegalArgumentException("Need more than one column to run PCA");

    DataInfo dinfo = new DataInfo(fr, 0, false, false, standardize ? DataInfo.TransformType.STANDARDIZE : DataInfo.TransformType.NONE);
    GramTask tsk = new GramTask(self(), dinfo, false,false).doAll(dinfo._adaptedFrame);
    PCAModel myModel = buildModel(dinfo, tsk);
    myModel.delete_and_lock(self());
    myModel.unlock(self());
    remove();                   // Close/remove job
    final JobState state = UKV.<Job>get(self()).state;
    new TAtomic<PCAModel>() {
      @Override
      public PCAModel atomic(PCAModel m) {
        if (m != null) m.get_params().state = state;
        return m;
      }
    }.invoke(dest());
  }

  @Override protected void init() {
    super.init();
    int num_ecols = selectFrame(source).numExpCols();
    Log.info("Running PCA on dataset with " + num_ecols + " expanded columns in Gram matrix");
    if(num_ecols > MAX_COL)
      throw new IllegalArgumentException("Cannot process more than " + MAX_COL + " columns, taking into account expanded categoricals");
  }

  @Override protected Response redirect() {
    return PCAProgressPage.redirect(this, self(), dest());
  }

  public PCAModel buildModel(DataInfo dinfo, GramTask tsk) {
    logStart();
    Matrix myGram = new Matrix(tsk._gram.getXX());   // X'X/n where n = num rows
    SingularValueDecomposition mySVD = myGram.svd();

    // Extract eigenvalues and eigenvectors
    // Note: Singular values ordered in weakly descending order by algorithm
    double[] Sval = mySVD.getSingularValues();
    double[][] eigVec = mySVD.getV().getArray();  // rows = features, cols = principal components
    assert Sval.length == eigVec.length;
    // DKV.put(EigenvectorMatrix.makeKey(input("source"), destination_key), new EigenvectorMatrix(eigVec));

    // Compute standard deviation
    double[] sdev = new double[Sval.length];
    double totVar = 0;
    double dfcorr = dinfo._adaptedFrame.numRows()/(dinfo._adaptedFrame.numRows() - 1.0);
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

    Key dataKey = input("source") == null ? null : Key.make(input("source"));
    int ncomp = Math.min(getNumPC(sdev, tolerance), max_pc);
    return new PCAModel(this, destination_key, dataKey, dinfo, tsk, sdev, propVar, cumVar, eigVec, mySVD.rank(), ncomp);
  }

  public static int getNumPC(double[] sdev, double tol) {
    if(sdev == null) return 0;
    double cutoff = tol*sdev[0];
    for( int i=0; i<sdev.length; i++ )
      if( sdev[i] < cutoff )
        return i;
    return sdev.length;
  }

  public static String link(Key src_key, String content) {
      RString rs = new RString("<a href='/2/PCA.query?%key_param=%$key'>%content</a>");
      rs.replace("key_param", "source");
      rs.replace("key", src_key.toString());
      rs.replace("content", content);
      return rs.toString();
  }
}
