package hex.nb;

import hex.FrameTask.DataInfo;
import water.*;
import water.Job.ModelJob;
import water.api.DocGen;
import water.fvec.*;
import water.util.RString;
import water.util.Utils;


/**
 * Naive Bayes
 * This is an algorithm for computing the conditional a-posterior probabilities of a categorical
 * response from independent predictors using Bayes rule.
 * <a href = "http://en.wikipedia.org/wiki/Naive_Bayes_classifier">Naive Bayes on Wikipedia</a>
 * <a href = "http://cs229.stanford.edu/notes/cs229-notes2.pdf">Lecture Notes by Andrew Ng</a>
 * @author anqi_fu
 *
 */
public class NaiveBayes extends ModelJob {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "naive bayes";

  @API(help = "Laplace smoothing parameter", filter = Default.class, lmin = 0, lmax = 100000, json = true)
  public int laplace = 0;

  @Override protected JobState execImpl() {
    Frame fr = DataInfo.prepareFrame(source, response, ignored_cols, false, true);

    // TODO: Temporarily reject data with missing entries until NA handling implemented
    Vec[] vecs = fr.vecs();
    for(int i = 0; i < vecs.length; i++) {
      if(!vecs[i].isEnum() || vecs[i].naCnt() != 0) throw H2O.unimpl();
    }

    DataInfo dinfo = new DataInfo(fr, 1, false, false);
    NBTask tsk = new NBTask(this, dinfo).doAll(dinfo._adaptedFrame);
    NBModel myModel = buildModel(dinfo, tsk, laplace);
    myModel.delete_and_lock(self());
    myModel.unlock(self());
    return JobState.DONE;
  }

  /* @Override protected void init() {
    super.init();
    Vec[] vecs = selectFrame(source).vecs();
    for(int i = 0; i < vecs.length; i++) {
      if(!vecs[i].isEnum()) throw H2O.unimpl();
    }
  } */

  @Override protected Response redirect() {
    return NBProgressPage.redirect(this, self(), dest());
  }

  public static String link(Key src_key, String content) {
    RString rs = new RString("<a href='/2/NaiveBayes.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", "source");
    rs.replace("key", src_key.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public NBModel buildModel(DataInfo dinfo, NBTask tsk) {
    return buildModel(dinfo, tsk, 0);
  }

  public NBModel buildModel(DataInfo dinfo, NBTask tsk, double laplace) {
    logStart();
    double[] pprior = tsk._rescnt.clone();
    double[][][] pcond = tsk._jntcnt.clone();
    String[][] domains = dinfo._adaptedFrame.domains();

    // Probability of predictor x_j conditional on response y
    for(int col = 0; col < pcond.length; col++) {
      for(int i = 0; i < pcond[0].length; i++) {
        for(int j = 0; j < pcond[0][0].length; j++)
          pcond[col][i][j] = (pcond[col][i][j] + laplace)/(pprior[i] + domains[col].length*laplace);
      }
    }

    // A-priori probability of response y
    for(int i = 0; i < pprior.length; i++)
      pprior[i] = (pprior[i] + laplace)/(tsk._nobs + tsk._nres*laplace);
      // pprior[i] = pprior[i]/tsk._nobs;     // Note: R doesn't apply laplace smoothing to priors, even though this is textbook definition

    Key dataKey = input("source") == null ? null : Key.make(input("source"));
    return new NBModel(destination_key, dataKey, dinfo, tsk, pprior, pcond, laplace);
  }

  // TODO: Need to handle NAs in some reasonable fashion
  // R's method: For each predictor x_j, skip counting that row for p(x_j|y) calculation if x_j = NA. If response y = NA, skip counting row entirely in all calculations
  // Irene's method: Just skip all rows where any x_j = NA or y = NA. Should be more memory-efficient, but results incomparable with R.
  public static class NBTask extends MRTask2<NBTask> {
    final Job _job;
    final protected DataInfo _dinfo;
    final int _nres;              // Number of levels for the response y

    public int _nobs;             // Number of rows where y != NA
    public double[] _rescnt;      // Count of each level in the response
    public double[][][] _jntcnt;  // For each predictor, joint count of response and predictor level

    public NBTask(Job job, DataInfo dinfo) {
      _job = job;
      _dinfo = dinfo;
      _nobs = 0;

      String[][] domains = dinfo._adaptedFrame.domains();
      int ncol = dinfo._adaptedFrame.numCols();
      _nres = domains[ncol-1].length;

      _rescnt = new double[_nres];
      _jntcnt = new double[ncol-1][][];
      for(int i = 0; i < _jntcnt.length; i++)
        _jntcnt[i] = new double[_nres][domains[i].length];
    }

    @Override public void map(Chunk[] chks) {
      int res_idx = chks.length-1;
      Chunk res = chks[res_idx];

      for(int row = 0; row < chks[0]._len; row++) {
        if(res.isNA0(row)) continue;
        int rlevel = (int)res.at0(row);
        _rescnt[rlevel]++;
        _nobs++;

        for(int col = 0; col < res_idx; col++) {
          Chunk C = chks[col];
          if(C.isNA0(row)) continue;
          int plevel = (int)C.at0(row);
          _jntcnt[col][rlevel][plevel]++;
        }
      }
    }

    @Override public void reduce(NBTask nt) {
      Utils.add(_rescnt, nt._rescnt);
      for(int col = 0; col < _jntcnt.length; col++)
        _jntcnt[col] = Utils.add(_jntcnt[col], nt._jntcnt[col]);
    }
  }
}
