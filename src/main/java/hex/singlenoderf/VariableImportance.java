package hex.singlenoderf;

import hex.ShuffleTask;
//import hex.gbm.DTree.TreeModel.CompressedTree;

import java.util.ArrayList;
//import java.util.Arrays;
import java.util.Random;

import water.AutoBuffer;
import water.Iced;
//import water.Key;
import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
//import water.util.ModelUtils;
import water.util.Utils;

/** Score given tree model and preserve errors per tree in form of votes (for classification)
 * or MSE (for regression).
 *
 * This is different from Model.score() function since the MR task
 * uses inverse loop: first over all trees and over all rows in chunk.
 */
public class VariableImportance extends MRTask2<VariableImportance> {
  /* @IN */ final private float     _rate;
//  /* @IN */       private int _trees; // FIXME: Pass only tree-keys since serialized trees are passed over wire !!!
  /* @IN */ final private int       _var;
  /* @IN */ final private boolean   _oob;
  /* @IN */ final private int       _ncols;
  /* @IN */ final private int       _nclasses;
  /* @IN */ final private boolean   _classification;
  /* @IN */ final private SpeeDRFModel _model;
  /* @IN */ final private int[]      _modelDataMap;
  /* @IN */ private Frame _data;
  /* @IN */ private int _classcol;
  /** Computed mapping of model prediction classes to confusion matrix classes */
  /* @IN */ private int[]     _model_classes_mapping;
  /** Computed mapping of data prediction classes to confusion matrix classes */
  /* @IN */ private int[]     _data_classes_mapping;
  /** Difference between model cmin and CM cmin */
  /* @IN */ private int       _cmin_model_mapping;
  /** Difference between data cmin and CM cmin */
  /* @IN */ private int       _cmin_data_mapping;
  /* @IN */ private int _cmin;


  /* @INOUT */ private final int _ntrees;
//  /* @OUT */ private long [/*ntrees*/] _votes; // Number of correct votes per tree (for classification only)
  /* @OUT */ private long [/*ntrees*/] _nrows; // Number of scored row per tree (for classification/regression)
//  /* @OUT */ private float[/*ntrees*/] _sse;   // Sum of squared errors per tree (for regression only)
  /* @OUT */ private long [/*ntrees*/] _votesSOOB;
  /* @OUT */ private long [/*ntrees*/] _votesOOB;
  /* @OUT */ private long [/*ntrees*/] _voteDiffs;
  /* @OUT */ private float _varimp;
  /* @OUT */ private float _varimpSD;
  /* @OUT */ private int[] _oobs;

  private VariableImportance(int trees, int nclasses, int ncols, float rate, int variable, SpeeDRFModel model, Frame fr, Vec resp) {
    _ncols = ncols;
    _rate = rate; _var = variable;
    _oob = true; _ntrees = trees;
    _nclasses = nclasses;
    _classification = (nclasses>1);
    _classcol = fr.numCols() - 1;
    _data = fr;
    _cmin = (int) resp.min();
    _model = model;
    _modelDataMap = _model.colMap(_data);
    init(resp);
  }

  private void init(Vec resp) {
    Vec respData  = _data.vecs()[_classcol];
    int model_min = (int) resp.min();
    int data_min = (int)respData.min();

    if (resp._domain!=null) {
      assert respData._domain != null;
      _model_classes_mapping = new int[resp._domain.length];
      _data_classes_mapping  = new int[respData._domain.length];
      // compute mapping
      alignEnumDomains(resp._domain, respData._domain, _model_classes_mapping, _data_classes_mapping);
    } else {
      assert respData._domain == null;
      _model_classes_mapping = null;
      _data_classes_mapping  = null;
      // compute mapping
      _cmin_model_mapping = model_min - Math.min(model_min, data_min);
      _cmin_data_mapping  = data_min  - Math.min(model_min, data_min);
    }
  }

  @Override public void map(Chunk[] chks) {
    _votesOOB = new long[_ntrees];
    _votesSOOB = new long[_ntrees];
    _voteDiffs = new long[_ntrees];
    _varimp = 0.f;
    _varimpSD = 0.f;
    _nrows      = new long[_ntrees];
    double[] data = new double[_ncols];
    float [] preds = new float[_nclasses+1];
    final int rows = chks[0]._len;
    int _N = _nclasses;
    int[] soob = null; // shuffled oob rows
    boolean collectOOB = true;
    final int cmin = _cmin;

    //Need the chunk of code to score over every tree...
    //Doesn't do anything with the first tree, we score time last *manually* (after looping over all da trees)
    long seedForOob = ShuffleTask.seed(chks[0].cidx());
    for( int ntree = 0; ntree < _ntrees; ntree++ ) {
      int oobcnt = 0;
      ArrayList<Integer> oob = new ArrayList<Integer>();  // oob rows
      long    treeSeed    = _model.seed(ntree);
      byte    producerId  = _model.producerId(ntree);
      int     init_row    = (int)chks[0]._start;
      long seed = Sampling.chunkSampleSeed(treeSeed, init_row);
      Random rand = Utils.getDeterRNG(seed);
      // Now for all rows, classify & vote!
      for (int row = 0; row < rows; row++) {
//        int row = r + (int)chks[0]._start;
        // ------ THIS CODE is crucial and serve to replay the same sequence
        // of random numbers as in the method Data.sampleFair()
        // Skip row used during training if OOB is computed
        float sampledItem = rand.nextFloat();
        // Bail out of broken rows with NA in class column.
        // Do not skip yet the rows with NAs in the rest of columns
        if (chks[_ncols - 1].isNA0(row)) continue;
        if (sampledItem < _model.sample) continue;
        oob.add(row);
        oobcnt++;

        // Predict with this tree - produce 0-based class index
        int prediction = (int) _model.classify0(ntree, chks, row, _modelDataMap, (short) _N, false);
        if (prediction >= _nclasses) continue; // Junk row cannot be predicted
        // Check tree miss
        int alignedPrediction = alignModelIdx(prediction);
        int alignedData = alignDataIdx((int) chks[_classcol].at80(row) - cmin);
        if (alignedPrediction == alignedData) _votesOOB[ntree]++;
      }

        _oobs = new int[oob.size()];
        for (int i = 0; i < oob.size(); ++i) _oobs[i] = oob.get(i);

      //score on shuffled data...
      if (soob==null || soob.length < oobcnt) soob = new int[oobcnt];
      Utils.shuffleArray(_oobs, oobcnt, soob, seedForOob, 0); // Shuffle array and copy results into <code>soob</code>
      for(int j = 0; j < oobcnt; j++) {
        int row = _oobs[j];
        // Do scoring:
        // - prepare a row data
        for (int i=0;i<chks.length - 1;i++) {
          data[i] = chks[i].at0(row); // 1+i - one free is expected by prediction
        }
        // - permute variable
        if (_var>=0) data[_var] = chks[_var].at0(soob[j]);
        else assert false;
        // - score data
        // - score only the tree
        int prediction = (int) Tree.classify(new AutoBuffer(_model.tree(ntree)), data, (double)_N, false); //.classify0(ntree, _data, chks, row, _modelDataMap, numClasses );
        if( prediction >= _nclasses ) continue;
        int pred = alignModelIdx(prediction);
        int actu = alignDataIdx((int) chks[_classcol].at80(_oobs[j]) - cmin);
        if (pred == actu) _votesSOOB[ntree]++;
        _nrows[ntree]++;
      }
    }
  }

  @Override public void reduce( VariableImportance t ) {
    Utils.add(_votesOOB, t._votesOOB);
    Utils.add(_votesSOOB, t._votesSOOB);
    Utils.add(_nrows, t._nrows);
  }

  /** Transforms 0-based class produced by model to CF zero-based */
  private int alignModelIdx(int modelClazz) {
    if (_model_classes_mapping!=null)
      return _model_classes_mapping[modelClazz];
    else
      return modelClazz + _cmin_model_mapping;
  }

  /** Transforms 0-based class from input data to CF zero-based */
  private int alignDataIdx(int dataClazz) {
    if (_data_classes_mapping!=null)
      return _data_classes_mapping[dataClazz];
    else
      return dataClazz + _cmin_data_mapping;
  }

  public static int alignEnumDomains(final String[] modelDomain, final String[] dataDomain, int[] modelMapping, int[] dataMapping) {
    assert modelMapping!=null && modelMapping.length == modelDomain.length;
    assert dataMapping!=null && dataMapping.length == dataDomain.length;

    int idx = 0, idxM = 0, idxD = 0;
    while(idxM!=modelDomain.length || idxD!=dataDomain.length) {
      if (idxM==modelDomain.length) { dataMapping[idxD++] = idx++; continue;  }
      if (idxD==dataDomain.length)  { modelMapping[idxM++] = idx++; continue; }
      int c = modelDomain[idxM].compareTo(dataDomain[idxD]);
      if (c < 0) {
        modelMapping[idxM] = idx;
        idxM++;
      } else if (c > 0) {
        dataMapping[idxD] = idx;
        idxD++;
      } else { // strings are identical
        modelMapping[idxM] = idx;
        dataMapping[idxD] = idx;
        idxM++; idxD++;
      }
      idx++;
    }
    return idx;
  }

  public TreeVotes[] resultVotes() {

    return new TreeVotes[]{new TreeVotes(_votesOOB, _nrows, _ntrees), new TreeVotes(_votesSOOB, _nrows, _ntrees)};
  }
//  public TreeSSE   resultSSE  () { return new TreeSSE  (_sse,   _nrows, _ntrees); }
  /* This is a copy of score0 method from DTree:615 */
//  private void score0(double data[], float preds[], CompressedTree[] ts) {
//    for( int c=0; c<ts.length; c++ )
//      if( ts[c] != null )
//        preds[ts.length==1?0:c+1] += ts[c].score(data);
//  }

//  private Chunk chk_resp( Chunk chks[] ) { return chks[_ncols]; }
//
//  private Random rngForTree(CompressedTree[] ts, int cidx) {
//    return _oob ? ts[0].rngForChunk(cidx) : new DummyRandom(); // k-class set of trees shares the same random number
//  }

  /* For bulk scoring
  public static TreeVotes collect(TreeModel tmodel, Frame f, int ncols, float rate, int variable) {
    CompressedTree[][] trees = new CompressedTree[tmodel.ntrees()][];
    for (int tidx = 0; tidx < tmodel.ntrees(); tidx++) trees[tidx] = tmodel.ctree(tidx);
    return new TreeVotesCollector(trees, tmodel.nclasses(), ncols, rate, variable).doAll(f).result();
  }*/

//  VariableImportance(int trees, int nclasses, int ncols, float rate, int variable, SpeeDRFModel model)

  public static TreeVotes[] collectVotes(int trees, int nclasses, Frame f, int ncols, float rate, int variable, SpeeDRFModel model, Vec resp) {
    return new VariableImportance(trees, nclasses, ncols, rate, variable, model, f, resp).doAll(f).resultVotes();
  }

//  public static TreeSSE collectSSE(CompressedTree[/*nclass || 1 for regression*/] tree, int nclasses, Frame f, int ncols, float rate, int variable) {
//    return new TreeMeasuresCollector(new CompressedTree[][] {tree}, nclasses, ncols, rate, variable).doAll(f).resultSSE();
//  }

//  private static final class DummyRandom extends Random {
//    @Override public final float nextFloat() { return 1.0f; }
//  }

  /** A simple holder for set of different tree measurements. */
  public static abstract class TreeMeasures<T extends TreeMeasures> extends Iced {
    /** Actual number of trees which votes are stored in this object */
    protected int _ntrees;
    /** Number of processed row per tree. */
    protected long[/*ntrees*/]   _nrows;

    public TreeMeasures(int initialCapacity) { _nrows = new long[initialCapacity]; }
    public TreeMeasures(long[] nrows, int ntrees) { _nrows = nrows; _ntrees = ntrees;}
    /** Returns number of rows which were used during voting per individual tree. */
    public final long[] nrows() { return _nrows; }
    /** Returns number of voting predictors */
    public final int    npredictors() { return _ntrees; }
    /** Returns a list of accuracies per tree. */
    public abstract double accuracy(int tidx);
    public final double[] accuracy() {
      double[] r = new double[_ntrees];
      // Average of all trees
      for (int tidx=0; tidx<_ntrees; tidx++) r[tidx] = accuracy(tidx);
      return r;
    }
    /** Compute variable importance with respect to given votes.
     * The given {@link T} object represents correct votes.
     * This object represents votes over shuffled data.
     *
     * @param right individual tree measurements performed over not shuffled data.
     * @return computed importance and standard deviation
     */
    public abstract double[/*2*/] imp(T right);

    public abstract T append(T t);
  }

  /** A class holding tree votes. */
  public static class TreeVotes extends TreeMeasures<TreeVotes> {
    /** Number of correct votes per tree */
    private long[/*ntrees*/]   _votes;

    public TreeVotes(int initialCapacity) {
      super(initialCapacity);
      _votes = new long[initialCapacity];
    }
    public TreeVotes(long[] votes, long[] nrows, int ntrees) {
      super(nrows, ntrees);
      _votes = votes;
    }
    /** Returns number of positive votes per tree. */
    public final long[] votes() { return _votes; }

    /** Returns accuracy per individual trees. */
    @Override public final double accuracy(int tidx)  {
      assert tidx < _nrows.length && tidx < _votes.length;
      return ((double) _votes[tidx]) / _nrows[tidx];
    }

    /** Compute variable importance with respect to given votes.
     * The given {@link TreeVotes} object represents correct votes.
     * This object represents votes over shuffled data.
     *
     * @param right individual tree voters performed over not shuffled data.
     * @return computed importance and standard deviation
     */
    @Override public final double[/*2*/] imp(TreeVotes right) {
      assert npredictors() == right.npredictors();
      int ntrees = npredictors();
      double imp = 0;
      double sd  = 0;
      // Over all trees
      for (int tidx = 0; tidx < ntrees; tidx++) {
        assert right.nrows()[tidx] == nrows()[tidx];
        double delta = ((double) (right.votes()[tidx] - votes()[tidx])) / nrows()[tidx];
        imp += delta;
        sd  += delta * delta;
      }
      double av = imp / ntrees;
      double csd = Math.sqrt( (sd/ntrees - av*av) / ntrees );
      return new double[] { av, csd};
    }

    /** Append a tree votes to a list of trees. */
    public TreeVotes append(long rightVotes, long allRows) {
      assert _votes.length > _ntrees && _votes.length == _nrows.length : "TreeVotes inconsistency!";
      _votes[_ntrees] = rightVotes;
      _nrows[_ntrees] = allRows;
      _ntrees++;
      return this;
    }

    @Override public TreeVotes append(final TreeVotes tv) {
      for (int i=0; i<tv.npredictors(); i++)
        append(tv._votes[i], tv._nrows[i]);
      return this;
    }
  }

  /** A simple holder serving SSE per tree. */
//  public static class TreeSSE extends TreeMeasures<TreeSSE> {
//    /** SSE per tree */
//    private float[/*ntrees*/]   _sse;
//
//    public TreeSSE(int initialCapacity) {
//      super(initialCapacity);
//      _sse = new float[initialCapacity];
//    }
//    public TreeSSE(float[] sse, long[] nrows, int ntrees) {
//      super(nrows, ntrees);
//      _sse = sse;
//    }
//    @Override public double accuracy(int tidx) {
//      return _sse[tidx] / _nrows[tidx];
//    }
//    @Override public double[] imp(TreeSSE right) {
//      assert npredictors() == right.npredictors();
//      int ntrees = npredictors();
//      double imp = 0;
//      double sd  = 0;
//      // Over all trees
//      for (int tidx = 0; tidx < ntrees; tidx++) {
//        assert right.nrows()[tidx] == nrows()[tidx]; // check that we iterate over same OOB rows
//        double delta = ((double) (_sse[tidx] - right._sse[tidx])) / nrows()[tidx];
//        imp += delta;
//        sd  += delta * delta;
//      }
//      double av = imp / ntrees;
//      double csd = Math.sqrt( (sd/ntrees - av*av) / ntrees );
//      return new double[] { av, csd };
//    }
//    @Override public TreeSSE append(TreeSSE t) {
//      for (int i=0; i<t.npredictors(); i++)
//        append(t._sse[i], t._nrows[i]);
//      return this;
//    }
//    /** Append a tree sse to a list of trees. */
//    public TreeSSE append(float sse, long allRows) {
//      assert _sse.length > _ntrees && _sse.length == _nrows.length : "TreeVotes inconsistency!";
//      _sse  [_ntrees] = sse;
//      _nrows[_ntrees] = allRows;
//      _ntrees++;
//      return this;
//    }
//  }

  public static TreeVotes asVotes(TreeMeasures tm) { return (TreeVotes) tm; }
//  public static TreeSSE   asSSE  (TreeMeasures tm) { return (TreeSSE)   tm; }
}
