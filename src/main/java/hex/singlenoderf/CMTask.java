package hex.singlenoderf;

import water.*;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;
import water.util.Log.Tag.Sys;
import water.util.ModelUtils;
import water.util.Utils;

import java.util.Arrays;
import java.util.Random;
import hex.VarImp;

/**
 * Confusion Matrix. Incrementally computes a Confusion Matrix for a forest
 * of Trees, vs a given input dataset. The set of Trees can grow over time. Each
 * request from the Confusion compute on any new trees (if any), and report a
 * matrix. Cheap if all trees already computed.
 */
public class CMTask extends MRTask2<CMTask> {
  public double[] _classWt;
  public boolean _computeOOB;
  public int _treesUsed;
  public Key _modelKey;
  public Key _datakey;
  public int _classcol;
  public CM _matrix;
  public float _sum; //sum of squares Sum_ti((f_ti - delta(oti,i))^2) AKA brier score ~ classification mse
  public CM[] _localMatrices;
  public long[] _errorsPerTree;
  public SpeeDRFModel _model;
  public int[] _modelDataMap;
  public Frame _data;
  public int  _N;
  public long _cms[][][];
  public VarImp _varimp;
  public int[] _oobs;
  public Key[][] _remoteChunksKeys;
  public float _ss; // Sum of squares
  public int _rowcnt; // Rows used in scoring for regression
  public boolean _score_new_tree_only;

  /** Data to replay the sampling algorithm */
  private long[]     _chunk_row_mapping;
  /** Number of rows at each node */
  private int[]     _rowsPerNode;
  /** Computed mapping of model prediction classes to confusion matrix classes */
  private int[]     _model_classes_mapping;
  /** Computed mapping of data prediction classes to confusion matrix classes */
  private int[]     _data_classes_mapping;
  /** Difference between model cmin and CM cmin */
  private int       _cmin_model_mapping;
  /** Difference between data cmin and CM cmin */
  private int       _cmin_data_mapping;

  transient private Random _rand;

  /** Confusion matrix
   * @param model the ensemble used to classify
   */
  private CMTask(SpeeDRFModel model, int treesToUse, boolean computeOOB, Frame fr, Vec resp) {
    _modelKey   = model._key;
    _datakey    = model._dataKey;
    _classcol   = fr.numCols() - 1; //model.test_frame == null ?  (model.fr.numCols() - 1) : (model.test_frame.numCols() - 1);
    _treesUsed  = treesToUse;
    _computeOOB = computeOOB;
    _model = model;
    _varimp = null;
    _ss = 0.f;
    _data = fr;
    shared_init(resp);
  }

  public static CMTask scoreTask(SpeeDRFModel model, int treesToUse, boolean computeOOB, Frame fr, Vec resp) {
    CMTask tsk = new CMTask(model, treesToUse, computeOOB, fr, resp);
    tsk.doAll(fr);
    return tsk;
  }

  /** Shared init: pre-compute local data for new Confusions, for remote Confusions*/
  private void shared_init(Vec resp) {
    /* For reproducibility we can control the randomness in the computation of the
   confusion matrix. The default seed when deserializing is 42. */
//    _data = _model.test_frame == null ? _model.fr : _model.test_frame;
    if (_model.validation) _computeOOB = false;
    _modelDataMap = _model.colMap(_data);
    assert !_computeOOB || _model._dataKey.equals(_datakey) : !_computeOOB + " || " + _model._dataKey + " equals " + _datakey;
    Vec respModel = resp;
    Vec respData  = _data.vecs()[_classcol];
    int model_max = (int)respModel.max();
    int model_min = (int)respModel.min();
    int data_max = (int)respData.max();
    int data_min = (int)respData.min();

    if (respModel._domain!=null) {
      assert respData._domain != null;
      _model_classes_mapping = new int[respModel._domain.length];
      _data_classes_mapping  = new int[respData._domain.length];
      // compute mapping
      _N = alignEnumDomains(respModel._domain, respData._domain, _model_classes_mapping, _data_classes_mapping);
    } else {
      assert respData._domain == null;
      _model_classes_mapping = null;
      _data_classes_mapping  = null;
      // compute mapping
      _cmin_model_mapping = model_min - Math.min(model_min, data_min);
      _cmin_data_mapping  = data_min  - Math.min(model_min, data_min);
      _N = Math.max(model_max, data_max) - Math.min(model_min, data_min) + 1;
    }
    assert _N > 0; // You know...it is good to be sure
    init();
  }

  public void init() {

    // Make a mapping from chunk# to row# just for chunks on this node

    // First compute the number of chunks homed to this node
    int total_home = 0;
    for (int i = 0; i < _data.anyVec().nChunks(); ++i) {
      if (_data.anyVec().chunkKey(i).home()) {
        total_home++;
      }
    }

    // Now generate the mapping
    _chunk_row_mapping = new long[total_home];
    int off=0;
    int cidx=0;
    for (int i = 0; i < _data.anyVec().nChunks(); ++i) {
      if (_data.anyVec().chunkKey(i).home()) {
        _chunk_row_mapping[cidx++] = _data.anyVec().chunk2StartElem(i);
      }
    }

    // Initialize number of rows per node
    _rowsPerNode = new int[H2O.CLOUD.size()];
    long chunksCount = _data.anyVec().nChunks();
    for(int ci=0; ci<chunksCount; ci++) {
      Key cKey = _data.anyVec().chunkKey(ci);
      _rowsPerNode[cKey.home_node().index()] += _data.anyVec().chunkLen(ci);
    }

    _remoteChunksKeys = new Key[H2O.CLOUD.size()][];
    int[] _remoteChunksCounter = new int[H2O.CLOUD.size()];

    for (int i = 0; i < _data.anyVec().nChunks(); ++i) {
      _remoteChunksCounter[_data.anyVec().chunkKey(i).home(H2O.CLOUD)]++;
    }

    for (int i = 0; i < H2O.CLOUD.size(); ++i) _remoteChunksKeys[i] = new Key[_remoteChunksCounter[i]];

    int[] cnter = new int[H2O.CLOUD.size()];
    for (int i = 0; i < _data.anyVec().nChunks(); ++i) {
      int node_idx = _data.anyVec().chunkKey(i).home(H2O.CLOUD);
      _remoteChunksKeys[node_idx][cnter[node_idx]++] = _data.anyVec().chunkKey(i);
    }
  }

  private int producerRemoteRows(byte treeProducerID, Key chunkKey) {
    Key[] remoteCKeys = _remoteChunksKeys[treeProducerID];
    int off = 0;
    for (int i=0; i<remoteCKeys.length; i++) {
      if (chunkKey.equals(remoteCKeys[i])) return off;
      off += _data.anyVec().chunkLen(i);
    }
    return off;
  }

  @Override public void map(Chunk[] chks) {
    final int rows = chks[0]._len;
    final int cmin       = _model.resp_min;
    short     numClasses = (short)_model.classes();
    _cms = new long[ModelUtils.DEFAULT_THRESHOLDS.length][2][2];

    // Votes: we vote each tree on each row, holding on to the votes until the end
    int[][] votes = new int[rows][_N];
    int[][] localVotes = _computeOOB ? new int[rows][_N] : null;
    // Errors per tree
    _errorsPerTree = new long[_model.treeCount()];
    // Replay the Data.java's "sample_fair" sampling algorithm to exclude data
    // we trained on during voting.
    for( int ntree = 0; ntree < _model.treeCount(); ntree++ ) {
      if (_score_new_tree_only) ntree = _model.treeCount() - 1;
      long    treeSeed    = _model.seed(ntree);
      byte    producerId  = _model.producerId(ntree);
      int     init_row    =   (int)chks[0]._start;
      boolean isLocalTree = _computeOOB && isLocalTree(producerId); // tree is local
      boolean isRemote    = true;
      for (long a_chunk_row_mapping : _chunk_row_mapping) {
        if (chks[0]._start == a_chunk_row_mapping) {
          isRemote = false;
          break;
        }
      }
      boolean isRemoteTreeChunk = _computeOOB && isRemote; // this is chunk which was used for construction the tree by another node
      if (isRemoteTreeChunk) init_row = _rowsPerNode[producerId] + (int)chks[0]._start + producerRemoteRows(producerId, chks[0]._vec.chunkKey(chks[0].cidx()));
      /* NOTE: Before changing used generator think about which kind of random generator you need:
       * if always deterministic or non-deterministic version - see hex.rf.Utils.get{Deter}RNG */
      // DEBUG: if( _computeOOB && (isLocalTree || isRemoteTreeChunk)) System.err.println(treeSeed + " : " + init_row + " (CM) " + isRemoteTreeChunk);
      long seed = Sampling.chunkSampleSeed(treeSeed, init_row);
      Random rand = Utils.getDeterRNG(seed);
      // Now for all rows, classify & vote!
      ROWS: for( int row = 0; row < rows; row++ ) {
        // ------ THIS CODE is crucial and serve to replay the same sequence
        // of random numbers as in the method Data.sampleFair()
        // Skip row used during training if OOB is computed
        float sampledItem = rand.nextFloat();
        // Bail out of broken rows with NA in class column.
        // Do not skip yet the rows with NAs in the rest of columns
        if( chks[_classcol].isNA0(row)) continue;

        if( _computeOOB && (isLocalTree || isRemoteTreeChunk) ) { // if OOBEE is computed then we need to take into account utilized sampling strategy
          if (sampledItem < _model.sample) continue;
        }
        // --- END OF CRUCIAL CODE ---

        // Predict with this tree - produce 0-based class index
        if (!_model.regression) {
          int prediction = (int)_model.classify0(ntree, chks, row, _modelDataMap, numClasses, false /*Not regression*/);
          if( prediction >= numClasses ) continue; // Junk row cannot be predicted
          // Check tree miss
          int alignedPrediction = alignModelIdx(prediction);
          int alignedData       = alignDataIdx((int) chks[_classcol].at80(row) - cmin);
          if (alignedPrediction != alignedData) {
            _errorsPerTree[ntree]++;
          }
          votes[row][alignedPrediction]++; // Vote the row
//          if (isLocalTree) localVotes[row][alignedPrediction]++; // Vote
        } else {
          float pred = _model.classify0(ntree, chks, row, _modelDataMap, (short) 0, true /*regression*/);
          float actual = chks[_classcol].at80(row);
          float delta = actual - pred;
          _ss += delta * delta;
          _rowcnt++;
        }
      }
    }
    if(!_model.regression) {
      // Assemble the votes-per-class into predictions & score each row
      _matrix = computeCM(votes, chks, false /*Do the _cms once*/, _model.get_params().balance_classes); // Make a confusion matrix for this chunk
      if (localVotes!=null) {
        _localMatrices = new CM[H2O.CLOUD.size()];
        _localMatrices[H2O.SELF.index()] = computeCM(localVotes, chks, true /*Don't compute the _cms again!*/, _model.get_params().balance_classes);
      }
    }
  }

  public static float[] computeVarImpSD(long[][] vote_diffs) {
    float[] res = new float[vote_diffs.length];
    for (int var = 0; var < vote_diffs.length; ++var) {
      float mean_diffs = 0.f;
      float r = 0.f;
      for (long d: vote_diffs[var]) mean_diffs += (float) d / (float) vote_diffs.length;
      for (long d: vote_diffs[var]) {
        r += (d - mean_diffs) * (d - mean_diffs);
      }
      r *= 1.f / (float)vote_diffs[var].length;
      res[var] = (float) Math.sqrt(r);
    }
    return res;
  }

  /** Returns true if tree was produced by this node.
   * Note: chunkKey is key stored at this local node */
  private boolean isLocalTree(byte treeProducerId) {
    assert _computeOOB : "Calling this method makes sense only for oobee";
    int idx  = H2O.SELF.index();
    return idx == treeProducerId;
  }

  /** Reduction combines the confusion matrices. */
  @Override public void reduce(CMTask drt) {
    if (!_model.regression) {
      if (_matrix == null) {
        _matrix = drt._matrix;
      } else {
        _matrix = _matrix.add(drt._matrix);
      }
      _sum += drt._sum;
      // Reduce tree errors
      long[] ept1 = _errorsPerTree;
      long[] ept2 = drt._errorsPerTree;
      if (ept1 == null) _errorsPerTree = ept2;
      else if (ept2 != null) {
        if (ept1.length < ept2.length) ept1 = Arrays.copyOf(ept1, ept2.length);
        for (int i = 0; i < ept2.length; i++) ept1[i] += ept2[i];
      }

      if (_cms!=null)
        for (int i = 0; i < _cms.length; i++) Utils.add(_cms[i], drt._cms[i]);

      if (_oobs != null)
        for (int i = 0; i < _oobs.length; ++i) _oobs[i] += drt._oobs[i];
    } else {
      _ss += drt._ss;
      _rowcnt += drt._rowcnt;
    }
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

  /** Merge model and data predictor domain to produce domain for CM.
   * The domain is expected to be ordered and containing unique values. */
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

  public static String[] domain(final Vec modelCol, final Vec dataCol) {
    int[] modelEnumMapping = null;
    int[] dataEnumMapping  = null;
    int N;

    if (modelCol._domain!=null) {
      assert dataCol._domain != null;
      modelEnumMapping = new int[modelCol._domain.length];
      dataEnumMapping  = new int[dataCol._domain.length];
      N = alignEnumDomains(modelCol._domain, dataCol._domain, modelEnumMapping, dataEnumMapping);
    } else {
      assert dataCol._domain == null;
      N = (int) (Math.max(modelCol.max(), dataCol.max()) - Math.min(modelCol.min(), dataCol.min()) + 1);
    }
    return domain(N, modelCol, dataCol, modelEnumMapping, dataEnumMapping);
  }

  public static String[] domain(int N, final Vec modelCol, final Vec dataCol, int[] modelEnumMapping, int[] dataEnumMapping) {
    String[] result      = new String[N];
    String[] modelDomain = modelCol._domain;
    String[] dataDomain  = dataCol._domain;

    if (modelDomain!=null) {
      assert dataDomain!=null;
      assert modelEnumMapping!=null && modelEnumMapping.length == modelDomain.length;
      assert dataEnumMapping!=null && dataEnumMapping.length == dataDomain.length;

      for (int i = 0; i < modelDomain.length; i++) result[modelEnumMapping[i]] = modelDomain[i];
      for (int i = 0; i < dataDomain.length; i++)  result[dataEnumMapping [i]] = dataDomain[i];
    } else {
      assert dataDomain==null;
      int dmin = (int) Math.min(modelCol.min(), dataCol.min());
      int dmax = (int) Math.max(modelCol.max(), dataCol.max());
      for (int i = dmin; i <= dmax; i++) result[i-dmin] = String.valueOf(i);
    }
    return result;
  }

  /** Compute confusion matrix domain based on model and data key. */
  public String[] domain(Vec modelResp) {
    return domain(_N, modelResp, _data.vecs()[_classcol], _model_classes_mapping, _data_classes_mapping);
  }

  /** Return number of classes - in fact dimension of CM. */
  public final int dimension() { return _N; }

  /** Confusion matrix representation. */
  static class CM extends Iced {
    /** The Confusion Matrix - a NxN matrix of [actual] -vs- [predicted] classes,
     referenced as _matrix[actual][predicted]. Each row in the dataset is
     voted on by all trees, and the majority vote is the predicted class for
     the row. Each row thus gets 1 entry in the matrix.*/
    protected long _matrix[][];
    /** Number of mistaken assignments. */
    protected long _errors;
    /** Number of rows used for building the matrix.*/
    protected long _rows;
    /** Number of skipped rows. Rows can contain bad data, or can be skipped by selecting only out-of-back rows */
    protected long _skippedRows;
    /** Domain - names of columns and rows */
    public float classError() { return _errors / (float) _rows; }
    /** Return number of rows used for CM computation */
    public long  rows()       { return _rows; }
    /** Return number of skipped rows during CM computation
     *  The number includes in-bag rows if oobee is used. */
    public long  skippedRows(){ return _skippedRows; }
    /** Add a confusion matrix. */
    public CM add(final CM cm) {
      if (cm!=null) {
        if( _matrix == null ) _matrix = cm._matrix;  // Take other work straight-up
        else Utils.add(_matrix,cm._matrix);
        _rows    += cm._rows;
        _errors  += cm._errors;
        _skippedRows += cm._skippedRows;
      }
      return this;
    }
    /** Text form of the confusion matrix */
    @Override public String toString() {
      if( _matrix == null ) return "no trees";
      int N = _matrix.length;
      final int K = N + 1;
      double[] e2c = new double[N];
      for( int i = 0; i < N; i++ ) {
        long err = -_matrix[i][i];
        for( int j = 0; j < N; j++ )   err += _matrix[i][j];
        e2c[i] = Math.round((err / (double) (err + _matrix[i][i])) * 100) / (double) 100;
      }
      String[][] cms = new String[K][K + 1];
      cms[0][0] = "";
      for( int i = 1; i < K; i++ ) cms[0][i] = "" + (i - 1);
      cms[0][K] = "err/class";
      for( int j = 1; j < K; j++ ) cms[j][0] = "" + (j - 1);
      for( int j = 1; j < K; j++ ) cms[j][K] = "" + e2c[j - 1];
      for( int i = 1; i < K; i++ )
        for( int j = 1; j < K; j++ ) cms[j][i] = "" + _matrix[j - 1][i - 1];
      int maxlen = 0;
      for( int i = 0; i < K; i++ )
        for( int j = 0; j < K + 1; j++ ) maxlen = Math.max(maxlen, cms[i][j].length());
      for( int i = 0; i < K; i++ )
        for( int j = 0; j < K + 1; j++ ) cms[i][j] = pad(cms[i][j], maxlen);
      String s = "";
      for( int i = 0; i < K; i++ ) {
        for( int j = 0; j < K + 1; j++ ) s += cms[i][j];
        s += "\n";
      }
      return s;
    }
    /** Pad a string with spaces. */
    private String pad(String s, int l){ String p=""; for(int i=0; i<l-s.length();i++)p+=" "; return " "+p+s; }
  }

  public static class CMFinal extends CM {
    final protected Key      _SpeeDRFModelKey;
    final protected String[] _domain;
    final protected long  [] _errorsPerTree;
    final protected boolean  _computedOOB;
    final protected long[][][] _cms;
    protected boolean        _valid;
    final protected float _sum;

    private CMFinal() {
      _valid         = false;
      _SpeeDRFModelKey = null;
      _domain        = null;
      _errorsPerTree = null;
      _computedOOB   = false;
      _sum = 0.f;
      _cms = null;
    }

    private CMFinal(CM cm, Key SpeeDRFModelKey, String[] domain, long[] errorsPerTree, boolean computedOOB, boolean valid, float sum, long[][][] cms) {
      _matrix = cm._matrix;
      _errors = cm._errors;
      _rows = cm._rows;
      _skippedRows = cm._skippedRows;
      _SpeeDRFModelKey    = SpeeDRFModelKey;
      _domain        = domain;
      _errorsPerTree = errorsPerTree;
      _computedOOB   = computedOOB;
      _valid         = valid;
      _sum = sum;
      _cms = cms;
    }

    /** Make non-valid confusion matrix */
    public static CMFinal make() {
      return new CMFinal();
    }

    /** Create a new confusion matrix. */
    public static CMFinal make(CM cm, SpeeDRFModel model, String[] domain, long[] errorsPerTree, boolean computedOOB, float sum, long[][][] cms) {
      return new CMFinal(cm, model._key, domain, errorsPerTree, computedOOB, true, sum, cms);
    }

    public String[] domain() { return _domain; }
    public int      dimension() { return _matrix.length; }
    public long     matrix(int i, int j) { return _matrix[i][j]; }
    public boolean  valid() { return _valid; }
    public float    mse() { return _sum / (float) _rows; }

    /** Output information about this RF. */
    public final void report() {
      double err = classError();
      assert _valid : "Trying to report status of invalid CM!";

      SpeeDRFModel model = UKV.get(_SpeeDRFModelKey);
      String s =
              "              Type of random forest: classification\n"
                      + "                    Number of trees: " + model.size() + "\n"
                      + "No of variables tried at each split: " + model.mtry + "\n"
                      + "              Estimate of err. rate: " + Math.round(err * 10000) / 100 + "%  (" + err + ")\n"
                      + "                              OOBEE: " + (_computedOOB ? "YES (sampling rate: "+model.sample*100+"%)" : "NO")+ "\n"
                      + "                   Confusion matrix:\n"
                      + toString() + "\n"
                      + "                          CM domain: " + Arrays.toString(_domain) + "\n"
                      + "          Avg tree depth (min, max): " + model.depth() + "\n"
                      + "         Avg tree leaves (min, max): " + model.leaves() + "\n"
                      + "                Validated on (rows): " + rows() + "\n"
                      + "     Rows skipped during validation: " + skippedRows() + "\n"
                      + "  Mispredictions per tree (in rows): " + Arrays.toString(_errorsPerTree)+"\n";
      Log.info(Sys.RANDF,s);
    }

    /**
     * Reports size of dataset and computed classification error.
     */
    public final void report(StringBuilder sb) {
      double err = _errors / (double) _rows;
      sb.append(_rows).append(',');
      sb.append(err).append(',');
    }
  }

  /** Compute the sum of squared errors */
  static float doSSECalc(int[] votes, float[] preds, int cclass) {
    float err;

    // Get the total number of votes for the row
    float sum = doSum(votes);

    // No votes for the row
    if (sum == 0) {
      err = 1f - (1f / (votes.length - 0f));
      return err * err;
    }

    err = Float.isInfinite(sum)
            ? (Float.isInfinite(preds[cclass + 1]) ? 0f : 1f)
            : 1f - preds[cclass + 1] / sum;

    return err * err;
  }

  static float doSum(int[] votes) {
    float sum = 0f;
    for (int v : votes)
      sum += v;
    return sum;
  }

  static float[] toProbs(float[] preds, float s ) {
    for (int i = 1; i < preds.length; ++i) {
      preds[i] /= s;
    }
    return preds;
  }

  /** Produce confusion matrix from given votes. */
  final CM computeCM(int[/**/][/**/] votes, Chunk[] chks, boolean local, boolean balance) {
    CM cm = new CM();
    int rows = votes.length;
    int validation_rows = 0;
    int cmin = (int) _data.vecs()[_classcol].min();

    // Assemble the votes-per-class into predictions & score each row

    // Make an empty confusion matrix for this chunk
    cm._matrix = new long[_N][_N];
    float preds[] = new float[_N+1];

    float num_trees = _errorsPerTree.length;

    // Loop over the rows
    for( int row = 0; row < rows; row++ ) {

      // Skip rows with missing response values
      if (chks[_classcol].isNA0(row)) continue;

      // The class votes for the i-th row
      int[] vi = votes[row];

      // Fill the predictions with the vote counts, keeping the 0th index unchanged
      for( int v=0; v<_N; v++ ) preds[v+1] = vi[v];

      float s = doSum(vi);
      if (s == 0) {
        cm._skippedRows++;
        continue;
      }

      int result;
      if (balance) {
        float[] scored = toProbs(preds.clone(), doSum(vi));
        double probsum=0;
        for( int c=1; c<scored.length; c++ ) {
          final double original_fraction = _model.priordist()[c-1];
          assert(original_fraction > 0) : "original fraction should be > 0, but is " + original_fraction + ": not using enough training data?";
          final double oversampled_fraction = _model.modeldist()[c-1];
          assert(oversampled_fraction > 0) : "oversampled fraction should be > 0, but is " + oversampled_fraction + ": not using enough training data?";
          assert(!Double.isNaN(scored[c]));
          scored[c] *= original_fraction / oversampled_fraction;
          probsum += scored[c];
        }
        for (int i=1;i<scored.length;++i) scored[i] /= probsum;
        result = ModelUtils.getPrediction(scored, row);
      } else {
        // `result` is the class with the most votes, accounting for ties in the shared logic in ModelUtils
        result = ModelUtils.getPrediction(preds, row);
      }

      // Get the class value from the response column for the current row
      int cclass = alignDataIdx((int) chks[_classcol].at80(row) - cmin);
      assert 0 <= cclass && cclass < _N : ("cclass " + cclass + " < " + _N);

      // Ignore rows with zero votes, but still update the sum of squared errors
      if( vi[result]==0 ) {
        cm._skippedRows++;
        if (!local) _sum += doSSECalc(vi, preds, cclass);
        continue;
      }

      // Update the confusion matrix
      cm._matrix[cclass][result]++;
      if( result != cclass ) cm._errors++;
      validation_rows++;

      // Update the sum of squared errors
      if (!local) _sum += doSSECalc(vi, preds, cclass);
      float sum = doSum(vi);

      // Binomial classification -> compute AUC, draw ROC
      if(_N == 2 && !local) {
        float snd = preds[2] / sum;
        for(int i = 0; i < ModelUtils.DEFAULT_THRESHOLDS.length; i++) {
          int p = snd >= ModelUtils.DEFAULT_THRESHOLDS[i] ? 1 : 0;
          _cms[i][cclass][p]++; // Increase matrix
        }
      }
    }

    // End of loop over rows, return confusion matrix
    cm._rows=validation_rows;
    return cm;
  }

  public static class MSETask extends MRTask2<MSETask> {
    //M
    double _ss;

    public static double doTask(Frame fr) {
      MSETask tsk = new MSETask();
      tsk.doAll(fr);
      return tsk._ss / (double) fr.numRows();
    }

    @Override public void map(Chunk[] cks) {
      for (int i = 0; i < cks[0]._len; ++i) {
        int cls = (int)cks[cks.length - 1].at0(i);
        double err = ( 1 - cks[cls+1].at0(i));
        _ss += err * err;
      }
    }

    @Override public void reduce(MSETask tsk) { _ss += tsk._ss; }
  }
}
