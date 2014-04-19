package hex.singlenoderf;

import com.google.common.primitives.Ints;
import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ChunkProgressJob;
import water.ValueArray.Column;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;
import water.util.Log.Tag.Sys;
import water.util.ModelUtils;
import water.util.Utils;

import java.util.Arrays;
import java.util.Random;

/**
 * Confusion Matrix. Incrementally computes a Confusion Matrix for a forest
 * of Trees, vs a given input dataset. The set of Trees can grow over time. Each
 * request from the Confusion compute on any new trees (if any), and report a
 * matrix. Cheap if all trees already computed.
 */
public class ConfusionTask extends MRTask2<ConfusionTask> {

  /** @IN: Class weights */
  double[] _classWt;
  /** @IN: Compute oobee or not */
  boolean  _computeOOB;
  /** @IN: Number of used trees in CM computation */
  int      _treesUsed;
  /** @IN: Key for the model used for construction of the confusion matrix. */
  Key      _modelKey;
  /** @IN: Dataset we are building the matrix on.  The column count must match the Trees.*/
  Key      _datakey;
  /** @IN: Column holding the class, defaults to last column */
  int     _classcol;
  /** @IN: Job */
  CMJob   _job;

  /** @OUT: Confusion matrix */
  CM   _matrix;
  /** @OUT: Local confusion matrixes if oobee is enabled. */
  CM[] _localMatrices;
  /** @OUT: Error rate per tree */
  private long[] _errorsPerTree;

  // Computed local data
  /** @LOCAL: Model used for construction of the confusion matrix. */
  private SpeeDRFModel _model;
  /** @LOCAL: Mapping from model columns to data columns */
  private int[] _modelDataMap;
  /** @LOCAL: The dataset to validate */
  public Frame _data;
  /** @LOCAL: Number of response classes = Max(responses in model, responses in test data)*/
  public int  _N;
  /** @LOCAL: Number of response classes in model */
  public int _MODEL_N;
  /** @LOCAL: Number of response classes in data */
  public int _DATA_N;
  /** For reproducibility we can control the randomness in the computation of the
   confusion matrix. The default seed when deserializing is 42. */
  transient private Random    _rand;
  /** @LOCAL: Data to replay the sampling algorithm */
  private int[]     _chunk_row_mapping;
  /** @LOCAL: Number of rows at each node */
  private int[]     _rowsPerNode;
  /** @LOCAL: Computed mapping of model prediction classes to confusion matrix classes */
  private int[]     _model_classes_mapping;
  /** @LOCAL: Computed mapping of data prediction classes to confusion matrix classes */
  private int[]     _data_classes_mapping;
  /** @LOCAL: Difference between model cmin and CM cmin */
  private int       _cmin_model_mapping;
  /** @LOCAL: Difference between data cmin and CM cmin */
  private int       _cmin_data_mapping;

  /**   Constructor for use by the serializers */
  public ConfusionTask() { }

  /** Confusion matrix
   * @param model the ensemble used to classify
   * @param datakey the key of the data that will be classified
   */
  private ConfusionTask(CMJob job, SpeeDRFModel model, int treesToUse, Key datakey, int classcol, double[] classWt, boolean computeOOB ) {
    _job        = job;
    _modelKey   = model._key;
    _datakey    = model._dataKey;
    _classcol   = model.fr.find(model.responseName());
    _classWt    = classWt != null && classWt.length > 0 ? classWt : null;
    _treesUsed  = treesToUse;
    _computeOOB = computeOOB;
    shared_init();
  }

  public Key keyForCM() { return keyForCM(_model._key,_treesUsed,_datakey,_classcol,_computeOOB); }
  static public Key keyForCM(Key modelKey, int msize, Key datakey, int classcol, boolean computeOOB) {
    return Key.make("ConfusionMatrix of (" + datakey+"["+classcol+"],"+modelKey+"["+msize+"],"+(computeOOB?"1":"0")+")");
  }

  public static void remove(SpeeDRFModel model, Key datakey, int classcol, boolean computeOOB) {
    Key key = keyForCM(model._key, model.size(), datakey, classcol, computeOOB);
    UKV.remove(key);
  }

  /**Apply a model to a dataset to produce a Confusion Matrix.  To support
   incremental & repeated model application, hash the model & data and look
   for that Key to already exist, returning a prior CM if one is available.*/
  static public Job make(SpeeDRFModel model, Key datakey, int classcol, double[] classWt, boolean computeOOB) {
    return make(model, model.size(), datakey, classcol, classWt, computeOOB);
  }
  static public Job make(final SpeeDRFModel model, final int modelSize, final Key datakey, final int classcol, final double[] classWt, final boolean computeOOB) {
    // Create a unique key for CM regarding given SpeeDRFModel, validation data and parameters
    final Key cmKey = keyForCM(model._key, modelSize, datakey, classcol, computeOOB);
    // Start a new job if CM is not yet computed
    final Value dummyCMVal = new Value(cmKey, CMFinal.make());
    final Value val = DKV.DputIfMatch(cmKey, dummyCMVal, null, null);
    if (val==null) {
      final CMJob cmJob = new CMJob(modelSize,cmKey);
      cmJob.description = "CM computation";
      // and start a new confusion matrix computation
      H2OCountedCompleter fjtask = new H2OCountedCompleter() {
        @Override public void compute2() {
          ConfusionTask cmTask = new ConfusionTask(cmJob, model, modelSize, datakey, classcol, classWt, computeOOB);
          cmTask.doAll(model.fr); // Invoke and wait for completion
          // Create final matrix
          CMFinal cmResult = CMFinal.make(cmTask._matrix, model, cmTask.domain(), cmTask._errorsPerTree, computeOOB);
          // Atomically overwrite the dummy result
          // Doing update via atomic is a bad idea since it can be overwritten by DputIfMatch above - CMFinal.updateDKV(cmJob.dest(), cmResult);
          // Rather do it directly
          Value oldVal = DKV.DputIfMatch(cmKey, new Value(cmKey, cmResult), dummyCMVal, null);
          // Be sure that nobody overwrite the value since I am only one writter
          assert oldVal == dummyCMVal;
          // Remove this jobs - it already finished or it was useless
          cmJob.remove();
          tryComplete();
        }
        @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
          cmJob.onException(ex);
          return super.onExceptionalCompletion(ex, caller);
        }
      };
      cmJob.start(fjtask);
      H2O.submitTask(fjtask);
      // FIXME the the job should be invoked asynchronously but for now we block immediately
      // since we do not store a list of previous jobs
      cmJob.get();
      return cmJob;
    } else {
      // We should return Job which is/was computing the CM with given cmKey
      return Job.findJobByDest(cmKey);
    }
  }

  /** Shared init: pre-compute local data for new Confusions, for remote Confusions*/
  private void shared_init() {
    _rand   = Utils.getRNG(0x92b5023f2cd40b7cL); // big random seed
    _data   = UKV.get(_datakey);
    _model  = UKV.get(_modelKey);

    _modelDataMap = _model.colMap(_model._names);
    assert !_computeOOB || _model._dataKey.equals(_datakey) : !_computeOOB + " || " + _model._dataKey + " equals " + _datakey ;
    Vec respModel = _model.get_response();
    Vec respData  = _data.vecs()[_classcol];
    _DATA_N  = (int) (respData.max() - respData.min() + 1);
    _MODEL_N = (int) (respModel.max() - respModel.min() + 1);
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
      _cmin_model_mapping = (int) (respModel.min() - Math.min(respModel.min(), respData.min()) );
      _cmin_data_mapping  = (int) (respData.min()  - Math.min(respModel.min(), respData.min()) );
      _N = (int) (Math.max(respModel.max(), respData.max()) - Math.min(respModel.min(), respData.min()) + 1);
    }
    assert _N > 0; // You know...it is good to be sure
    init();
  }

  /**
   * Once-per-remote invocation init. The standard M/R framework will endlessly
   * clone the original object "for free" (well, for very low cost), but the
   * wire-line format does not send over things we can compute locally. So
   * compute locally, once, some things we want in all cloned instances.
   */
  public void init() {
//    super.init();
//    shared_init();
    // Make a mapping from chunk# to row# just for chunks on this node
//    long l = ValueArray.getChunkIndex(_keys[_keys.length-1])+1;
//    _chunk_row_mapping = new int[Ints.checkedCast(l)];
    int total_home = 0;
    for (int i = 0; i < _data.anyVec().nChunks(); ++i) {
      if (_data.anyVec().chunkKey(i).home()) {
        total_home++;
      }
    }
    _chunk_row_mapping = new int[total_home];

    int off=0;
    int cidx=0;
    for (int i = 0; i < _data.anyVec().nChunks(); ++i) {
      if (_data.anyVec().chunkKey(i).home()) {
        _chunk_row_mapping[cidx++] = off;
        off += _data.anyVec().chunkLen(i);
      }
    }
    // Initialize number of rows per node
    _rowsPerNode = new int[H2O.CLOUD.size()];
    long chunksCount = _data.anyVec().nChunks();
    for(int ci=0; ci<chunksCount; ci++) {
      Key cKey = _data.anyVec().chunkKey(ci);
      _rowsPerNode[cKey.home_node().index()] += _data.anyVec().chunkLen(ci);
    }
  }

  /**A classic Map/Reduce style incremental computation of the confusion
   * matrix on a chunk of data.
   * */
  @Override public void map(Chunk[] chks) {
//    AutoBuffer cdata      = _data.getChunk(chunkKey);
//    final int nchk       = (int) ValueArray.getChunkIndex(chunkKey);

    final int rows = chks[0]._len;
    final int cmin       = (int) _data.vecs()[_classcol].min();
    short     numClasses = (short)_model.classes();

    // Votes: we vote each tree on each row, holding on to the votes until the end
    int[][] votes = new int[rows][_N];
    int[][] localVotes = _computeOOB ? new int[rows][_N] : null;
    // Errors per tree
    _errorsPerTree = new long[_model.treeCount()];
    // Replay the Data.java's "sample_fair" sampling algorithm to exclude data
    // we trained on during voting.
    for( int ntree = 0; ntree < _model.treeCount(); ntree++ ) {
      long    treeSeed    = _model.seed(ntree);
      byte    producerId  = _model.producerId(ntree);
      int     init_row    = (int)chks[0]._start;
      boolean isLocalTree = _computeOOB && isLocalTree(ntree, producerId); // tree is local
      boolean isRemote = true;
      for (int a_chunk_row_mapping : _chunk_row_mapping) {
        if (chks[0]._start == a_chunk_row_mapping) {
          isRemote = false;
          break;
        }
      }
      boolean isRemoteTreeChunk = _computeOOB && isRemote; // this is chunk which was used for construction the tree by another node
      if (isRemoteTreeChunk) init_row = _rowsPerNode[producerId] + (int)chks[0]._start;
      /* NOTE: Before changing used generator think about which kind of random generator you need:
       * if always deterministic or non-deterministic version - see hex.rf.Utils.get{Deter}RNG */
      // DEBUG: if( _computeOOB && (isLocalTree || isRemoteTreeChunk)) System.err.println(treeSeed + " : " + init_row + " (CM) " + isRemoteTreeChunk);
      long seed = Sampling.chunkSampleSeed(treeSeed, init_row);
      Random rand = Utils.getDeterRNG(seed);
      // Now for all rows, classify & vote!
      ROWS: for( int r = 0; r < rows; r++ ) {
        int row = r + (int)chks[0]._start;
        // ------ THIS CODE is crucial and serve to replay the same sequence
        // of random numbers as in the method Data.sampleFair()
        // Skip row used during training if OOB is computed
        float sampledItem = rand.nextFloat();
        // Bail out of broken rows with NA in class column.
        // Do not skip yet the rows with NAs in the rest of columns
        if( chks[_classcol].isNA(row)) continue ROWS;

        if( _computeOOB && (isLocalTree || isRemoteTreeChunk)) { // if OOBEE is computed then we need to take into account utilized sampling strategy
          switch( _model.sampling_strategy ) {
            case RANDOM          : if (sampledItem < _model.sample ) continue ROWS; break;
            case STRATIFIED_LOCAL:
              int clazz = (int) chks[_classcol].at8(row) - cmin;
              if (sampledItem < _model.strata_samples[clazz] ) continue ROWS;
              break;
            default: assert false : "The selected sampling strategy does not support OOBEE replay!"; break;
          }
        }
        // --- END OF CRUCIAL CODE ---

        // Predict with this tree - produce 0-based class index
        int prediction = _model.classify0(ntree, _data, chks, row, _modelDataMap, numClasses );
        if( prediction >= numClasses ) continue ROWS; // Junk row cannot be predicted
        // Check tree miss
        int alignedPrediction = alignModelIdx(prediction);
        int alignedData       = alignDataIdx((int) _data.vecs()[_classcol].at8(row) - cmin);
        if (alignedPrediction != alignedData) _errorsPerTree[ntree]++;
        votes[r][alignedPrediction]++; // Vote the row
        if (isLocalTree) localVotes[r][alignedPrediction]++; // Vote
      }
    }
    // Assemble the votes-per-class into predictions & score each row
    _matrix = computeCM(votes, chks); // Make a confusion matrix for this chunk
    if (localVotes!=null) {
      _localMatrices = new CM[H2O.CLOUD.size()];
      _localMatrices[H2O.SELF.index()] = computeCM(localVotes, chks);
    }
  }


  /** Returns true if tree was produced by this node.
   * Note: chunkKey is key stored at this local node */
  private boolean isLocalTree(int ntree, byte treeProducerId) {
    assert _computeOOB == true : "Calling this method makes sense only for oobee";
    int idx  = H2O.SELF.index();
    return idx == treeProducerId;
  }

//  /** Returns true if chunk was loaded during processing the tree. */
//  private boolean isRemoteChunk(byte treeProducerId, Key chunkKey) {
//    // it is not local tree => try to find if tree producer used chunk key for
//    // tree construction
//    Key[] remoteCKeys = _model._remoteChunksKeys[treeProducerId];
//    for (int i=0; i<remoteCKeys.length; i++)
//      if (chunkKey.equals(remoteCKeys[i])) return true;
//    return false;
//  }
//
//  private int producerRemoteRows(byte treeProducerId, Key chunkKey) {
//    Key[] remoteCKeys = _model._remoteChunksKeys[treeProducerId];
//    int off = 0;
//    for (int i=0; i<remoteCKeys.length; i++) {
//      if (chunkKey.equals(remoteCKeys[i])) return off;
//      off += _data.rpc(ValueArray.getChunkIndex(remoteCKeys[i]));
//    }
//    assert false : "Never should be here!";
//    return off;
//
//  }

  /** Reduction combines the confusion matrices. */
  @Override public void reduce(ConfusionTask drt) {
    ConfusionTask C = drt;
    if (_matrix == null) {
      _matrix = C._matrix;
    } else {
      _matrix = _matrix.add(C._matrix);
    }
    // Reduce tree errors
    long[] ept1 = _errorsPerTree;
    long[] ept2 = C._errorsPerTree;
    if (ept1 == null) _errorsPerTree = ept2;
    else if (ept2 != null) {
      if (ept1.length < ept2.length) ept1 = Arrays.copyOf(ept1, ept2.length);
      for (int i = 0; i < ept2.length; i++) ept1[i] += ept2[i];
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
        if (modelMapping!=null) modelMapping[idxM] = idx;
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
    int N = 0;

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
  public String[] domain() {
    return domain(_N, _model.get_response(), _data.vecs()[_classcol], _model_classes_mapping, _data_classes_mapping);
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
    protected boolean        _valid;

    private CMFinal() {
      _valid         = false;
      _SpeeDRFModelKey    = null;
      _domain        = null;
      _errorsPerTree = null;
      _computedOOB   = false;
    }
    private CMFinal(CM cm, Key SpeeDRFModelKey, String[] domain, long[] errorsPerTree, boolean computedOOB, boolean valid) {
      _matrix = cm._matrix; _errors = cm._errors; _rows = cm._rows; _skippedRows = cm._skippedRows;
      _SpeeDRFModelKey    = SpeeDRFModelKey;
      _domain        = domain;
      _errorsPerTree = errorsPerTree;
      _computedOOB   = computedOOB;
      _valid         = valid;
    }
    /** Make non-valid confusion matrix */
    public static final CMFinal make() {
      return new CMFinal();
    }
    /** Create a new confusion matrix. */
    public static final CMFinal make(CM cm, SpeeDRFModel model, String[] domain, long[] errorsPerTree, boolean computedOOB) {
      return new CMFinal(cm, model._key, domain, errorsPerTree, computedOOB, true);
    }
    public String[] domain() { return _domain; }
    public int      dimension() { return _matrix.length; }
    public long     matrix(int i, int j) { return _matrix[i][j]; }
    public boolean  valid() { return _valid; }

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

    public static void updateDKV(final Key key, final CMFinal cm) {
      new TAtomic<CMFinal>() {
        @Override public CMFinal atomic(CMFinal old) {
          if(old == null) return null;
          return cm;
        }
      }.invoke(key);
    }
  }

  /** Produce confusion matrix from given votes. */
  final CM computeCM(int[][] votes, Chunk[] chks) {
    CM cm = new CM();
    int rows = votes.length;
    int validation_rows = 0;
    int cmin = (int) _data.vecs()[_classcol].min();
    // Assemble the votes-per-class into predictions & score each row
    cm._matrix = new long[_N][_N];          // Make an empty confusion matrix for this chunk
    float preds[] = new float[_N+1];
    for( int r = 0; r < rows; r++ ) { // Iterate over rows
      int row = r + (int)chks[0]._start;
      int[] vi = votes[r];                // Votes for i-th row
      for( int v=0; v<_N; v++ ) preds[v+1] = vi[v];
      if(_classWt != null )                 // Apply class weights
        for( int v = 0; v<_N; v++) preds[v+1] *= _classWt[v];
      int result = ModelUtils.getPrediction(preds, row); // Share logic to get a prediction for classifiers (solve ties)
      if( vi[result]==0 ) { cm._skippedRows++; continue; }// Ignore rows with zero votes

      int cclass = alignDataIdx((int) chks[_classcol].at8(row) - cmin);
      assert 0 <= cclass && cclass < _N : ("cclass " + cclass + " < " + _N);
      cm._matrix[cclass][result]++;
      if( result != cclass ) cm._errors++;
      validation_rows++;
    }

    cm._rows=validation_rows;
    return cm;
  }

  public static class CMJob extends ChunkProgressJob {
    public CMJob(long chunksTotal, Key dest) {
      super(chunksTotal,dest);
    }
  }
}