package hex.rf;

import java.util.Arrays;
import java.util.Random;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ChunkProgressJob;
import water.ValueArray.Column;
import water.util.*;
import water.util.Log.Tag.Sys;

import com.google.common.primitives.Ints;

/**
 * Confusion Matrix. Incrementally computes a Confusion Matrix for a forest
 * of Trees, vs a given input dataset. The set of Trees can grow over time. Each
 * request from the Confusion compute on any new trees (if any), and report a
 * matrix. Cheap if all trees already computed.
 */
public class ConfusionTask extends MRTask {

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
  transient private RFModel _model;
  /** @LOCAL: Mapping from model columns to data columns */
  transient private int[] _modelDataMap;
  /** @LOCAL: The dataset to validate */
  transient public ValueArray _data;
  /** @LOCAL: Number of response classes = Max(responses in model, responses in test data)*/
  transient public int  _N;
  /** @LOCAL: Number of response classes in model */
  transient public int _MODEL_N;
  /** @LOCAL: Number of response classes in data */
  transient public int _DATA_N;
  /** For reproducibility we can control the randomness in the computation of the
      confusion matrix. The default seed when deserializing is 42. */
  transient private Random    _rand;
  /** @LOCAL: Data to replay the sampling algorithm */
  transient private int[]     _chunk_row_mapping;
  /** @LOCAL: Number of rows at each node */
  transient private int[]     _rowsPerNode;
  /** @LOCAL: Computed mapping of model prediction classes to confusion matrix classes */
  transient private int[]     _model_classes_mapping;
  /** @LOCAL: Computed mapping of data prediction classes to confusion matrix classes */
  transient private int[]     _data_classes_mapping;
  /** @LOCAL: Difference between model cmin and CM cmin */
  transient private int       _cmin_model_mapping;
  /** @LOCAL: Difference between data cmin and CM cmin */
  transient private int       _cmin_data_mapping;

  /**   Constructor for use by the serializers */
  public ConfusionTask() { }

  /** Confusion matrix
   * @param model the ensemble used to classify
   * @param datakey the key of the data that will be classified
   */
  private ConfusionTask(CMJob job, RFModel model, int treesToUse, Key datakey, int classcol, double[] classWt, boolean computeOOB ) {
    _job        = job;
    _modelKey   = model._selfKey;
    _datakey    = datakey;
    _classcol   = classcol;
    _classWt    = classWt != null && classWt.length > 0 ? classWt : null;
    _treesUsed  = treesToUse;
    _computeOOB = computeOOB;
    shared_init();
  }

  public Key keyForCM() { return keyForCM(_model._selfKey,_treesUsed,_datakey,_classcol,_computeOOB); }
  static public Key keyForCM(Key modelKey, int msize, Key datakey, int classcol, boolean computeOOB) {
    return Key.make("ConfusionMatrix of (" + datakey+"["+classcol+"],"+modelKey+"["+msize+"],"+(computeOOB?"1":"0")+")");
  }

  public static void remove(RFModel model, Key datakey, int classcol, boolean computeOOB) {
    Key key = keyForCM(model._selfKey, model.size(), datakey, classcol, computeOOB);
    UKV.remove(key);
  }

  /**Apply a model to a dataset to produce a Confusion Matrix.  To support
     incremental & repeated model application, hash the model & data and look
     for that Key to already exist, returning a prior CM if one is available.*/
  static public CMJob make(RFModel model, Key datakey, int classcol, double[] classWt, boolean computeOOB) {
    return make(model, model.size(), datakey, classcol, classWt, computeOOB);
  }
  static public CMJob make(final RFModel model, final int modelSize, final Key datakey, final int classcol, final double[] classWt, final boolean computeOOB) {
    // Create a unique key for CM regarding given RFModel, validation data and parameters
    final Key cmKey = keyForCM(model._selfKey, modelSize, datakey, classcol, computeOOB);
    // Start a new job if CM is not yet computed
    final Value dummyCMVal = new Value(cmKey, CMFinal.make());
    final Value val = DKV.DputIfMatch(cmKey, dummyCMVal, null, null);
    if (val==null) {
      final CMJob cmJob = new CMJob(modelSize,cmKey);
      cmJob.destination_key = cmKey;
      cmJob.description = "CM computation";
      // and start a new confusion matrix computation
      H2OCountedCompleter fjtask = new H2OCountedCompleter() {
        @Override public void compute2() {
          ConfusionTask cmTask = new ConfusionTask(cmJob, model, modelSize, datakey, classcol, classWt, computeOOB);
          cmTask.invoke(datakey); // Invoke and wait for completion
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
      return (CMJob) Job.findJobByDest(cmKey);
    }
  }

  /** Shared init: pre-compute local data for new Confusions, for remote Confusions*/
  private void shared_init() {
    _rand   = Utils.getRNG(0x92b5023f2cd40b7cL); // big random seed
    _data   = UKV.get(_datakey);
    _model  = UKV.get(_modelKey);
    _modelDataMap = _model.columnMapping(_data.colNames());
    assert !_computeOOB || _model._dataKey.equals(_datakey) : !_computeOOB + " || " + _model._dataKey + " equals " + _datakey ;
    Column respModel = _model.response();
    Column respData  = _data._cols[_classcol];
    _DATA_N  = (int) respData.numDomainSize();
    _MODEL_N = (int) respModel.numDomainSize();
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
      _cmin_model_mapping = (int) (respModel._min - Math.min(respModel._min, respData._min) );
      _cmin_data_mapping  = (int) (respData._min  - Math.min(respModel._min, respData._min) );
      _N = (int) (Math.max(respModel._max, respData._max) - Math.min(respModel._min, respData._min) + 1);
    }
    assert _N > 0; // You know...it is good to be sure
  }

  /**
   * Once-per-remote invocation init. The standard M/R framework will endlessly
   * clone the original object "for free" (well, for very low cost), but the
   * wire-line format does not send over things we can compute locally. So
   * compute locally, once, some things we want in all cloned instances.
   */
  @Override public void init() {
    super.init();
    shared_init();
    // Make a mapping from chunk# to row# just for chunks on this node
    long l = ValueArray.getChunkIndex(_keys[_keys.length-1])+1;
    _chunk_row_mapping = new int[Ints.checkedCast(l)];
    int off=0;
    for( Key k : _keys )
      if( k.home() ) {
        l = ValueArray.getChunkIndex(k);
        _chunk_row_mapping[(int)l] = off;
        off += _data.rpc(l);
      }
    // Initialize number of rows per node
    _rowsPerNode = new int[H2O.CLOUD.size()];
    long chunksCount = _data.chunks();
    for(int ci=0; ci<chunksCount; ci++) {
      Key cKey = _data.getChunkKey(ci);
      _rowsPerNode[cKey.home_node().index()] += _data.rpc(ci);
    }
  }

  /**A classic Map/Reduce style incremental computation of the confusion
   * matrix on a chunk of data.
   * */
  public void map(Key chunkKey) {
    AutoBuffer cdata      = _data.getChunk(chunkKey);
    final int nchk       = (int) ValueArray.getChunkIndex(chunkKey);
    final int rows       = _data.rpc(nchk);
    final int cmin       = (int) _data._cols[_classcol]._min;
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
      int     init_row    = _chunk_row_mapping[nchk];
      boolean isLocalTree = _computeOOB ? isLocalTree(ntree, producerId) : false; // tree is local
      boolean isRemoteTreeChunk = _computeOOB ? isRemoteChunk(producerId, chunkKey) : false; // this is chunk which was used for construction the tree by another node
      if (isRemoteTreeChunk) init_row = _rowsPerNode[producerId] + producerRemoteRows(producerId, chunkKey);
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
        if( _data.isNA(cdata, row, _classcol)) continue ROWS;

        if( _computeOOB && (isLocalTree || isRemoteTreeChunk)) { // if OOBEE is computed then we need to take into account utilized sampling strategy
          switch( _model._samplingStrategy ) {
          case RANDOM          : if (sampledItem < _model._sample ) continue ROWS; break;
          case STRATIFIED_LOCAL:
            int clazz = (int) _data.data(cdata, row, _classcol) - cmin;
            if (sampledItem < _model._strataSamples[clazz] ) continue ROWS;
            break;
          default: assert false : "The selected sampling strategy does not support OOBEE replay!"; break;
          }
        }
        // --- END OF CRUCIAL CODE ---

        // Predict with this tree - produce 0-based class index
        int prediction = _model.classify0(ntree, _data, cdata, row, _modelDataMap, numClasses );
        if( prediction >= numClasses ) continue ROWS; // Junk row cannot be predicted
        // Check tree miss
        int alignedPrediction = alignModelIdx(prediction);
        int alignedData       = alignDataIdx((int) _data.data(cdata, row, _classcol) - cmin);
        if (alignedPrediction != alignedData) _errorsPerTree[ntree]++;
        votes[row][alignedPrediction]++; // Vote the row
        if (isLocalTree) localVotes[row][alignedPrediction]++; // Vote
      }
    }
    // Assemble the votes-per-class into predictions & score each row
    _matrix = computeCM(votes, cdata); // Make a confusion matrix for this chunk
    if (localVotes!=null) {
      _localMatrices = new CM[H2O.CLOUD.size()];
      _localMatrices[H2O.SELF.index()] = computeCM(localVotes, cdata);
    }
  }


  /** Returns true if tree was produced by this node.
   * Note: chunkKey is key stored at this local node */
  private boolean isLocalTree(int ntree, byte treeProducerId) {
    assert _computeOOB == true : "Calling this method makes sense only for oobee";
    int idx  = H2O.SELF.index();
    return idx == treeProducerId;
  }

  /** Returns true if chunk was loaded during processing the tree. */
  private boolean isRemoteChunk(byte treeProducerId, Key chunkKey) {
    // it is not local tree => try to find if tree producer used chunk key for
    // tree construction
    Key[] remoteCKeys = _model._remoteChunksKeys[treeProducerId];
    for (int i=0; i<remoteCKeys.length; i++)
      if (chunkKey.equals(remoteCKeys[i])) return true;
    return false;
  }

  private int producerRemoteRows(byte treeProducerId, Key chunkKey) {
    Key[] remoteCKeys = _model._remoteChunksKeys[treeProducerId];
    int off = 0;
    for (int i=0; i<remoteCKeys.length; i++) {
      if (chunkKey.equals(remoteCKeys[i])) return off;
      off += _data.rpc(ValueArray.getChunkIndex(remoteCKeys[i]));
    }
    assert false : "Never should be here!";
    return off;

  }

  /** Reduction combines the confusion matrices. */
  public void reduce(DRemoteTask drt) {
    ConfusionTask C = (ConfusionTask) drt;
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

  public static String[] domain(final Column modelCol, final Column dataCol) {
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
      N = (int) (Math.max(modelCol._max, dataCol._max) - Math.min(modelCol._min, dataCol._min) + 1);
    }
    return domain(N, modelCol, dataCol, modelEnumMapping, dataEnumMapping);
  }

  public static String[] domain(int N, final Column modelCol, final Column dataCol, int[] modelEnumMapping, int[] dataEnumMapping) {
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
      int dmin = (int) Math.min(modelCol._min, dataCol._min);
      int dmax = (int) Math.max(modelCol._max, dataCol._max);
      for (int i = dmin; i <= dmax; i++) result[i-dmin] = String.valueOf(i);
    }
    return result;
  }

  /** Compute confusion matrix domain based on model and data key. */
  public String[] domain() {
    return domain(_N, _model.response(), _data._cols[_classcol], _model_classes_mapping, _data_classes_mapping);
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
    final protected Key      _rfModelKey;
    final protected String[] _domain;
    final protected long  [] _errorsPerTree;
    final protected boolean  _computedOOB;
    protected boolean        _valid;

    private CMFinal() {
      _valid         = false;
      _rfModelKey    = null;
      _domain        = null;
      _errorsPerTree = null;
      _computedOOB   = false;
    }
    private CMFinal(CM cm, Key rfModelKey, String[] domain, long[] errorsPerTree, boolean computedOOB, boolean valid) {
      _matrix = cm._matrix; _errors = cm._errors; _rows = cm._rows; _skippedRows = cm._skippedRows;
      _rfModelKey    = rfModelKey;
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
    public static final CMFinal make(CM cm, RFModel model, String[] domain, long[] errorsPerTree, boolean computedOOB) {
      return new CMFinal(cm, model._selfKey, domain, errorsPerTree, computedOOB, true);
    }
    public String[] domain() { return _domain; }
    public int      dimension() { return _matrix.length; }
    public long     matrix(int i, int j) { return _matrix[i][j]; }
    public boolean  valid() { return _valid; }

    /** Output information about this RF. */
    public final void report() {
      double err = classError();
      assert _valid == true : "Trying to report status of invalid CM!";

      RFModel model = UKV.get(_rfModelKey);
      String s =
            "              Type of random forest: classification\n"
          + "                    Number of trees: " + model.size() + "\n"
          + "No of variables tried at each split: " + model._splitFeatures + "\n"
          + "              Estimate of err. rate: " + Math.round(err * 10000) / 100 + "%  (" + err + ")\n"
          + "                              OOBEE: " + (_computedOOB ? "YES (sampling rate: "+model._sample*100+"%)" : "NO")+ "\n"
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
  final CM computeCM(int[][] votes, AutoBuffer bits) {
    CM cm = new CM();
    int rows = votes.length;
    int validation_rows = 0;
    int cmin = (int) _data._cols[_classcol]._min;

    // Assemble the votes-per-class into predictions & score each row
    cm._matrix = new long[_N][_N]; // Make an empty confusion matrix for this chunk
    for( int i = 0; i < rows; i++ ) {
      int[] vi = votes[i];
      if(_classWt != null )
        for( int v = 0; v<_N; v++) vi[v] = (int)(vi[v]*_classWt[v]);
      int result = 0, tied = 1;
      for( int l = 1; l<_N; l++)
        if( vi[l] > vi[result] ) { result=l; tied=1; }
        else if( vi[l] == vi[result] ) { tied++; }
      if( vi[result]==0 ) { cm._skippedRows++; continue; }// Ignore rows with zero votes
      if( tied>1 ) {                // Tie-breaker logic
        int j = _rand.nextInt(tied);
        int k = 0;
        for( int l=0; l<_N; l++ )
          if( vi[l]==vi[result] && (k++ >= j) )  // From zero to number of tied classes-1
            { result = l; break; }
      }
      int cclass = alignDataIdx((int) _data.data(bits, i, _classcol) - cmin);
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
