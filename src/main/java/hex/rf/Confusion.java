package hex.rf;

import java.util.Arrays;
import java.util.Random;

import water.*;
import water.ValueArray.Column;
import water.util.Utils;

import com.google.common.primitives.Ints;

/**
 * Confusion Matrix. Incrementally computes a Confusion Matrix for a KEY_OF_KEYS
 * of Trees, vs a given input dataset. The set of Trees can grow over time. Each
 * request from the Confusion compute on any new trees (if any), and report a
 * matrix. Cheap if all trees already computed.
 */
public class Confusion extends MRTask {

  /** Number of used trees in CM computation */
  public int _treesUsed;
  /** Key for the model used for construction of the confusion matrix. */
  public Key _modelKey;
  /** Model used for construction of the confusion matrix. */
  transient private RFModel _model;
  /** Mapping from model columns to data columns */
  transient private int[] _modelDataMap;
  /** Dataset we are building the matrix on.  The column count must match the Trees.*/
  public Key    _datakey;
  /** Column holding the class, defaults to last column */
  int   _classcol;
  /** The dataset */
  transient public ValueArray _data;
  /** Number of response classes */
  transient public int  _N;
  /** The Confusion Matrix - a NxN matrix of [actual] -vs- [predicted] classes,
      referenced as _matrix[actual][predicted]. Each row in the dataset is
      voted on by all trees, and the majority vote is the predicted class for
      the row. Each row thus gets 1 entry in the matrix.*/
  public long                 _matrix[][];
  /** Number of mistaken assignments. */
  private long                _errors;
  /** Error rate per tree */
  private long[]              _errorsPerTree;
  /** Number of rows used for building the matrix.*/
  private long                _rows;
  /** Number of skipped rows => rows can contain bad data, or can be skipped by selecting only out-of-back rows */
  private long                _skippedRows;
  /** Class weights */
  private double[]            _classWt;
  /** For reproducibility we can control the randomness in the computation of the
      confusion matrix. The default seed when deserializing is 42. */
  private transient Random    _rand;
  /** Data to replay the sampling algorithm */
  transient private int[]     _chunk_row_mapping;
  public boolean _computeOOB;

  /**   Constructor for use by the serializers */
  public Confusion() { }

  /** Confusion matrix
   * @param model the ensemble used to classify
   * @param datakey the key of the data that will be classified
   */
  private Confusion(RFModel model, Key datakey, int classcol, double[] classWt, boolean computeOOB ) {
    _modelKey = model._selfKey;
    _datakey = datakey;
    _classcol = classcol;
    _classWt = classWt != null && classWt.length > 0 ? classWt : null;
    _treesUsed = model.size();
    _computeOOB = computeOOB;
    shared_init();
  }

  public Key keyFor() { return keyFor(_model._selfKey,_model.size(),_datakey, _classcol, _computeOOB); }
  static public Key keyFor(Key modelKey, int msize, Key datakey, int classcol, boolean computeOOB) {
    return Key.make("ConfusionMatrix of (" + datakey+"["+classcol+"],"+modelKey+"["+msize+"],"+(computeOOB?"1":"0")+")");
  }

  static public Key keyForProgress(Key modelKey, int msize, Key datakey, int classcol, boolean computeOOB) {
    // make sure it is a system key
    return Key.make("\0" + "ConfusionMatrixProgress of (" + datakey+"["+classcol+"],"+modelKey+")");
  }

  public static void remove(RFModel model, Key datakey, int classcol, boolean computeOOB) {
    Key key = keyFor(model._selfKey, model.size(), datakey, classcol, computeOOB);
    UKV.remove(key);
  }

  /**Apply a model to a dataset to produce a Confusion Matrix.  To support
     incremental & repeated model application, hash the model & data and look
     for that Key to already exist, returning a prior CM if one is available.*/
  static public Confusion make(RFModel model, Key datakey, int classcol, double[] classWt,boolean computeOOB) {
    Key key = keyFor(model._selfKey, model.size(), datakey, classcol, computeOOB);
    Confusion C = UKV.get(key, new Confusion());
    if( C != null ) {         // Look for a prior cached result
      C.shared_init();
      return C;
    }

    // mark that we are computing the matrix now
    Key progressKey = keyForProgress(model._selfKey, model.size(), datakey, classcol, computeOOB);
    Value v = DKV.DputIfMatch(progressKey, new Value(progressKey,"IN_PROGRESS"), null, null);
    C = new Confusion(model,datakey,classcol,classWt,computeOOB);
    if (v != null) { // someone is already working on the matrix, stop
      C._matrix = null;
      return C;
    }
    if( model.size() > 0 ) C.invoke(datakey); // Compute on it: count votes
    UKV.put(key,C);          // Output to cloud
    UKV.remove(progressKey); // signal that we have done computing the matrix
    if( classWt != null )
      for( int i=0; i<classWt.length; i++ )
        if( classWt[i] != 1.0 )
          Utils.pln("[CM] Weighted votes "+i+" by "+classWt[i]);
    return C;
  }

  public boolean isValid() { return _matrix != null; }

  /** Shared init: for new Confusions, for remote Confusions*/
  private void shared_init() {
    _rand   = Utils.getRNG(0x92b5023f2cd40b7cL); // big random seed
    _data = ValueArray.value(DKV.get(_datakey));
    _model = UKV.get(_modelKey, new RFModel());
    _modelDataMap = _model.columnMapping(_data.colNames());
    assert !_computeOOB || _model._dataKey==_datakey : _computeOOB + " || " + _model._dataKey + " == " + _datakey ;
    Column c = _data._cols[_classcol];
    _N = (int)((c._max - c._min)+1);
    assert _N > 0;
  }

  /**
   * Once-per-remote invocation init. The standard M/R framework will endlessly
   * clone the original object "for free" (well, for very low cost), but the
   * wire-line format does not send over things we can compute locally. So
   * compute locally, once, some things we want in all cloned instances.
   */
  public void init() {
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
  }


  /**A classic Map/Reduce style incremental computation of the confusion matrix on a chunk of data. */
  public void map(Key chunk_key) {
    AutoBuffer bits = _data.getChunk(chunk_key);
    final int rowsize = _data._rowsize;
    final int rows = bits.remaining() / rowsize;
    final int cmin = (int) _data._cols[_classcol]._min;
    int nchk = (int) ValueArray.getChunkIndex(chunk_key);
    final Column[] cols = _data._cols;
    short numClasses = (short)_model.classes();

    // Votes: we vote each tree on each row, holding on to the votes until the end
    int[][] votes = new int[rows][_N];

    _errorsPerTree = new long[_model.treeCount()];
    // Replay the Data.java's "sample_fair" sampling algorithm to exclude data
    // we trained on during voting.
    for( int ntree = 0; ntree < _model.treeCount(); ntree++ ) {
      long seed = _model.seed(ntree);
      long init_row = _chunk_row_mapping[nchk];
      /* NOTE: Before changing used generator think about which kind of random generator you need:
       * if always deterministic or non-deterministic version - see hex.rf.Utils.get{Deter}RNG */
      seed = seed + (init_row<<16);
      Random rand = Utils.getDeterRNG(seed);
      // Now for all rows, classify & vote!
      ROWS: for( int row = 0; row < rows; row++ ) {
        // ------ THIS CODE is crucial and serve to replay the same sequence
        // of random numbers as in the method Data.sampleFair()
        // Skip row used during training if OOB is computed
        float sampledItem = rand.nextFloat();
        if( _computeOOB &&  sampledItem < _model._sample ) continue ROWS;
        // ------

        // Bail out of broken rows
        for( int c = 0; c < _modelDataMap.length; c++ )
          if( _data.isNA(bits, row, cols[_modelDataMap[c]])) continue ROWS;

        // Predict with this tree
        int prediction = _model.classify0(ntree, _data, bits, row, _modelDataMap, numClasses );
        if( prediction >= _N ) continue ROWS; // Junk row cannot be predicted
        if (prediction != (int) _data.data(bits, row, _classcol) - cmin) _errorsPerTree[ntree]++;
        votes[row][prediction]++; // Vote the row
      }
    }

    int validation_rows = 0;
    // Assemble the votes-per-class into predictions & score each row
    _matrix = new long[_N][_N]; // Make an empty confusion matrix for this chunk
    for( int i = 0; i < rows; i++ ) {
      int[] vi = votes[i];
      if( _classWt != null )
        for( int v = 0; v<_N; v++) vi[v] = (int)(vi[v]*_classWt[v]);
      int result = 0, tied = 1;
      for( int l = 1; l<_N; l++)
        if( vi[l] > vi[result] ) { result=l; tied=1; }
        else if( vi[l] == vi[result] ) { tied++; }
      if( vi[result]==0 ) { _skippedRows++; continue; }// Ignore rows with zero votes
      if( tied>1 ) {                // Tie-breaker logic
        int j = _rand.nextInt(tied);
        int k = 0;
        for( int l=0; l<_N; l++ )
          if( vi[l]==vi[result] && (k++ >= j) )  // From zero to number of tied classes-1
            { result = l; break; }
      }
      int cclass = (int) _data.data(bits, i, _classcol) - cmin;
      assert 0 <= cclass && cclass < _N : ("cclass " + cclass + " < " + _N);
      _matrix[cclass][result]++;
      if( result != cclass ) _errors++;
      validation_rows++;
    }

    assert (_rows  == 0) : "Confusion matrix.map(): _rows!=0 ";
    _rows=Math.max(validation_rows,_rows);
  }

  /** Reduction combines the confusion matrices. */
  public void reduce(DRemoteTask drt) {
    Confusion C = (Confusion) drt;
    long[][] m1 = _matrix;
    long[][] m2 = C._matrix;
    if( m1 == null ) _matrix = m2;  // Take other work straight-up
    else {
      for( int i = 0; i < m1.length; i++ )
        for( int j = 0; j < m1.length; j++ )  m1[i][j] += m2[i][j];
    }
    _rows    += C._rows;
    _errors  += C._errors;
    _skippedRows += C._skippedRows;

    // Reduce tree errors
    long[] ept1 = _errorsPerTree;
    long[] ept2 = C._errorsPerTree;
    if (ept1 == null) _errorsPerTree = ept2;
    else if (ept2 != null) {
      if (ept1.length < ept2.length) ept1 = Arrays.copyOf(ept1, ept2.length);
      for (int i = 0; i < ept2.length; i++) ept1[i] += ept2[i];
    }
  }

  /** Text form of the confusion matrix */
  private String confusionMatrix() {
    if( _matrix == null ) return "no trees";
    final int K = _N + 1;
    double[] e2c = new double[_N];
    for( int i = 0; i < _N; i++ ) {
      long err = -_matrix[i][i];
      for( int j = 0; j < _N; j++ )   err += _matrix[i][j];
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

  /** Output information about this RF. */
  public final void report() {
    double err = _errors / (double) _rows;
    String s =
          "              Type of random forest: classification\n"
        + "                    Number of trees: " + _model.size() + "\n"
        + "No of variables tried at each split: " + _model._splitFeatures + "\n"
        + "              Estimate of err. rate: " + Math.round(err * 10000) / 100 + "%  (" + err + ")\n"
        + "                              OOBEE: " + (_computeOOB ? "YES (sampling rate: "+_model._sample*100+"%)" : "NO")+ "\n"
        + "                   Confusion matrix:\n"
        + confusionMatrix() + "\n"
        + "          Avg tree depth (min, max): "  + _model.depth() + "\n"
        + "         Avg tree leaves (min, max): " + _model.leaves() + "\n"
        + "                Validated on (rows): " + _rows + "\n"
        + "     Rows skipped during validation: " + _skippedRows + "\n"
        + "  Mispredictions per tree (in rows): " + Arrays.toString(_errorsPerTree)+"\n";


    Utils.pln(s);
  }

  /** Returns classification error. */
  public float classError() { return _errors / (float) _rows; }
  /** Return number of rows used for CM computation */
  public long rows() { return _rows; }
  /** Return number of skipped rows during CM computation
   *  The number includes in-bag rows if oobee is used. */
  public long skippedRows() { return _skippedRows; }

  /**
   * Reports size of dataset and computed classification error.
   */
  public final void report(StringBuilder sb) {
    double err = _errors / (double) _rows;
    sb.append(_rows).append(',');
    sb.append(err).append(',');
  }
}
