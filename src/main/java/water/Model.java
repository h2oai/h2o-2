package water;

import static water.util.JCodeGen.toStaticVar;
import hex.ConfusionMatrix;
import hex.VarImp;

import java.util.*;

import javassist.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.*;
import water.serial.AutoBufferSerializer;
import water.util.*;
import water.util.Log.Tag.Sys;

/**
 * A Model models reality (hopefully).
 * A model can be used to 'score' a row, or a collection of rows on any
 * compatible dataset - meaning the row has all the columns with the same names
 * as used to build the mode.
 */
public abstract class Model extends Lockable<Model> {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  /** Dataset key used to *build* the model, for models for which this makes
   *  sense, or null otherwise.  Not all models are built from a dataset (eg
   *  artificial models), or are built from a single dataset (various ensemble
   *  models), so this key has no *mathematical* significance in the model but
   *  is handy during common model-building and for the historical record.  */
  @API(help="Datakey used to *build* the model")
  public final Key _dataKey;

  /** Columns used in the model and are used to match up with scoring data
   *  columns.  The last name is the response column name. */
  @API(help="Column names used to build the model")
  public final String _names[];

  /** Categorical/factor/enum mappings, per column.  Null for non-enum cols.
   *  The last column holds the response col enums.  */
  @API(help="Column names used to build the model")
  public final String _domains[][];

  @API(help = "Relative class distribution factors in original data")
  public final float[] _priorClassDist;

  @API(help = "Relative class distribution factors used for model building")
  protected float[] _modelClassDist;
  // WARNING: be really careful to modify this POJO because
  // modification does not involve update in DKV
  public void setModelClassDistribution(float[] classdist) {
    _modelClassDist = classdist.clone();
  }

  private final UniqueId uniqueId;

  /** The start time in mS since the epoch for model training. */
  public long training_start_time = 0L;

  /** The duration in mS for model training. */
  public long training_duration_in_ms = 0L;

  /** Any warnings thrown during model building. */
  @API(help="warnings")
  public String[]  warnings = new String[0];

  /** Whether or not this model has cross-validated results stored. */
  protected boolean _have_cv_results;

  /** Full constructor from frame: Strips out the Vecs to just the names needed
   *  to match columns later for future datasets.
   */
  public Model( Key selfKey, Key dataKey, Frame fr, float[] priorClassDist ) {
    this(selfKey,dataKey,fr.names(),fr.domains(), priorClassDist, null, 0, 0);
  }
  public Model( Key selfKey, Key dataKey, String names[], String domains[][], float[] priorClassDist, float[] modelClassDist) {
    this(selfKey,dataKey,names,domains,priorClassDist,modelClassDist,0,0);
  }
  /** Full constructor */
  public Model( Key selfKey, Key dataKey, String names[], String domains[][], float[] priorClassDist, float[] modelClassDist, long training_start_time, long training_duration_in_ms ) {
    super(selfKey);
    this.uniqueId = new UniqueId(_key);
    if( domains == null ) domains=new String[names.length+1][];
    assert domains.length==names.length;
    assert names.length >= 1;
    assert names[names.length-1] != null; // Have a valid response-column name?
    _dataKey = dataKey;
    _names   = names;
    _domains = domains;
    _priorClassDist = priorClassDist;
    _modelClassDist = modelClassDist;
    this.training_duration_in_ms = training_duration_in_ms;
    this.training_start_time = training_start_time;
  }

  // Currently only implemented by GLM2, DeepLearning, GBM and DRF:
  public Request2 get_params() { throw new UnsupportedOperationException("get_params() has not yet been implemented in class: " + this.getClass()); }

  // NOTE: this is a local copy of the Job; to get the real state you need to get it from the DKV.
  // Currently only implemented by GLM2, DeepLearning, GBM and DRF:
  public Request2 job() { throw new UnsupportedOperationException("job() has not yet been implemented in class: " + this.getClass()); }

  public enum ModelCategory {
    Unknown,
    Binomial,
    Multinomial,
    Regression,
    Clustering;
  }

  // TODO: override in KMeansModel once that's rewritten on water.Model
  public ModelCategory getModelCategory() {
    return (isClassifier() ?
            (nclasses() > 2 ? ModelCategory.Multinomial : ModelCategory.Binomial) :
            ModelCategory.Regression);
  }

  /** Remove any Model internal Keys */
  @Override public Futures delete_impl(Futures fs) { return fs; /* None in the default Model */ }
  @Override public String errStr() { return "Model"; }
  public void addWarning(String warning) {
    if(this.warnings == null || this.warnings.length == 0)
      this.warnings = new String[]{warning};
    else {
      this.warnings = Arrays.copyOf(this.warnings,this.warnings.length+1);
      this.warnings[this.warnings.length-1] = warning;
    }
  }

  public boolean isSupervised() { return true; }

  public UniqueId getUniqueId() {
    return this.uniqueId;
  }

  public void start_training(long training_start_time) {
    Log.info("setting training_start_time to: " + training_start_time + " for Model: " + this._key.toString() + " (" + this.getClass().getSimpleName() + "@" + System.identityHashCode(this) + ")");

    final long t = training_start_time;
    new TAtomic<Model>() {
      @Override public Model atomic(Model m) {
          if (m != null) {
            m.training_start_time = t;
          } return m;
      }
    }.invoke(_key);
    this.training_start_time = training_start_time;
  }
  public void start_training(Model previous) {
    training_start_time = System.currentTimeMillis();
    Log.info("setting training_start_time to: " + training_start_time + " for Model: " + this._key.toString() + " (" + this.getClass().getSimpleName() + "@" + System.identityHashCode(this) + ") [checkpoint case]");
    if (null != previous)
      training_duration_in_ms += previous.training_duration_in_ms;

    final long t = training_start_time;
    final long d = training_duration_in_ms;
    new TAtomic<Model>() {
      @Override public Model atomic(Model m) {
          if (m != null) {
            m.training_start_time = t;
            m.training_duration_in_ms = d;
          } return m;
      }
    }.invoke(_key);
  }
  public void stop_training() {
    training_duration_in_ms += (System.currentTimeMillis() - training_start_time);
    Log.info("setting training_duration_in_ms to: " + training_duration_in_ms + " for Model: " + this._key.toString() + " (" + this.getClass().getSimpleName() + "@" + System.identityHashCode(this) + ")");

    final long d = training_duration_in_ms;
    new TAtomic<Model>() {
      @Override public Model atomic(Model m) {
          if (m != null) {
            m.training_duration_in_ms = d;
          } return m;
      }
    }.invoke(_key);
  }

  public String responseName() { return   _names[  _names.length-1]; }
  public String[] classNames() { return _domains[_domains.length-1]; }
  public boolean isClassifier() { return classNames() != null ; }
  public int nclasses() {
    String cns[] = classNames();
    return cns==null ? 1 : cns.length;
  }
  /** Returns number of input features */
  public int nfeatures() { return _names.length - 1; }

  /** For classifiers, confusion matrix on validation set. */
  public ConfusionMatrix cm() { return null; }
  /** Returns mse for validation set. */
  public double mse() { return Double.NaN; }
  /** Variable importance of individual input features measured by this model. */
  public VarImp varimp() { return null; }

  public boolean hasCrossValModels() { return _have_cv_results; }

  /** Bulk score for given <code>fr</code> frame.
   * The frame is always adapted to this model.
   *
   * @param fr frame to be scored
   * @return frame holding predicted values
   *
   * @see #score(Frame, boolean)
   */
  public Frame score(Frame fr) {
    return score(fr, true);
  }
  /** Bulk score the frame <code>fr</code>, producing a Frame result; the 1st Vec is the
   *  predicted class, the remaining Vecs are the probability distributions.
   *  For Regression (single-class) models, the 1st and only Vec is the
   *  prediction value.
   *
   *  The flat <code>adapt</code>
   * @param fr frame which should be scored
   * @param adapt a flag enforcing an adaptation of <code>fr</code> to this model. If flag
   *        is <code>false</code> scoring code expect that <code>fr</code> is already adapted.
   * @return a new frame containing a predicted values. For classification it contains a column with
   *         prediction and distribution for all response classes. For regression it contains only
   *         one column with predicted values.
   */
  public final Frame score(Frame fr, boolean adapt) {
    if (isSupervised()) {
      int ridx = fr.find(responseName());
      if (ridx != -1) { // drop the response for scoring!
        fr = new Frame(fr);
        fr.remove(ridx);
      }
    }
    // Adapt the Frame layout - returns adapted frame and frame containing only
    // newly created vectors
    Frame[] adaptFrms = adapt ? adapt(fr,false) : null;
    // Adapted frame containing all columns - mix of original vectors from fr
    // and newly created vectors serving as adaptors
    Frame adaptFrm = adapt ? adaptFrms[0] : fr;
    // Contains only newly created vectors. The frame eases deletion of these vectors.
    Frame onlyAdaptFrm = adapt ? adaptFrms[1] : null;
    // Invoke scoring
    Frame output = scoreImpl(adaptFrm);
    // Be nice to DKV and delete vectors which i created :-)
    if (adapt) onlyAdaptFrm.delete();
    return output;
  }

  /** Score already adapted frame.
   *
   * @param adaptFrm
   * @return
   */
  protected Frame scoreImpl(Frame adaptFrm) {
    if (isSupervised()) {
      int ridx = adaptFrm.find(responseName());
      assert ridx == -1 : "Adapted frame should not contain response in scoring method!";
      assert nfeatures() == adaptFrm.numCols() : "Number of model features " + nfeatures() + " != number of test set columns: " + adaptFrm.numCols();
      assert adaptFrm.vecs().length == nfeatures() : "Scoring data set contains wrong number of columns: " + adaptFrm.vecs().length + " instead of " + nfeatures();
    }
    // Create a new vector for response
    // If the model produces a classification/enum, copy the domain into the
    // result vector.
    int nc = nclasses();
    Vec [] newVecs = new Vec[]{adaptFrm.anyVec().makeZero(classNames())};
    if(nc > 1)
      newVecs = Utils.join(newVecs,adaptFrm.anyVec().makeZeros(nc));
    String [] names = new String[newVecs.length];
    names[0] = "predict";
    for(int i = 1; i < names.length; ++i)
      names[i] = classNames()[i-1];
    final int num_features = nfeatures();
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        double tmp [] = new double[num_features]; // We do not need the last field representing response
        float preds[] = new float [nclasses()==1?1:nclasses()+1];
        int len = chks[0]._len;
        for( int row=0; row<len; row++ ) {
          float p[] = score0(chks,row,tmp,preds);
          for( int c=0; c<preds.length; c++ )
            chks[num_features+c].set0(row,p[c]);
        }
      }
    }.doAll(Utils.join(adaptFrm.vecs(),newVecs));
    // Return just the output columns
    return new Frame(names,newVecs);
  }

  /** Single row scoring, on a compatible Frame.  */
  public final float[] score( Frame fr, boolean exact, int row ) {
    double tmp[] = new double[fr.numCols()];
    for( int i=0; i<tmp.length; i++ )
      tmp[i] = fr.vecs()[i].at(row);
    return score(fr.names(),fr.domains(),exact,tmp);
  }

  /** Single row scoring, on a compatible set of data.  Fairly expensive to adapt. */
  public final float[] score( String names[], String domains[][], boolean exact, double row[] ) {
    return score(adapt(names,domains,exact),row,new float[nclasses()]);
  }

  /** Single row scoring, on a compatible set of data, given an adaption vector */
  public final float[] score( int map[][][], double row[], float[] preds ) {
    /*FIXME final int[][] colMap = map[map.length-1]; // Response column mapping is the last array
    assert colMap.length == _names.length-1 : " "+Arrays.toString(colMap)+" "+Arrays.toString(_names);
    double tmp[] = new double[colMap.length]; // The adapted data
    for( int i=0; i<colMap.length; i++ ) {
      // Column mapping, or NaN for missing columns
      double d = colMap[i]==-1 ? Double.NaN : row[colMap[i]];
      if( map[i] != null ) {    // Enum mapping
        int e = (int)d;
        if( e < 0 || e >= map[i].length ) d = Double.NaN; // User data is out of adapt range
        else {
          e = map[i][e];
          d = e==-1 ? Double.NaN : (double)e;
        }
      }
      tmp[i] = d;
    }
    return score0(tmp,preds);   // The results. */
    return null;
  }

  /** Build an adaption array.  The length is equal to the Model's vector length.
   *  Each inner 2D-array is a
   *  compressed domain map from data domains to model domains - or null for non-enum
   *  columns, or null for identity mappings.  The extra final int[] is the
   *  column mapping itself, mapping from model columns to data columns. or -1
   *  if missing.
   *  If 'exact' is true, will throw if there are:
   *    any columns in the model but not in the input set;
   *    any enums in the data that the model does not understand
   *    any enums returned by the model that the data does not have a mapping for.
   *  If 'exact' is false, these situations will use or return NA's instead.
   */
  private int[][][] adapt( String names[], String domains[][], boolean exact) {
    int maplen = names.length;
    int map[][][] = new int[maplen][][];
    // Make sure all are compatible
    for( int c=0; c<names.length;++c) {
            // Now do domain mapping
      String ms[] = _domains[c];  // Model enum
      String ds[] =  domains[c];  // Data  enum
      if( ms == ds ) { // Domains trivially equal?
      } else if( ms == null ) {
        throw new IllegalArgumentException("Incompatible column: '" + _names[c] + "', expected (trained on) numeric, was passed a categorical");
      } else if( ds == null ) {
        if( exact )
          throw new IllegalArgumentException("Incompatible column: '" + _names[c] + "', expected (trained on) categorical, was passed a numeric");
        throw H2O.unimpl();     // Attempt an asEnum?
      } else if( !Arrays.deepEquals(ms, ds) ) {
        map[c] = getDomainMapping(_names[c], ms, ds, exact);
      } // null mapping is equal to identity mapping
    }
    return map;
  }

  /**
   * Type of missing columns during adaptation between train/test datasets
   * Overload this method for models that have sparse data handling.
   * Otherwise, NaN is used.
   * @return real-valued number (can be NaN)
   */
  protected double missingColumnsType() { return Double.NaN; }

  /** Build an adapted Frame from the given Frame. Useful for efficient bulk
   *  scoring of a new dataset to an existing model.  Same adaption as above,
   *  but expressed as a Frame instead of as an int[][]. The returned Frame
   *  does not have a response column.
   *  It returns a <b>two element array</b> containing an adapted frame and a
   *  frame which contains only vectors which where adapted (the purpose of the
   *  second frame is to delete all adapted vectors with deletion of the
   *  frame). */
  public Frame[] adapt( final Frame fr, boolean exact) {
    return adapt(fr, exact, true);
  }

  public Frame[] adapt( final Frame fr, boolean exact, boolean haveResponse) {
    Frame vfr = new Frame(fr); // To avoid modification of original frame fr
    int n = _names.length;
    if (haveResponse && isSupervised()) {
      int ridx = vfr.find(_names[_names.length - 1]);
      if (ridx != -1 && ridx != vfr._names.length - 1) { // Unify frame - put response to the end
        String name = vfr._names[ridx];
        vfr.add(name, vfr.remove(ridx));
      }
      n = ridx == -1 ? _names.length - 1 : _names.length;
    }
    String [] names = isSupervised() ? Arrays.copyOf(_names, n) : _names.clone();
    Frame  [] subVfr;
    // replace missing columns with NaNs (or 0s for DeepLearning with sparse data)
    subVfr = vfr.subframe(names, missingColumnsType());
    vfr = subVfr[0]; // extract only subframe but keep the rest for delete later
    Vec[] frvecs = vfr.vecs();
    boolean[] toEnum = new boolean[frvecs.length];
    if(!exact) for(int i = 0; i < n;++i)
      if(_domains[i] != null && !frvecs[i].isEnum()) {// if model expects domain but input frame does not have domain => switch vector to enum
        frvecs[i] = frvecs[i].toEnum();
        toEnum[i] = true;
      }
    int[][][] map = adapt(names,vfr.domains(),exact);
    assert map.length == names.length; // Be sure that adapt call above do not skip any column
    ArrayList<Vec> avecs = new ArrayList<Vec>(); // adapted vectors
    ArrayList<String> anames = new ArrayList<String>(); // names for adapted vector

    for( int c=0; c<map.length; c++ ) // Iterate over columns
      if(map[c] != null) { // Column needs adaptation
        Vec adaptedVec;
        if (toEnum[c]) { // Vector was flipped to column already, compose transformation
          adaptedVec = TransfVec.compose( (TransfVec) frvecs[c], map[c], vfr.domains()[c], false);
        } else adaptedVec = frvecs[c].makeTransf(map[c], vfr.domains()[c]);
        avecs.add(frvecs[c] = adaptedVec);
        anames.add(names[c]); // Collect right names
      } else if (toEnum[c]) { // Vector was transformed to enum domain, but does not need adaptation we need to record it
        avecs.add(frvecs[c]);
        anames.add(names[c]);
      }
    // Fill trash bin by vectors which need to be deleted later by the caller.
    Frame vecTrash = new Frame(anames.toArray(new String[anames.size()]), avecs.toArray(new Vec[avecs.size()]));
    if (subVfr[1]!=null) vecTrash.add(subVfr[1], true);
    return new Frame[] { new Frame(names,frvecs), vecTrash };
  }

  /** Returns a mapping between values of model domains (<code>modelDom</code>) and given column domain.
   * @see #getDomainMapping(String, String[], String[], boolean) */
  public static int[][] getDomainMapping(String[] modelDom, String[] colDom, boolean exact) {
    return getDomainMapping(null, modelDom, colDom, exact);
  }
  /**
   * Returns a mapping for given column according to given <code>modelDom</code>.
   * In this case, <code>modelDom</code> is
   *
   * @param colName name of column which is mapped, can be null.
   * @param modelDom
   * @param logNonExactMapping
   * @return
   */
  public static int[][] getDomainMapping(String colName, String[] modelDom, String[] colDom, boolean logNonExactMapping) {
    int emap[] = new int[modelDom.length];
    boolean bmap[] = new boolean[modelDom.length];
    HashMap<String,Integer> md = new HashMap<String, Integer>((int) ((colDom.length/0.75f)+1));
    for( int i = 0; i < colDom.length; i++) md.put(colDom[i], i);
    for( int i = 0; i < modelDom.length; i++) {
      Integer I = md.get(modelDom[i]);
      if (I == null && logNonExactMapping)
        Log.warn(Sys.SCORM, "Domain mapping: target domain contains the factor '"+modelDom[i]+"' which DOES NOT appear in input domain " + (colName!=null?"(column: " + colName+")":""));
      if (I!=null) {
        emap[i] = I;
        bmap[i] = true;
      }
    }
    if (logNonExactMapping) { // Inform about additional values in column domain which do not appear in model domain
      for (int i=0; i<colDom.length; i++) {
        boolean found = false;
        for (int j=0; j<emap.length; j++)
          if (emap[j]==i) { found=true; break; }
        if (!found)
          Log.warn(Sys.SCORM, "Domain mapping: target domain DOES NOT contain the factor '"+colDom[i]+"' which appears in input domain "+ (colName!=null?"(column: " + colName+")":""));
      }
    }

    // produce packed values
    int[][] res = Utils.pack(emap, bmap);
    // Sort values in numeric order to support binary search in TransfVec
    Utils.sortWith(res[0], res[1]);
    return res;
  }

  /** Bulk scoring API for one row.  Chunks are all compatible with the model,
   *  and expect the last Chunks are for the final distribution and prediction.
   *  Default method is to just load the data into the tmp array, then call
   *  subclass scoring logic. */
  protected float[] score0( Chunk chks[], int row_in_chunk, double[] tmp, float[] preds ) {
    assert chks.length>=_names.length; // Last chunk is for the response
    for( int i=0; i<nfeatures(); i++ ) // Do not include last value since it can contains a response
      tmp[i] = chks[i].at0(row_in_chunk);
    float[] scored = score0(tmp,preds);
    // Correct probabilities obtained from training on oversampled data back to original distribution
    // C.f. http://gking.harvard.edu/files/0s.pdf Eq.(27)
    if (isClassifier() && _priorClassDist != null && _modelClassDist != null) {
      assert(scored.length == nclasses()+1); //1 label + nclasses probs
      ModelUtils.correctProbabilities(scored, _priorClassDist, _modelClassDist);
      //set label based on corrected probabilities (max value wins, with deterministic tie-breaking)
      scored[0] = ModelUtils.getPrediction(scored, tmp);
    }
    return scored;
  }

  /**
   * Compute the model error for a given test data set
   * For multi-class classification, this is the classification error based on assigning labels for the highest predicted per-class probability.
   * For binary classification, this is the classification error based on assigning labels using the optimal threshold for maximizing the F1 score.
   * For regression, this is the mean squared error (MSE).
   * @param ftest Frame containing test data
   * @param vactual The response column Vec
   * @param fpreds Frame containing ADAPTED (domain labels from train+test data) predicted data (classification: label + per-class probabilities, regression: target)
   * @param hitratio_fpreds Frame containing predicted data (domain labels from test data) (classification: label + per-class probabilities, regression: target)
   * @param label Name for the scored data set to be printed
   * @param printMe Whether to print the scoring results to Log.info
   * @param max_conf_mat_size Largest size of Confusion Matrix (#classes) for it to be printed to Log.info
   * @param cm Confusion Matrix object to populate for multi-class classification (also used for regression)
   * @param auc AUC object to populate for binary classification
   * @param hr HitRatio object to populate for classification
   * @return model error, see description above
   */
  public double calcError(final Frame ftest, final Vec vactual,
                          final Frame fpreds, final Frame hitratio_fpreds,
                          final String label, final boolean printMe,
                          final int max_conf_mat_size, final water.api.ConfusionMatrix cm,
                          final AUC auc,
                          final HitRatio hr)
  {
    StringBuilder sb = new StringBuilder();
    double error = Double.POSITIVE_INFINITY;
    // populate AUC
    if (auc != null) {
      assert(isClassifier());
      assert(nclasses() == 2);
      auc.actual = ftest;
      auc.vactual = vactual;
      auc.predict = fpreds;
      auc.vpredict = fpreds.vecs()[2]; //binary classifier (label, prob0, prob1 (THIS ONE), adaptedlabel)
      auc.invoke();
      auc.toASCII(sb);
      error = auc.data().err(); //using optimal threshold for F1
    }
    // populate CM
    if (cm != null) {
      cm.actual = ftest;
      cm.vactual = vactual;
      cm.predict = fpreds;
      cm.vpredict = fpreds.vecs()[0]; // prediction (either label or regression target)
      cm.invoke();
      if (isClassifier()) {
        if (auc != null) {
          AUCData aucd = auc.data();
          //override the CM with the one computed by AUC (using optimal threshold)
          //Note: must still call invoke above to set the domains etc.
          cm.cm = new long[3][3]; // 1 extra layer for NaNs (not populated here, since AUC skips them)
          cm.cm[0][0] = aucd.cm()[0][0];
          cm.cm[1][0] = aucd.cm()[1][0];
          cm.cm[0][1] = aucd.cm()[0][1];
          cm.cm[1][1] = aucd.cm()[1][1];
          double cm_err = new hex.ConfusionMatrix(cm.cm).err();
          double auc_err = aucd.err();
          if (! (Double.isNaN(cm_err) && Double.isNaN(auc_err))) // NOTE: NaN != NaN
            assert(cm_err == auc_err); //check consistency with AUC-computed error
        } else {
          error = new hex.ConfusionMatrix(cm.cm).err(); //only set error if AUC didn't already set the error
        }
        if (cm.cm.length <= max_conf_mat_size+1) cm.toASCII(sb);
      } else {
        assert(auc == null);
        error = cm.mse;
        cm.toASCII(sb);
      }
    }
    // populate HitRatio
    if (hr != null) {
      assert(isClassifier());
      hr.actual = ftest;
      hr.vactual = vactual;
      hr.predict = hitratio_fpreds;
      hr.invoke();
      hr.toASCII(sb);
    }
    if (printMe && sb.length() > 0) {
      Log.info("Scoring on " + label + " data:");
      for (String s : sb.toString().split("\n")) Log.info(s);
    }
    return error;
  }

  /** Subclasses implement the scoring logic.  The data is pre-loaded into a
   *  re-used temp array, in the order the model expects.  The predictions are
   *  loaded into the re-used temp array, which is also returned.  */
  protected abstract float[] score0(double data[/*ncols*/], float preds[/*nclasses+1*/]);
  // Version where the user has just ponied-up an array of data to be scored.
  // Data must be in proper order.  Handy for JUnit tests.
  public double score(double [] data){ return Utils.maxIndex(score0(data,new float[nclasses()]));  }

  /** Debug flag to generate benchmar code */
  protected static final boolean GEN_BENCHMARK_CODE = false;

  /** Return a String which is a valid Java program representing a class that
   *  implements the Model.  The Java is of the form:
   *  <pre>
   *    class UUIDxxxxModel {
   *      public static final String NAMES[] = { ....column names... }
   *      public static final String DOMAINS[][] = { ....domain names... }
   *      // Pass in data in a double[], pre-aligned to the Model's requirements.
   *      // Jam predictions into the preds[] array; preds[0] is reserved for the
   *      // main prediction (class for classifiers or value for regression),
   *      // and remaining columns hold a probability distribution for classifiers.
   *      float[] predict( double data[], float preds[] );
   *      double[] map( HashMap &lt; String,Double &gt; row, double data[] );
   *      // Does the mapping lookup for every row, no allocation
   *      float[] predict( HashMap &lt; String,Double &gt; row, double data[], float preds[] );
   *      // Allocates a double[] for every row
   *      float[] predict( HashMap &lt; String,Double &gt; row, float preds[] );
   *      // Allocates a double[] and a float[] for every row
   *      float[] predict( HashMap &lt; String,Double &gt; row );
   *    }
   *  </pre>
   */
  public String toJava() { return toJava(new SB()).toString(); }
  public SB toJava( SB sb ) {
    SB fileContextSB = new SB(); // preserve file context
    String modelName = JCodeGen.toJavaId(_key.toString());
    // HEADER
    sb.p("import java.util.Map;").nl();
    sb.p("import water.genmodel.GenUtils.*;").nl().nl();
    sb.p("// AUTOGENERATED BY H2O at ").p(new Date().toString()).nl();
    sb.p("// ").p(H2O.getBuildVersion().toString()).nl();
    sb.p("//").nl();
    sb.p("// Standalone prediction code with sample test data for ").p(this.getClass().getSimpleName()).p(" named ").p(modelName).nl();
    sb.p("//").nl();
    sb.p("// How to download, compile and execute:").nl();
    sb.p("//     mkdir tmpdir").nl();
    sb.p("//     cd tmpdir").nl();
    sb.p("//     curl http:/").p(H2O.SELF.toString()).p("/h2o-model.jar > h2o-model.jar").nl();
    sb.p("//     curl http:/").p(H2O.SELF.toString()).p("/2/").p(this.getClass().getSimpleName()).p("View.java?_modelKey=").pobj(_key).p(" > ").p(modelName).p(".java").nl();
    sb.p("//     javac -cp h2o-model.jar -J-Xmx2g -J-XX:MaxPermSize=128m ").p(modelName).p(".java").nl();
    if (GEN_BENCHMARK_CODE)
    sb.p("//     java -cp h2o-model.jar:. -Xmx2g -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=256m ").p(modelName).nl();
    sb.p("//").nl();
    sb.p("//     (Note:  Try java argument -XX:+PrintCompilation to show runtime JIT compiler behavior.)").nl();
    sb.nl();
    sb.p("public class ").p(modelName).p(" extends water.genmodel.GeneratedModel {").nl(); // or extends GenerateModel
    toJavaInit(sb, fileContextSB).nl();
    toJavaNAMES(sb, fileContextSB);
    toJavaNCLASSES(sb);
    toJavaDOMAINS(sb, fileContextSB);
    toJavaPROB(sb);
    toJavaSuper(sb); //
    toJavaPredict(sb, fileContextSB);
    sb.p("}").nl();
    sb.p(fileContextSB).nl(); // Append file
    return sb;
  }

  /** Generate implementation for super class. */
  protected SB toJavaSuper( SB sb ) {
    sb.nl();
    sb.ii(1);
    sb.i().p("public String[]   getNames() { return NAMES; } ").nl();
    sb.i().p("public String[][] getDomainValues() { return DOMAINS; }").nl();
    String uuid = this.uniqueId != null ? this.uniqueId.getId() : this._key.toString();
    sb.i().p("public String     getUUID() { return ").ps(uuid).p("; }").nl();

    return sb;
  }
  private SB toJavaNAMES(SB sb, SB fileContextSB) {
    String namesHolderClassName = "NamesHolder";
    sb.i().p("// ").p("Names of columns used by model.").nl();
    sb.i().p("public static final String[] NAMES = NamesHolder.VALUES;").nl();
    // Generate class which fills the names into array
    fileContextSB.i().p("// The class representing training column names ").nl();
    JCodeGen.toClassWithArray(fileContextSB, null, namesHolderClassName, _names);
    return sb;
  }
  protected SB toJavaNCLASSES( SB sb ) { return isClassifier() ? JCodeGen.toStaticVar(sb, "NCLASSES", nclasses(), "Number of output classes included in training data response column.") : sb; }
  private SB toJavaDOMAINS( SB sb, SB fileContextSB ) {
    sb.nl();
    sb.ii(1);
    sb.i().p("// Column domains. The last array contains domain of response column.").nl();
    sb.i().p("public static final String[][] DOMAINS = new String[][] {").nl();
    for (int i=0; i<_domains.length; i++) {
      String[] dom = _domains[i];
      String colInfoClazz = "ColInfo_"+i;
      sb.i(1).p("/* ").p(_names[i]).p(" */ ");
      if (dom != null) sb.p(colInfoClazz).p(".VALUES"); else sb.p("null");
      if (i!=_domains.length-1) sb.p(',');
      sb.nl();
      if (dom != null) {
        fileContextSB.i().p("// The class representing column ").p(_names[i]).nl();
        JCodeGen.toClassWithArray(fileContextSB, null, colInfoClazz, dom);
      }
    }
    return sb.i().p("};").nl();
  }
  private SB toJavaPROB( SB sb) {
    sb.di(1);
    toStaticVar(sb, "PRIOR_CLASS_DISTRIB", _priorClassDist, "Prior class distribution");
    toStaticVar(sb, "MODEL_CLASS_DISTRIB", _modelClassDist, "Class distribution used for model building");
    return sb;
  }
  // Override in subclasses to provide some top-level model-specific goodness
  protected SB toJavaInit(SB sb, SB fileContextSB) { return sb; }
  protected void toJavaInit(CtClass ct) { }
  // Override in subclasses to provide some inside 'predict' call goodness
  // Method returns code which should be appended into generated top level class after
  // predict method.
  protected void toJavaPredictBody(SB bodySb, SB classCtxSb, SB fileCtxSb) {
    throw new IllegalArgumentException("This model type does not support conversion to Java");
  }
  // Wrapper around the main predict call, including the signature and return value
  private SB toJavaPredict(SB ccsb, SB fileCtxSb) { // ccsb = classContext
    ccsb.nl();
    ccsb.p("  // Pass in data in a double[], pre-aligned to the Model's requirements.").nl();
    ccsb.p("  // Jam predictions into the preds[] array; preds[0] is reserved for the").nl();
    ccsb.p("  // main prediction (class for classifiers or value for regression),").nl();
    ccsb.p("  // and remaining columns hold a probability distribution for classifiers.").nl();
    ccsb.p("  public final float[] predict( double[] data, float[] preds) { preds = predict( data, preds, "+toJavaDefaultMaxIters()+"); return preds; }").nl();
//    ccsb.p("  public final float[] predict( double[] data, float[] preds) { return predict( data, preds, "+toJavaDefaultMaxIters()+"); }").nl();
    ccsb.p("  public final float[] predict( double[] data, float[] preds, int maxIters ) {").nl();
    SB classCtxSb = new SB();
    toJavaPredictBody(ccsb.ii(1), classCtxSb, fileCtxSb); ccsb.di(1);
    ccsb.p("    return preds;").nl();
    ccsb.p("  }").nl();
    ccsb.p(classCtxSb);
    return ccsb;
  }

  protected String toJavaDefaultMaxIters() { return "-1"; }

  /** Generates code which unify preds[1,...NCLASSES] */
  protected void toJavaUnifyPreds(SB bodySb) {
  }
  /** Fill preds[0] based on already filled and unified preds[1,..NCLASSES]. */
  protected void toJavaFillPreds0(SB bodySb) {
    // Pick max index as a prediction
    if (isClassifier()) {
      if (_priorClassDist!=null && _modelClassDist!=null) {
        bodySb.i().p("water.util.ModelUtils.correctProbabilities(preds, PRIOR_CLASS_DISTRIB, MODEL_CLASS_DISTRIB);").nl();
      }
      bodySb.i().p("preds[0] = water.util.ModelUtils.getPrediction(preds,data);").nl();
    } else {
      bodySb.i().p("preds[0] = preds[1];").nl();
    }
  }

  /**
   * Compute the cross validation error from an array of predictions for N folds.
   * Also stores the results in the model for display/query.
   * @param source Full training data
   * @param response Full response
   * @param cv_preds N Frames containing predictions made by N-fold CV runs on disjoint contiguous holdout pieces of the training data
   * @param offsets Starting row numbers for the N CV pieces (length = N+1, first element: 0, last element: #rows)
   */
  public final void scoreCrossValidation(Job.ValidatedJob job, Frame source, Vec response, Frame[] cv_preds, long[] offsets) {
    assert(offsets[0] == 0);
    assert(offsets[offsets.length-1] == source.numRows());

    //Hack to make a frame with the correct dimensions and vector group
    Frame cv_pred = score(source);

    // Stitch together the content of cv_pred from cv_preds
    for (int i=0; i<cv_preds.length; ++i) {
      // stitch probabilities (or regression values)
      for (int c=(isClassifier() ? 1 : 0); c<cv_preds[i].numCols(); ++c) {
        Vec.Writer vw = cv_pred.vec(c).open();
        try {
          for (long r=0; r < cv_preds[i].numRows(); ++r) {
            vw.set(offsets[i] + r, cv_preds[i].vec(c).at(r));
          }
        } finally {
          vw.close();
        }
      }
      if (isClassifier()) {
        // make labels
        float[] probs = new float[cv_preds[i].numCols()];
        Vec.Writer vw = cv_pred.vec(0).open();
        try {
          for (long r = 0; r < cv_preds[i].numRows(); ++r) {
            //probs[0] stays 0, is not used in getPrediction
            for (int c = 1; c < cv_preds[i].numCols(); ++c) {
              probs[c] = (float) cv_preds[i].vec(c).at(r);
            }
            final int label = ModelUtils.getPrediction(probs, (int)r);
            vw.set(offsets[i] + r, label);
          }
        } finally {
          vw.close();
        }
      }
    }

    // Now score the model on the N folds
    try {
      AUC auc = nclasses() == 2 ? new AUC() : null;
      water.api.ConfusionMatrix cm = new water.api.ConfusionMatrix();
      HitRatio hr = isClassifier() ? new HitRatio() : null;
      double cv_error = calcError(source, response, cv_pred, cv_pred, "cross-validated", true, 10, cm, auc, hr);
      setCrossValidationError(job, cv_error, cm, auc == null ? null : auc.data(), hr);
    } finally {
      // cleanup temporary frame wit predictions
      cv_pred.delete();
    }
  }

  protected void setCrossValidationError(Job.ValidatedJob job, double cv_error, water.api.ConfusionMatrix cm, AUCData auc, HitRatio hr) { throw H2O.unimpl(); }

  protected void printCrossValidationModelsHTML(StringBuilder sb) {
    if (job() == null) return;
    Job.ValidatedJob job = (Job.ValidatedJob)job();
    if (job.xval_models != null && job.xval_models.length > 0) {
      sb.append("<h4>Cross Validation Models</h4>");
      sb.append("<table class='table table-bordered table-condensed'>");
      sb.append("<tr><th>Model</th></tr>");
      for (Key k : job.xval_models) {
        Model m = UKV.get(k);
        Job j = m != null ? (Job)m.job() : null;
        sb.append("<tr>");
        sb.append("<td>" + (m != null ? Inspector.link(k.toString(), k.toString()) : "Pending") + (j != null ? ", Progress: " + Utils.formatPct(j.progress()) : "") + "</td>");
        sb.append("</tr>");
      }
      sb.append("</table>");
    }
  }

  /** Helper type for serialization */
  protected static class ModelAutobufferSerializer extends AutoBufferSerializer<Model> { }

  /** Returns a model serializer into AutoBuffer. */
  public AutoBufferSerializer<Model> getModelSerializer() {
    return new ModelAutobufferSerializer();
  }
}
