package water;

import static water.util.Utils.contains;
import hex.ConfusionMatrix;
import hex.VarImp;
import hex.deeplearning.DeepLearningModel;
import javassist.*;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.TransfVec;
import water.fvec.Vec;
import water.util.*;
import water.util.Log.Tag.Sys;

import java.util.*;

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
  final protected float[] _priorClassDist;
  @API(help = "Relative class distribution factors used for model building")
  protected float[] _modelClassDist;
  public void setModelClassDistribution(float[] classdist) {
    _modelClassDist = classdist.clone();
  }

  private final UniqueId uniqueId;

  /** Full constructor from frame: Strips out the Vecs to just the names needed
   *  to match columns later for future datasets.  */
  public Model( Key selfKey, Key dataKey, Frame fr, float[] priorClassDist ) {
    this(selfKey,dataKey,fr.names(),fr.domains(),priorClassDist);
  }

  /** Constructor from frame (without prior class dist): Strips out the Vecs to just the names needed
   *  to match columns later for future datasets.  */
  public Model( Key selfKey, Key dataKey, Frame fr ) {
    this(selfKey,dataKey,fr.names(),fr.domains(),null);
  }

  /** Constructor without prior class distribution */
  public Model( Key selfKey, Key dataKey, String names[], String domains[][]) {
    this(selfKey,dataKey,names,domains,null);
  }

  /** Full constructor */
  public Model( Key selfKey, Key dataKey, String names[], String domains[][], float[] priorClassDist ) {
    super(selfKey);
    this.uniqueId = new UniqueId(_key);
    if( domains == null ) domains=new String[names.length+1][];
    assert domains.length==names.length;
    assert names.length > 1;
    assert names[names.length-1] != null; // Have a valid response-column name?
    _dataKey = dataKey;
    _names   = names;
    _domains = domains;
    _priorClassDist = priorClassDist;
  }

  // currently only implemented by GLM2, DeepLearning, GBM and DRF:
  public Request2 get_params() { throw new UnsupportedOperationException("get_params() has not yet been implemented in class: " + this.getClass()); }
  public Request2 job() { throw new UnsupportedOperationException("job() has not yet been implemented in class: " + this.getClass()); }

  /** Simple shallow copy constructor to a new Key */
  public Model( Key selfKey, Model m ) { this(selfKey,m._dataKey,m._names,m._domains); }

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

  public UniqueId getUniqueId() {
    return this.uniqueId;
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

  /** Bulk score for given <code>fr</code> frame.
   * The frame is always adapted to this model.
   *
   * @param fr frame to be scored
   * @return frame holding predicted values
   *
   * @see #score(Frame, boolean)
   */
  public final Frame score(Frame fr) {
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
    int ridx = fr.find(responseName());
    if (ridx != -1) { // drop the response for scoring!
      fr = new Frame(fr);
      fr.remove(ridx);
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
  private Frame scoreImpl(Frame adaptFrm) {
    int ridx = adaptFrm.find(responseName());
    assert ridx == -1 : "Adapted frame should not contain response in scoring method!";
    assert nfeatures() == adaptFrm.numCols() : "Number of model features " + nfeatures() + " != number of test set columns: " + adaptFrm.numCols();
    assert adaptFrm.vecs().length == _names.length-1 : "Scoring data set contains wrong number of columns: " + adaptFrm.vecs().length  + " instead of " + (_names.length-1);

    // Create a new vector for response
    // If the model produces a classification/enum, copy the domain into the
    // result vector.
    Vec v = adaptFrm.anyVec().makeZero(classNames());
    adaptFrm.add("predict",v);
    if( nclasses() > 1 ) {
      String prefix = "";
      for( int c=0; c<nclasses(); c++ ) // if any class is the same as column name in frame, then prefix all classnames
        if (contains(adaptFrm._names, classNames()[c])) { prefix = "class_"; break; }
      for( int c=0; c<nclasses(); c++ )
        adaptFrm.add(prefix+classNames()[c],adaptFrm.anyVec().makeZero());
    }
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        double tmp [] = new double[_names.length-1]; // We do not need the last field representing response
        float preds[] = new float [nclasses()==1?1:nclasses()+1];
        int len = chks[0]._len;
        for( int row=0; row<len; row++ ) {
          float p[] = score0(chks,row,tmp,preds);
          for( int c=0; c<preds.length; c++ )
            chks[_names.length-1+c].set0(row,p[c]);
        }
      }
    }.doAll(adaptFrm);
    // Return just the output columns
    int x=_names.length-1, y=adaptFrm.numCols();
    return adaptFrm.extractFrame(x, y);
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

  /** Build an adapted Frame from the given Frame. Useful for efficient bulk
   *  scoring of a new dataset to an existing model.  Same adaption as above,
   *  but expressed as a Frame instead of as an int[][]. The returned Frame
   *  does not have a response column.
   *  It returns a <b>two element array</b> containing an adapted frame and a
   *  frame which contains only vectors which where adapted (the purpose of the
   *  second frame is to delete all adapted vectors with deletion of the
   *  frame). */
  public Frame[] adapt( final Frame fr, boolean exact) {
    Frame vfr = new Frame(fr); // To avoid modification of original frame fr
    int ridx = vfr.find(_names[_names.length-1]);
    if(ridx != -1 && ridx != vfr._names.length-1){ // Unify frame - put response to the end
      String n = vfr._names[ridx];
      vfr.add(n,vfr.remove(ridx));
    }
    int n = ridx == -1?_names.length-1:_names.length;
    String [] names = Arrays.copyOf(_names, n);
    Frame  [] subVfr;
    // replace missing columns with NaNs (or 0s for DeepLearning with sparse data)
    subVfr = vfr.subframe(names, (this instanceof DeepLearningModel && ((DeepLearningModel)this).get_params().sparse) ? 0 : Double.NaN);
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
    for( int i=0; i<_names.length-1; i++ ) // Do not include last value since it can contains a response
      tmp[i] = chks[i].at0(row_in_chunk);
    float[] scored = score0(tmp,preds);
    // Correct probabilities obtained from training on oversampled data back to original distribution
    // C.f. http://gking.harvard.edu/files/0s.pdf Eq.(27)
    if (isClassifier() && _priorClassDist != null && _modelClassDist != null) {
      assert(scored.length == nclasses()+1); //1 label + nclasses probs
      double probsum=0;
      for( int c=1; c<scored.length; c++ ) {
        final double original_fraction = _priorClassDist[c-1];
        assert(original_fraction > 0);
        final double oversampled_fraction = _modelClassDist[c-1];
        assert(oversampled_fraction > 0);
        assert(!Double.isNaN(scored[c]));
        scored[c] *= original_fraction / oversampled_fraction;
        probsum += scored[c];
      }
      for (int i=1;i<scored.length;++i) scored[i] /= probsum;
      //set label based on corrected probabilities (max value wins, with deterministic tie-breaking)
      scored[0] = ModelUtils.getPrediction(scored, tmp);
    }
    return scored;
  }

  /** Subclasses implement the scoring logic.  The data is pre-loaded into a
   *  re-used temp array, in the order the model expects.  The predictions are
   *  loaded into the re-used temp array, which is also returned.  */
  protected abstract float[] score0(double data[/*ncols*/], float preds[/*nclasses+1*/]);
  // Version where the user has just ponied-up an array of data to be scored.
  // Data must be in proper order.  Handy for JUnit tests.
  public double score(double [] data){ return Utils.maxIndex(score0(data,new float[nclasses()]));  }


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
    sb.p("import java.util.Map;").nl().nl();
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
    sb.p("//     java -cp h2o-model.jar:. -Xmx2g -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=256m ").p(modelName).nl();
    sb.p("//").nl();
    sb.p("//     (Note:  Try java argument -XX:+PrintCompilation to show runtime JIT compiler behavior.)").nl();
    sb.nl();
    sb.p("public class ").p(modelName).p(" extends water.genmodel.GeneratedModel {").nl(); // or extends GenerateModel
    toJavaInit(sb, fileContextSB).nl();
    toJavaNAMES(sb);
    toJavaNCLASSES(sb);
    toJavaDOMAINS(sb);
    toJavaSuper(sb); //
    toJavaPredict(sb, fileContextSB);
    sb.p(TOJAVA_MAP);
    sb.p(TOJAVA_PREDICT_MAP);
    sb.p(TOJAVA_PREDICT_MAP_ALLOC1);
    sb.p(TOJAVA_PREDICT_MAP_ALLOC2);
    sb.p("}").nl();
    sb.p(fileContextSB).nl(); // Append file
    return sb;
  }
  // Same thing as toJava, but as a Javassist CtClass
  private CtClass makeCtClass() throws CannotCompileException {
    CtClass clz = ClassPool.getDefault().makeClass(JCodeGen.toJavaId(_key.toString()));
    clz.addField(CtField.make(toJavaNAMES   (new SB()).toString(),clz));
    clz.addField(CtField.make(toJavaNCLASSES(new SB()).toString(),clz));
    toJavaInit(clz);            // Model-specific top-level goodness
    clz.addMethod(CtMethod.make(toJavaPredict(new SB(), new SB()).toString(),clz)); // FIX ME
    clz.addMethod(CtMethod.make(TOJAVA_MAP,clz));
    clz.addMethod(CtMethod.make(TOJAVA_PREDICT_MAP,clz));
    clz.addMethod(CtMethod.make(TOJAVA_PREDICT_MAP_ALLOC1,clz));
    clz.addMethod(CtMethod.make(TOJAVA_PREDICT_MAP_ALLOC2,clz));
    return clz;
  }
  /** Generate implementation for super class. */
  protected SB toJavaSuper( SB sb ) {
    sb.nl();
    sb.ii(1);
    sb.i().p("public String[] getNames() { return NAMES; } ").nl();
    sb.i().p("public String[][] getDomainValues() { return DOMAINS; }").nl();
    sb.di(1);
    return sb;
  }
  private SB toJavaNAMES( SB sb ) {
    sb.nl();
    sb.i(1).p("// Names of columns used by model.").nl();
    return sb.i(1).p("public static final String[] NAMES = new String[] ").toJavaStringInit(_names).p(";").nl();
  }
  private SB toJavaNCLASSES( SB sb ) {
    sb.nl();
    sb.i(1).p("// Number of output classes included in training data response column,").nl();
    return sb.i(1).p("public static final int NCLASSES = ").p(nclasses()).p(";").nl();
  }
  private SB toJavaDOMAINS( SB sb ) {
    sb.nl();
    sb.i(1).p("// Column domains. The last array contains domain of response column.").nl();
    sb.i(1).p("public static final String[][] DOMAINS = new String[][] {").nl();
    for (int i=0; i<_domains.length; i++) {
      String[] dom = _domains[i];
      sb.i(2).p("/* ").p(_names[i]).p(" */ ");
      if (dom==null) sb.p("null");
      else {
        sb.p("new String[] {");
        for (int j=0; j<dom.length; j++) {
          if (j>0) sb.p(',');
          sb.p('"').pj(dom[j]).p('"');
        }
        sb.p("}");
      }
      if (i!=_domains.length-1) sb.p(',');
      sb.nl();
    }
    return sb.i(1).p("};").nl();
  }
  // Override in subclasses to provide some top-level model-specific goodness
  protected SB toJavaInit(SB sb, SB fileContextSB) { return sb; }
  protected void toJavaInit(CtClass ct) { }
  // Override in subclasses to provide some inside 'predict' call goodness
  // Method returns code which should be appended into generated top level class after
  // predit method.
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
    ccsb.p("  public final float[] predict( double[] data, float[] preds) { return predict( data, preds, "+toJavaDefaultMaxIters()+"); }").nl();
    ccsb.p("  public final float[] predict( double[] data, float[] preds, int maxIters ) {").nl();
    SB classCtxSb = new SB();
    toJavaPredictBody(ccsb.ii(2), classCtxSb, fileCtxSb); ccsb.di(1);
    ccsb.p("    return preds;").nl();
    ccsb.p("  }").nl();
    ccsb.p(classCtxSb);
    return ccsb;
  }

  protected String toJavaDefaultMaxIters() { return "-1"; }

  private static final String TOJAVA_MAP =
    "\n"+
    "  // Takes a HashMap mapping column names to doubles.  Looks up the column\n"+
    "  // names needed by the model, and places the doubles into the data array in\n"+
    "  // the order needed by the model.  Missing columns use NaN.\n"+
    "  double[] map( Map<String, Double> row, double data[] ) {\n"+
    "    for( int i=0; i<NAMES.length-1; i++ ) {\n"+
    "      Double d = (Double)row.get(NAMES[i]);\n"+
    "      data[i] = d==null ? Double.NaN : d;\n"+
    "    }\n"+
    "    return data;\n"+
    "  }\n";
  private static final String TOJAVA_PREDICT_MAP =
    "\n"+
    "  // Does the mapping lookup for every row, no allocation\n"+
    "  float[] predict( Map<String, Double> row, double data[], float preds[] ) {\n"+
    "    return predict(map(row,data),preds);\n"+
    "  }\n";
  private static final String TOJAVA_PREDICT_MAP_ALLOC1 =
    "\n"+
    "  // Allocates a double[] for every row\n"+
    "  float[] predict( Map<String, Double> row, float preds[] ) {\n"+
    "    return predict(map(row,new double[NAMES.length]),preds);\n"+
    "  }\n";
  private static final String TOJAVA_PREDICT_MAP_ALLOC2 =
    "\n"+
    "  // Allocates a double[] and a float[] for every row\n"+
    "  float[] predict( Map<String, Double> row ) {\n"+
    "    return predict(map(row,new double[NAMES.length]),new float[NCLASSES+1]);\n"+
    "  }\n";

  // Convenience method for testing: build Java, convert it to a class &
  // execute it: compare the results of the new class's (JIT'd) scoring with
  // the built-in (interpreted) scoring on this dataset.  Throws if there
  // is any error (typically an AssertionError).
  public void testJavaScoring( Frame fr ) {
    try {
      //System.out.println(toJava());
      Class clz = ClassPool.getDefault().toClass(makeCtClass());
      Object modelo = clz.newInstance();
    }
    catch( CannotCompileException cce ) { throw new Error(cce); }
    catch( InstantiationException cce ) { throw new Error(cce); }
    catch( IllegalAccessException cce ) { throw new Error(cce); }
  }

}
