package water;

import hex.ConfusionMatrix;
import java.util.Arrays;
import java.util.HashMap;
import javassist.*;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.*;
import water.util.Log.Tag.Sys;
import water.util.Log;
import water.util.Utils;

/**
 * A Model models reality (hopefully).
 * A model can be used to 'score' a row, or a collection of rows on any
 * compatible dataset - meaning the row has all the columns with the same names
 * as used to build the mode.
 */
public abstract class Model extends Iced {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  /** Key associated with this Model, if any.  */
  @API(help="Key associated with Model")
  public Key _selfKey;

  /** Dataset key used to *build* the model, for models for which this makes
   *  sense, or null otherwise.  Not all models are built from a dataset (eg
   *  artificial models), or are built from a single dataset (various ensemble
   *  models), so this key has no *mathematical* significance in the model but
   *  is handy during common model-building and for the historical record.  */
  @API(help="Datakey used to *build* the model")
  public Key _dataKey;

  /** Columns used in the model and are used to match up with scoring data
   *  columns.  The last name is the response column name. */
  @API(help="Column names used to build the model")
  public String _names[];

  /** Categorical/factor/enum mappings, per column.  Null for non-enum cols.
   *  The last column holds the response col enums.  */
  @API(help="Column names used to build the model")
  public String _domains[][];

  public Model() {
  }

  /** Full constructor from frame: Strips out the Vecs to just the names needed
   *  to match columns later for future datasets.  */
  public Model( Key selfKey, Key dataKey, Frame fr ) {
    this(selfKey,dataKey,fr.names(),fr.domains());
  }

  /** Full constructor */
  public Model( Key selfKey, Key dataKey, String names[], String domains[][] ) {
    if( domains == null ) domains=new String[names.length+1][];
    assert domains.length==names.length;
    assert names.length > 1;
    assert names[names.length-1] != null; // Have a valid response-column name?
    _selfKey = selfKey;
    _dataKey = dataKey;
    _names   = names;
    _domains = domains;
  }

  /** Simple shallow copy constructor to a new Key */
  public Model( Key selfKey, Model m ) { this(selfKey,m._dataKey,m._names,m._domains); }

  /** Default Job to train model */
  public Job defaultTrainJob() {
    return null;
  }

  /** Called when deleting this model, to cleanup any internal keys */
  public void delete() { UKV.remove(_selfKey); }

  public String responseName() { return   _names[  _names.length-1]; }
  public String[] classNames() { return _domains[_domains.length-1]; }
  public boolean isClassifier() { return classNames() != null ; }
  public int nclasses() {
    String cns[] = classNames();
    return cns==null ? 1 : cns.length;
  }

  /** For classifiers, confusion matrix on validation set. */
  public ConfusionMatrix cm() {
    return null;
  }

  /** Bulk score the frame 'fr', producing a Frame result; the 1st Vec is the
   *  predicted class, the remaining Vecs are the probability distributions.
   *  For Regression (single-class) models, the 1st and only Vec is the
   *  prediction value.  Also passed in a flag describing how hard we try to
   *  adapt the frame.  */
  public Frame score( Frame fr, boolean exact ) {
    // Adapt the Frame layout - returns adapted frame and frame containing only
    // newly created vectors
    Frame[] adaptFrms = adapt(fr,exact,false);
    // Adapted frame containing all columns - mix of original vectors from fr
    // and newly created vectors serving as adaptors
    Frame adaptFrm = adaptFrms[0];
    // Contains only newly created vectors. The frame eases deletion of these vectors.
    Frame onlyAdaptFrm = adaptFrms[1];
    Vec v = adaptFrm.anyVec().makeZero();
    // If the model produces a classification/enum, copy the domain into the
    // result vector.
    v._domain = _domains[_domains.length-1];
    adaptFrm.add("predict",v);
    if( nclasses() > 1 )
      for( int c=0; c<nclasses(); c++ )
        adaptFrm.add(classNames()[c],adaptFrm.anyVec().makeZero());
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        double tmp[] = new double[_names.length];
        float preds[] = new float[nclasses()];
        Chunk p = chks[_names.length-1];
        for( int i=0; i<p._len; i++ ) {
          score0(chks,i,tmp,preds);
          if( nclasses() > 1 ) {
            p.set0(i,Utils.maxIndex(preds));
            for( int c=0; c<nclasses(); c++ )
              chks[_names.length+c].set0(i,preds[c]);
          } else {
            p.set0(i,preds[0]);
          }
        }
      }
    }.doAll(adaptFrm);
    // Return just the output columns
    int x=_names.length-1, y=adaptFrm.numCols();
    Frame output = adaptFrm.extractFrame(x, y);
    // Delete manually only vectors which i created :-/
    onlyAdaptFrm.remove();
    return output;
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
    return score(adapt(names,domains,exact,false),row,new float[nclasses()]);
  }

  /** Single row scoring, on a compatible set of data, given an adaption vector */
  public final float[] score( int map[][], double row[], float[] preds ) {
    int[] colMap = map[map.length-1]; // Column mapping is the final array
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
    return score0(tmp,preds);   // The results.
  }

  /** Build an adaption array.  The length is equal to the Model's vector
   *  length minus the response plus a column mapping.  Each inner array is a
   *  domain map from data domains to model domains - or null for non-enum
   *  columns, or null for identity mappings.  The extra final int[] is the
   *  column mapping itself, mapping from model columns to data columns. or -1
   *  if missing.
   *  If 'exact' is true, will throw if there are:
   *    any columns in the model but not in the input set;
   *    any enums in the data that the model does not understand
   *    any enums returned by the model that the data does not have a mapping for.
   *  If 'exact' is false, these situations will use or return NA's instead.
   */
  private int[][] adapt( String names[], String domains[][], boolean exact, boolean response ) {
    int length = response ? _names.length : _names.length-1;
    int map[][] = new int[length + 1][];

    // Build the column mapping: cmap[model_col] == user_col, or -1 if missing.
    int cmap[] = map[length] = new int[length];
    HashMap<String,Integer> m = new HashMap<String, Integer>();
    for( int d = 0; d <  names.length  ; ++d) m.put(names[d], d);
    for( int c = 0; c < length; ++c) {
      Integer I = m.get(_names[c]);
      cmap[c] = I==null ? -1 : I; // Check for data missing model column
    }

    // Make sure all are compatible
    for( int c=0; c<cmap.length; c++ ) {
      int d = cmap[c];          // Matching data column
      if( d == -1 ) {           // Column was missing from data
        if( exact ) throw new IllegalArgumentException("Model requires a column called "+_names[c]);
        continue;               // Cannot check domains of missing columns
      }

      // Now do domain mapping
      String ms[] = _domains[c];  // Model enum
      String ds[] =  domains[d];  // Data  enum
      if( ms == ds ) { // Domains trivially equal?
      } else if( ms == null && ds != null ) {
        throw new IllegalArgumentException("Incompatible column: '" + _names[c] + "', expected (trained on) numeric, was passed a categorical");
      } else if( ms != null && ds == null ) {
        if( exact )
          throw new IllegalArgumentException("Incompatible column: '" + _names[c] + "', expected (trained on) categorical, was passed a numeric");
        throw H2O.unimpl();     // Attempt an asEnum?
      } else if( !Arrays.deepEquals(ms, ds) ) {
        map[c] = getDomainMapping(_names[c], ms, ds, exact);
      } else {
        // null mapping is equal to identity mapping
      }
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
  public Frame[] adapt( Frame fr, boolean exact, boolean response ) {
    String frnames[] = fr.names();
    Vec frvecs[] = fr.vecs();
    int map[][] = adapt(frnames,fr.domains(),exact,response);
    int cmap[] =     map[map.length-1];
    Vec vecs[] = new Vec[map.length-1];
    int avCnt = 0;
    for( int c=0; c<cmap.length; c++ ) if (map[c] != null) avCnt++;
    Vec[]    avecs = new Vec[avCnt]; // list of adapted vectors
    String[] anames = new String[avCnt]; // names of adapted vectors
    avCnt = 0;
    for( int c=0; c<cmap.length; c++ ) { // iterate over columns
      int d = cmap[c];          // Data index
      if( d == -1 ) throw H2O.unimpl(); // Swap in a new all-NA Vec
      else if( map[c] == null ) {       // No or identity domain map?
        vecs[c] = frvecs[d];            // Just use the Vec as-is
      } else {
        // Domain mapping - creates a new vector
        vecs[c] = avecs[avCnt] = frvecs[d].makeTransf(map[c]);
        anames[avCnt] = frnames[d];
        avCnt++;
      }
    }
    return new Frame[] { new Frame(Arrays.copyOf(_names,map.length-1),vecs), new Frame(anames, avecs) };
  }

  /** Returns a mapping between values domains for a given column.  */
  private static int[] getDomainMapping(String colName, String[] modelDom, String[] dom, boolean exact) {
    int emap[] = new int[dom.length];
    HashMap<String,Integer> md = new HashMap<String, Integer>();
    for( int i = 0; i < modelDom.length; i++) md.put(modelDom[i], i);
    for( int i = 0; i < dom.length; i++) {
      Integer I = md.get(dom[i]);
      if( I==null && exact )
        Log.warn(Sys.SCORM, "Column "+colName+" was not trained with factor '"+dom[i]+"' which appears in the data");
      emap[i] = I==null ? -1 : I;
    }
    for( int i = 0; i < dom.length; i++)
      assert emap[i]==-1 || modelDom[emap[i]].equals(dom[i]);
    return emap;
  }

  /** Bulk scoring API for one row.  Chunks are all compatible with the model,
   *  and expect the last Chunks are for the final distribution & prediction.
   *  Default method is to just load the data into the tmp array, then call
   *  subclass scoring logic. */
  protected float[] score0( Chunk chks[], int row_in_chunk, double[] tmp, float[] preds ) {
    assert chks.length>=_names.length; // Last chunk is for the response
    for( int i=0; i<_names.length; i++ )
      tmp[i] = chks[i].at0(row_in_chunk);
    return score0(tmp,preds);
  }

  /** Subclasses implement the scoring logic.  The data is pre-loaded into a
   *  re-used temp array, in the order the model expects.  The predictions are
   *  loaded into the re-used temp array, which is also returned.  */
  protected abstract float[] score0(double data[/*ncols*/], float preds[/*nclasses*/]);
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
   *      double[] map( HashMap<String,Double> row, double data[] );
   *      // Does the mapping lookup for every row, no allocation
   *      float[] predict( HashMap<String,Double> row, double data[], float preds[] );
   *      // Allocates a double[] for every row
   *      float[] predict( HashMap<String,Double> row, float preds[] );
   *      // Allocates a double[] and a float[] for every row
   *      float[] predict( HashMap<String,Double> row );
   *    }
   *  </pre>
   */
  public String toJava() {
    SB sb = new SB();
    sb.p("\n");
    sb.p("class ").p(_selfKey.toString()).p(" {\n");
    toJavaNAMES(sb);
    toJavaNCLASSES(sb);
    toJavaInit(sb);  sb.p("\n");
    toJavaPredict(sb);
    sb.p(TOJAVA_MAP);
    sb.p(TOJAVA_PREDICT_MAP);
    sb.p(TOJAVA_PREDICT_MAP_ALLOC1);
    sb.p(TOJAVA_PREDICT_MAP_ALLOC2);
    sb.p("}\n");
    return sb.toString();
  }
  // Same thing as toJava, but as a Javassist CtClass
  private CtClass makeCtClass() throws CannotCompileException {
    CtClass clz = ClassPool.getDefault().makeClass(_selfKey.toString());
    clz.addField(CtField.make(toJavaNAMES   (new SB()).toString(),clz));
    clz.addField(CtField.make(toJavaNCLASSES(new SB()).toString(),clz));
    toJavaInit(clz);            // Model-specific top-level goodness
    clz.addMethod(CtMethod.make(toJavaPredict(new SB()).toString(),clz));
    clz.addMethod(CtMethod.make(TOJAVA_MAP,clz));
    clz.addMethod(CtMethod.make(TOJAVA_PREDICT_MAP,clz));
    clz.addMethod(CtMethod.make(TOJAVA_PREDICT_MAP_ALLOC1,clz));
    clz.addMethod(CtMethod.make(TOJAVA_PREDICT_MAP_ALLOC2,clz));
    return clz;
  }


  private SB toJavaNAMES( SB sb ) {
    return sb.p("  public static final String []NAMES = new String[] ").p(_names).p(";\n");
  }
  private SB toJavaNCLASSES( SB sb ) {
    return sb.p("  public static final int NCLASSES = ").p(nclasses()).p(";\n");
  }
  // Override in subclasses to provide some top-level model-specific goodness
  protected void toJavaInit(SB sb) { };
  protected void toJavaInit(CtClass ct) { };
  // Override in subclasses to provide some inside 'predict' call goodness
  protected void toJavaPredictBody(SB sb) {
    throw new IllegalArgumentException("This model type does not support conversion to Java");
  }
  // Wrapper around the main predict call, including the signature and return value
  private SB toJavaPredict(SB sb) {
    sb.p("  // Pass in data in a double[], pre-aligned to the Model's requirements.\n");
    sb.p("  // Jam predictions into the preds[] array; preds[0] is reserved for the\n");
    sb.p("  // main prediction (class for classifiers or value for regression),\n");
    sb.p("  // and remaining columns hold a probability distribution for classifiers.\n");
    sb.p("  float[] predict( double data[], float preds[] ) {\n");
    toJavaPredictBody(sb);
    sb.p("    return preds;\n");
    sb.p("  }\n");
    return sb;
  }

  private static final String TOJAVA_MAP =
    "  // Takes a HashMap mapping column names to doubles.  Looks up the column\n"+
    "  // names needed by the model, and places the doubles into the data array in\n"+
    "  // the order needed by the model.  Missing columns use NaN.\n"+
    "  double[] map( java.util.HashMap row, double data[] ) {\n"+
    "    for( int i=0; i<NAMES.length-1; i++ ) {\n"+
    "      Double d = (Double)row.get(NAMES[i]);\n"+
    "      data[i] = d==null ? Double.NaN : d;\n"+
    "    }\n"+
    "    return data;\n"+
    "  }\n";
  private static final String TOJAVA_PREDICT_MAP =
    "  // Does the mapping lookup for every row, no allocation\n"+
    "  float[] predict( java.util.HashMap row, double data[], float preds[] ) {\n"+
    "    return predict(map(row,data),preds);\n"+
    "  }\n";
  private static final String TOJAVA_PREDICT_MAP_ALLOC1 =
    "  // Allocates a double[] for every row\n"+
    "  float[] predict( java.util.HashMap row, float preds[] ) {\n"+
    "    return predict(map(row,new double[NAMES.length]),preds);\n"+
    "  }\n";
  private static final String TOJAVA_PREDICT_MAP_ALLOC2 =
    "  // Allocates a double[] and a float[] for every row\n"+
    "  float[] predict( java.util.HashMap row ) {\n"+
    "    return predict(map(row,new double[NAMES.length]),new float[NCLASSES+1]);\n"+
    "  }\n";

  // Can't believe this wasn't done long long ago
  protected static class SB {
    public final StringBuilder _sb = new StringBuilder();
    public SB p( String s ) { _sb.append(s); return this; }
    public SB p( float  s ) { _sb.append(s); return this; }
    public SB p( char   s ) { _sb.append(s); return this; }
    public SB p( int    s ) { _sb.append(s); return this; }
    public SB indent( int d ) { for( int i=0; i<d; i++ ) p("  "); return this; }
    // Convert a String[] into a valid Java String initializer
    SB p( String[] ss ) {
      p('{');
      for( int i=0; i<ss.length-1; i++ )  p('"').p(ss[i]).p("\",");
      if( ss.length > 0 ) p('"').p(ss[ss.length-1]).p('"');
      return p('}');
    }
    @Override public String toString() { return _sb.toString(); }
  }

  // Convenience method for testing: build Java, convert it to a class &
  // execute it: compare the results of the new class's (JIT'd) scoring with
  // the built-in (interpreted) scoring on this dataset.  Throws if there
  // is any error (typically an AssertionError).
  public void testJavaScoring( Frame fr ) {
    try {
      System.out.println(toJava());
      Class clz = ClassPool.getDefault().toClass(makeCtClass());
      Object modelo = clz.newInstance();
    }
    catch( CannotCompileException cce ) { throw new Error(cce); }
    catch( InstantiationException cce ) { throw new Error(cce); }
    catch( IllegalAccessException cce ) { throw new Error(cce); }
  }
}
