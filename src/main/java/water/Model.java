package water;

import hex.ConfusionMatrix;
import hex.VariableImportance;

import java.io.*;
import java.util.*;

import javassist.*;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.*;
import water.util.*;
import water.util.Log.Tag.Sys;

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
  public final Key _selfKey;

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
  public ConfusionMatrix cm() { return null; }
  /** Returns mse for validation set. */
  public double mse() { return Double.NaN; }
  /** Variable importance of individual variables measured by this model. */
  public VariableImportance varimp() { return null; }

  /** Bulk score the frame 'fr', producing a Frame result; the 1st Vec is the
   *  predicted class, the remaining Vecs are the probability distributions.
   *  For Regression (single-class) models, the 1st and only Vec is the
   *  prediction value.  Also passed in a flag describing how hard we try to
   *  adapt the frame.  */
  public Frame score( Frame fr) {
    int ridx = fr.find(_names[_names.length-1]);
    if(ridx != -1){ // drop the response for scoring!
      fr = new Frame(fr);
      fr.remove(ridx);
    }
    // Adapt the Frame layout - returns adapted frame and frame containing only
    // newly created vectors
    Frame[] adaptFrms = adapt(fr,false);
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
          float[] out = score0(chks,i,tmp,preds);
          if( nclasses() > 1 ) {
            if( Float.isNaN(out[0]) ) p.setNA0(i);
            else p.set0(i, Utils.maxIndex(out));
            for( int c=0; c<nclasses(); c++ )
              chks[_names.length+c].set0(i,out[c]);
          } else {
            p.set0(i,out[0]);
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
    return score(adapt(names,domains,exact),row,new float[nclasses()]);
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
  private int[][] adapt( String names[], String domains[][], boolean exact) {
    int maplen = names.length;
    int map[][] = new int[maplen][];
    // Make sure all are compatible
    for( int c=0; c<names.length;++c) {
            // Now do domain mapping
      String ms[] = _domains[c];  // Model enum
      String ds[] =  domains[c];  // Data  enum
      if( ms == ds ) { // Domains trivially equal?
      } else if( ms == null && ds != null ) {
        throw new IllegalArgumentException("Incompatible column: '" + _names[c] + "', expected (trained on) numeric, was passed a categorical");
      } else if( ms != null && ds == null ) {
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
    vfr = vfr.subframe(names); // select only supported columns, if column is missing Exception is thrown
    Vec[] frvecs = vfr.vecs();
    boolean[] toEnum = new boolean[frvecs.length];
    if(!exact) for(int i = 0; i < n;++i)
      if(_domains[i] != null && !frvecs[i].isEnum()) {// if model expects domain but input frame does not have domain => switch vector to enum
        frvecs[i] = frvecs[i].toEnum();
        toEnum[i] = true;
      }
    int map[][] = adapt(names,vfr.domains(),exact);
    assert map.length == names.length; // Be sure that adapt call above do not skip any column
    ArrayList<Vec> avecs = new ArrayList<Vec>(); // adapted vectors
    ArrayList<String> anames = new ArrayList<String>(); // names for adapted vector

    for( int c=0; c<map.length; c++ ) // Iterate over columns
      if(map[c] != null) { // Column needs adaptation
        Vec adaptedVec = null;
        if (toEnum[c]) { // Vector was flipped to column already, compose transformation
          adaptedVec = TransfVec.compose( (TransfVec) frvecs[c], map[c], false );
        } else adaptedVec = frvecs[c].makeTransf(map[c]);
        avecs.add(frvecs[c] = adaptedVec);
        anames.add(names[c]); // Collect right names
      }
    return new Frame[] { new Frame(names,frvecs), new Frame(anames.toArray(new String[anames.size()]), avecs.toArray(new Vec[avecs.size()])) };
  }

  /** Returns a mapping between values domains for a given column.  */
  public static int[] getDomainMapping(String colName, String[] modelDom, String[] dom, boolean exact) {
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
  public String toJava() { return toJava(new SB()).toString(); }
  public SB toJava( SB sb ) {
    SB fileContextSB = new SB(); // preserve file context
    String modelName = JCodeGen.toJavaId(_selfKey.toString());
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
    sb.p("//     curl http:/").p(H2O.SELF.toString()).p("/2/").p(this.getClass().getSimpleName()).p("View.java?_modelKey=").pobj(_selfKey).p(" > ").p(modelName).p(".java").nl();
    sb.p("//     javac -cp h2o-model.jar -J-Xmx2g -J-XX:MaxPermSize=128m ").p(modelName).p(".java").nl();
    sb.p("//     java -cp h2o-model.jar:. -Xmx2g -XX:MaxPermSize=256m ").p(modelName).nl();
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
    CtClass clz = ClassPool.getDefault().makeClass(JCodeGen.toJavaId(_selfKey.toString()));
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
      if (dom==null) sb.i(2).p("null");
      else {
        sb.i(2).p("new String[] {");
        for (int j=0; j<dom.length; j++) {
          if (j>0) sb.p(',');
          sb.p('"').p(dom[j]).p('"');
        }
        sb.p("}");
      }
      if (i!=_domains.length-1) sb.p(',');
      sb.nl();
    }
    return sb.i(1).p("};").nl();
  }
  // Override in subclasses to provide some top-level model-specific goodness
  protected SB toJavaInit(SB sb, SB fileContextSB) { return sb; };
  protected void toJavaInit(CtClass ct) { };
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

  protected String toJavaDefaultMaxIters() { return "-1"; };

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
