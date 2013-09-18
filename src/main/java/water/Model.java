package water;

import java.util.Arrays;

import water.api.DocGen;
import water.api.Request.API;
import water.fvec.*;
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
    this(selfKey,dataKey,fr.names(),domains(fr));
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

  private static String[][] domains(Frame fr) {
    String[][] domains = fr.domains();
    if(domains[domains.length-1] == null)
      domains[domains.length-1] = responseDomain(fr);
    return domains;
  }
  /** If response column is not an enum, use numbers */
  public static String[] responseDomain(Frame fr) {
    Vec resp = fr._vecs[fr._vecs.length-1];
    String[] domain = resp._domain;
    if(resp._domain == null) {
      int min = (int) resp.min();
      int max = (int) resp.max();
      domain = new String[max - min + 1];
      for( int i = 0; i < domain.length; i++ )
        domain[i] = "" + (min + i);
    }
    return domain;
  }

  /** Simple shallow copy constructor to a new Key */
  public Model( Key selfKey, Model m ) { this(selfKey,m._dataKey,m._names,m._domains); }

  /** Called when deleting this model, to cleanup any internal keys */
  public void delete() { UKV.remove(_selfKey); }

  public String responseName() { return   _names[  _names.length-1]; }
  public String[] classNames() { return _domains[_domains.length-1]; }
  public int nclasses() {
    String cns[] = classNames();
    return cns==null ? 1 : cns.length;
  }

  /** Bulk score the frame 'fr', producing a single output vector.  Also passed
   *  in a flag describing how hard we try to adapt the frame.  */
  public Vec score( Frame fr, boolean exact ) {
    Frame fr2 = adapt(fr,exact); // Adapt the Frame layout
    Vec v = fr2.anyVec().makeZero();
    // If the model produces a classification/enum, copy the domain into the
    // result vector.
    v._domain = _domains[_domains.length-1];
    fr2.add("predict",v);
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        double tmp[] = new double[_names.length];
        float preds[] = new float[nclasses()];
        Chunk p = chks[chks.length-1];
        for( int i=0; i<p._len; i++ )
          p.set0(i,Utils.maxIndex(score0(chks,i,tmp,preds)));
      }
    }.doAll(fr2);
    return v;
  }

  /** Single row scoring, on a compatible (but not adapted) Frame.  Fairly expensive to adapt.  */
  public final float[] score( Frame fr, boolean exact, int row ) {
    double tmp[] = new double[fr.numCols()];
    for( int i=0; i<tmp.length; i++ )
      tmp[i] = fr._vecs[i].at(row);
    return score(fr.names(),fr.domains(),exact,tmp);
  }

  /** Single row scoring, on a compatible set of data.  Fairly expensive to adapt. */
  public final float[] score( String names[], String domains[][], boolean exact, double row[] ) {
    return score(adapt(names,domains,exact),row,new float[nclasses()]);
  }

  /** Single row scoring, on a compatible set of data, given an adaption vector */
  public final float[] score( int map[][], double row[], float[] preds ) {
    int[] colMap = map[map.length-1]; // Column mapping is the final array
    assert colMap.length == _names.length;
    double tmp[] = new double[_names.length]; // The adapted data
    for( int i=0; i<_names.length; i++ ) {
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
   *  length minus the response plus a column mapping.  Each inner array
   *  is a domain map from user domains to model domains - or null for non-enum
   *  columns.  The extra final int[] is the column mapping itself.
   *  If 'exact' is true, will throw if there are:
   *    any columns in the model but not in the input set;
   *    any enums in the data that the model does not understand
   *    any enums returned by the model that the data does not understand.
   *  If 'exact' is false, these situations will use or return NA's instead.
   */
  private int[][] adapt( String names[], String domains[][], boolean exact ) {
    // Make sure all are compatible
    for( int i=0; i<_names.length-1; i++ ) {
      if( !_names[i].equals(names[i]) ) throw H2O.unimpl();
      if( _domains[i] != domains[i] ) {
        if( _domains[i] == null || domains[i] == null ) {
          if( exact )
            throw new IllegalArgumentException("Model expects "+Arrays.toString(_domains[i])+" but was passed "+Arrays.toString(domains[i]));
          throw H2O.unimpl();
        }
        for( int j=0; j<_names.length; j++ ) {
          if( !(_domains[i][j].equals(domains[i][j])) )
            throw H2O.unimpl();
        }
      }
    }
    // Trivial non-mapping map
    return new int[names.length][];
  }

  /** Build an adapted Frame from the given Frame.  Useful for efficient bulk
   *  scoring of a new dataset to an existing model.  Same adaption as above,
   *  but expressed as a Frame instead of as an int[][].     */
  public Frame adapt( Frame fr, boolean exact ) {
    int[][] map = adapt(fr.names(),fr.domains(),exact);
    for( int i=0; i<map.length; i++ )
      if( map[i] != null ) throw H2O.unimpl();
    return new Frame(fr);
  }

  /** Bulk scoring API for whole chunks.  Chunks are all compatible with the
   *  model, and expect the last Chunk is for the final score.  Default method
   *  just does row-at-a-time for the whole chunk. */
  protected void score0( Chunk chks[] ) {
    double tmp[] = new double[_names.length];
    float preds[] = new float[nclasses()];
    int len = chks[0]._len;
    for( int i=0; i<len; i++ )
      chks[_names.length].set0(i,Utils.maxIndex(score0(chks,i,tmp,preds)));
  }

  /** Bulk scoring API for one row.  Chunks are all compatible with the model,
   *  and expect the last Chunk is for the final score.  Default method is to
   *  just load the data into the tmp array, then call subclass scoring
   *  logic. */
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
}
