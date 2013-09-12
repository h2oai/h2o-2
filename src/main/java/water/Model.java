package water;

import com.google.gson.JsonObject;
import java.util.Arrays;
import water.api.Constants;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.*;

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
  public final String _domain[][];

  /** Empty constructor for deserialization */
  //public Model() { _selfKey = null; _names=null; _domain=null; dataKey=null; }

  /** Full constructor from frame: Strips out the Vecs to just the names needed
   *  to match columns later for future datasets.  */
  public Model( Key selfKey, Key dataKey, Frame fr ) {
    this(selfKey,dataKey,fr.names(),fr.domains());
  }

  /** Full constructor */
  public Model( Key selfKey, Key dataKey, String names[], String domain[][] ) {
    if( domain == null ) domain=new String[names.length+1][];
    assert domain.length==names.length;
    assert names.length > 1;
    assert names[names.length-1] != null; // Have a valid response-column name?
    _selfKey = selfKey;
    _dataKey = dataKey;
    _names = names;
    _domain = domain;
  }

  /** Simple shallow copy constructor to a new Key */
  public Model( Key selfKey, Model m ) { this(selfKey,m._dataKey,m._names,m._domain); }

  /** Called when deleting this model, to cleanup any internal keys */
  public void delete() { UKV.remove(_selfKey); }

  public String responseName() { return _names[_names.length-1]; }

  /** Bulk score the frame 'fr', producing a single output vector */
  public Vec score( Frame fr, Key key ) {
    Vec v = fr.anyVec().makeZero();
    fr.add("predict",v);
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        double tmp[] = new double[_names.length];
        Chunk p = chks[chks.length-1];
        for( int i=0; i<p._len; i++ )
          p.set0(i,score0(chks,i,tmp));
      }
    }.doAll(fr);
    fr.remove(fr.numCols()-1);
    return v;
  }

  /** Single row scoring, on a compatible (but not adapted) Frame.  Fairly expensive to adapt.  */
  public final double score( Frame fr, int row ) {
    double tmp[] = new double[fr.numCols()];
    for( int i=0; i<tmp.length; i++ )
      tmp[i] = fr._vecs[i].at(row);
    return score(fr.names(),fr.domains(),tmp);
  }

  /** Single row scoring, on a compatible set of data.  Fairly expensive to adapt. */
  public final double score( String names[], String domains[][], double row[] ) {
    return score(adapt(names,domains),row);
  }

  /** Single row scoring, on a compatible set of data, given an adaption vector */
  public final double score( int map[][], double row[] ) {
    int[] resMap = map[map.length-2]; // Response mapping is the almost last array
    int[] colMap = map[map.length-1]; // Column   mapping is the final array
    assert colMap.length == _names.length;
    double tmp[] = new double[_names.length]; // The adapted data
    for( int i=0; i<_names.length; i++ ) {
      // Column mapping, or NaN for missing columns
      double d = colMap[i]==-1 ? Double.NaN : row[colMap[i]];
      if( map[i] != null ) {    // Enum mapping
        int e = (int)d;
        d = e >= 0 && e < map[i].length ? (double)map[i][e] : Double.NaN;
      }
      tmp[i] = d;
    }
    double d = score0(tmp);     // The results.
    int e = (int)d;             // For enum results
    // Convert the model's enum result back to the user's enum result
    return resMap == null ? d : ((e>=0 && e<resMap.length) ? resMap[e] : Double.NaN);
  }

  /** Build an adaption array.  The length is equal to the Model's vector
   *  length plus a response mapping plus a column mapping.  Each inner array
   *  is a domain map from user domains to model domains - or null for non-enum
   *  columns.  The extra final int[] is the column mapping itself.  */
  private int[][] adapt( String names[], String domains[][] ) {
    throw H2O.unimpl();
  }

  /** Build an adapted Frame from the given Frame.  Useful for efficient bulk scoring
   *  of a new dataset to an existing model.  Same adaption as above, but expressed
   *  as a Frame instead of as an int[][].  */
  public Frame adapt( Frame fr ) {
    throw H2O.unimpl();
  }

  /** Bulk scoring API for whole chunks.  Chunks are all compatible with the
   *  model, and expect the last Chunk is for the final score.  Default method
   *  just does row-at-a-time for the whole chunk. */
  protected void score0( Chunk chks[] ) {
    double tmp[] = new double[_names.length];
    int len = chks[0]._len;
    for( int i=0; i<len; i++ )
      chks[_names.length].set0(i,score0(chks,i,tmp));
  }

  /** Bulk scoring API for one row.  Chunks are all compatible with the model,
   *  and expect the last Chunk is for the final score.  Default method is to
   *  just load the data into the tmp array, then call subclass scoring
   *  logic. */
  protected double score0( Chunk chks[], int row_in_chunk, double[] tmp ) {
    assert chks.length>=_names.length; // Last chunk is for the response
    for( int i=0; i<_names.length; i++ )
      tmp[i] = chks[i].at0(row_in_chunk);
    return score0(tmp);
  }

  /** Subclasses implement the scoring logic.  The data is pre-loaded into a
   *  re-used temp array, in the order the model expects. */
  protected abstract double score0(double [] data);
  // Version where the user has just ponied-up an array of data to be scored.
  // Data must be in proper order.  Handy for JUnit tests.
  public double score(double [] data){ return score0(data);  }
}
