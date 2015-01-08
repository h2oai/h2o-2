package water.fvec;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.exec.Flow;
import water.util.Log;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IllegalFormatException;
import java.util.Random;

/**
 * A collection of named Vecs.  Essentially an R-like data-frame.  Multiple
 * Frames can reference the same Vecs.  A Frame is a lightweight object, it is
 * meant to be cheaply created and discarded for data munging purposes.
 * E.g. to exclude a Vec from a computation on a Frame, create a new Frame that
 * references all the Vecs but this one.
 */
public class Frame extends Lockable<Frame> {
  public String[] _names;
  Key[] _keys;          // Keys for the vectors
  private transient Vec[] _vecs;// The Vectors (transient to avoid network traffic)
  private transient Vec _col0;  // First readable vec; fast access to the VectorGroup's Chunk layout
  private final UniqueId uniqueId;

  public Frame(Key k){
    super(k);
    uniqueId = new UniqueFrameId(k, this);
  }
  public Frame( Frame fr ) { this(fr._key,fr._names.clone(), fr.vecs().clone()); _col0 = null; }
  public Frame( Vec... vecs ){ this(null,vecs);}
  public Frame( String[] names, Vec[] vecs ) { this(null,names,vecs); }


  public Frame( Key key, String[] names, Vec[] vecs ) {
    super(key);
    this.uniqueId = new UniqueFrameId(_key, this);
    if( names==null ) {
      names = new String[vecs.length];
      for( int i=0; i<vecs.length; i++ ) names[i] = "C"+(i+1);
    }
    assert names.length == vecs.length : "Number of columns does not match to number of cols' names.";
    _names=names;
    _vecs=vecs;
    _keys = new Key[vecs.length];
    for( int i=0; i<vecs.length; i++ )
      _keys[i] = vecs[i]._key;
    assert checkCompatible();
  }

  /**
   * Task to compare the two frames, returns true if they are identical.
   * We can't in general expect frames to be bit-compatible so we compare the numbers,
   * integers are compared exaclty, doubles only with given precision (1e-8 is default).
   * (compression scheme may be altered by the way they were parsed and by rebalancing)
   * The frames are expected to be compatible.
   * @param f
   * @return
   */
  public final boolean isIdentical(Frame f){
    FrameIdenticalTask fbt = new FrameIdenticalTask(this,f);
    H2O.submitTask(fbt);
    fbt.join();
    return fbt._res;
  }
  public static class FrameIdenticalTask extends H2OCountedCompleter {
    final Frame _f1;
    final Frame _f2;
    public FrameIdenticalTask(Frame f1, Frame f2){_f1 = f1; _f2 = f2;}
    boolean _res;
    double _fpointPrecision = 1e-8;
    private Vec.VecIdenticalTask[] _vts;
    @Override
    public void compute2() {
      if(_f1 == _f2){
        _res = true;
      } else if(Arrays.deepEquals(_f1.names(), _f2.names())){
        _vts = new Vec.VecIdenticalTask[_f1.numCols()];
        addToPendingCount(_vts.length);
        for(int i = 0; i < _vts.length; ++i) {
          _vts[i] = new Vec.VecIdenticalTask(this,_fpointPrecision);
          _vts[i].asyncExec(_f1.vec(i),_f2.vec(i));
        }
      }
      tryComplete();
    }

    @Override public void onCompletion(CountedCompleter cc){
      if(_vts != null){
        _res = _vts[0]._res;
        for(int i = 1; i < _vts.length; ++i)
          _res = _res && _vts[i]._res;
      }
    }
  }
  public UniqueId getUniqueId() {
    return this.uniqueId;
  }

  /** 64-bit checksum of the checksums of the vecs.  SHA-265 checksums of the chunks are XORed
   * together.  Since parse always parses the same pieces of files into the same offsets
   * in some chunk this checksum will be consistent across reparses.
   */
  public long checksum() {
    Vec [] vecs = vecs();
    long _checksum = 0;
    for(int i = 0; i < _names.length; ++i) {
      long vec_checksum = vecs[i].checksum();
      _checksum ^= vec_checksum;
      _checksum ^= (2147483647 * i);
    }
    return _checksum;
  }

  public Vec vec(String name){
    Vec [] vecs = vecs();
    for(int i = 0; i < _names.length; ++i)
      if(_names[i].equals(name))return vecs[i];
    return null;
  }
  /** Returns the vector by given index.
   * <p>The call is direct equivalent to call <code>vecs()[i]</code> and
   * it does not do any array bounds checking.</p>
   * @param idx idx of column
   * @return this frame idx-th vector, never returns <code>null</code>
   */
  public Vec vec(int idx) {
    Vec[] vecs = vecs();
    return vecs[idx];
  }
  /** Returns a subframe of this frame containing only vectors with desired names.
   *
   * @param names list of vector names
   * @return a new frame which collects vectors from this frame with desired names.
   * @throws IllegalArgumentException if there is no vector with desired name in this frame.
   */
  public Frame subframe(String[] names) { return subframe(names, false, 0)[0]; }
  /** Returns a new frame composed of vectors of this frame selected by given names.
   * The method replaces missing vectors by a constant column filled by given value.
   * @param names names of vector to compose a subframe
   * @param c value to fill missing columns.
   * @return two frames, the first contains subframe, the second contains newly created constant vectors or null
   */
  public Frame[] subframe(String[] names, double c) { return subframe(names, true, c); }
  /** Create a subframe from this frame based on desired names.
   * Throws an exception if desired column is not in this frame and <code>replaceBy</code> is <code>false</code>.
   * Else replace a missing column by a constant column with given value.
   *
   * @param names list of column names to extract
   * @param replaceBy should be missing column replaced by a constant column
   * @param c value for constant column
   * @return array of 2 frames, the first is containing a desired subframe, the second one contains newly created columns or null
   * @throws IllegalArgumentException if <code>replaceBy</code> is false and there is a missing column in this frame
   */
  private Frame[] subframe(String[] names, boolean replaceBy, double c){
    Vec [] vecs     = new Vec[names.length];
    Vec [] cvecs    = replaceBy ? new Vec   [names.length] : null;
    String[] cnames = replaceBy ? new String[names.length] : null;
    int ccv = 0; // counter of constant columns
    vecs();                     // Preload the vecs
    HashMap<String, Integer> map = new HashMap<String, Integer>((int) ((names.length/0.75f)+1)); // avoid rehashing by set up initial capacity
    for(int i = 0; i < _names.length; ++i) map.put(_names[i], i);
    for(int i = 0; i < names.length; ++i)
      if(map.containsKey(names[i])) vecs[i] = _vecs[map.get(names[i])];
      else if (replaceBy) {
        Log.warn("Column " + names[i] + " is missing, filling it in with " + c);
        cnames[ccv] = names[i];
        vecs[i] = cvecs[ccv++] = anyVec().makeCon(c);
      }
    return new Frame[] { new Frame(names,vecs), ccv>0 ?  new Frame(Arrays.copyOf(cnames, ccv), Arrays.copyOf(cvecs,ccv)) : null };
  }

  public final Vec[] vecs(int [] idxs) {
    Vec [] all = vecs();
    Vec [] res = new Vec[idxs.length];
    for(int i = 0; i < idxs.length; ++i)
      res[i] = all[idxs[i]];
    return res;
  }
  // Return (and cache) vectors
  public final Vec[] vecs() {
    Vec[] tvecs = _vecs; // read the content
    return tvecs == null ? (_vecs=vecs_impl()) : tvecs;
  }
  // Compute vectors for caching
  private Vec[] vecs_impl() {
    // Load all Vec headers; load them all in parallel by spawning F/J tasks.
    final Vec [] vecs = new Vec[_keys.length];
    Futures fs = new Futures();
    for( int i=0; i<_keys.length; i++ ) {
      final int ii = i;
      final Key k = _keys[i];
      H2OCountedCompleter t = new H2OCountedCompleter() {
          // We need higher priority here as there is a danger of deadlock in
          // case of many calls from MRTask2 at once (e.g. frame with many
          // vectors invokes rollup tasks for all vectors in parallel).  Should
          // probably be done in CPS style in the future
          @Override public byte priority(){return H2O.MIN_HI_PRIORITY;}
          @Override public void compute2() {
            Value v = DKV.get(k);
            if( v==null ) Log.err("Missing vector #" + ii + " (" + _names[ii] + ") during Frame fetch: "+k);
            vecs[ii] = v.get();
            tryComplete();
          }
        };
      H2O.submitTask(t);
      fs.add(t);
    }
    fs.blockForPending();
    return vecs;
  }
  // Force a cache-flush & reload, assuming vec mappings were altered remotely
  public final Vec[] reloadVecs() { _vecs=null; return vecs(); }

  /** Finds the first column with a matching name.  */
  public int find( String name ) {
    if (_names!=null)
      for( int i=0; i<_names.length; i++ )
        if( name.equals(_names[i]) )
          return i;
    return -1;
  }

  public int find( Vec vec ) {
    Vec[] vecs = vecs();
    for( int i=0; i<vecs.length; i++ )
      if( vec.equals(vecs[i]) )
        return i;
    return -1;
  }


  // Return Frame 'f' if 'f' is compatible with 'this'.
  // Return a new Frame compatible with 'this' and a copy of 'f's data otherwise.
  public Frame makeCompatible( Frame f) {
    // Small data frames are always "compatible"
    if( anyVec()==null)      // Or it is small
      return f;                 // Then must be compatible
    // Same VectorGroup is also compatible
    if( f.anyVec() == null ||
        f.anyVec().group().equals(anyVec().group()) && Arrays.equals(f.anyVec()._espc,anyVec()._espc))
      return f;
    // Ok, here make some new Vecs with compatible layout
    Key k = Key.make();
    H2O.submitTask(new RebalanceDataSet(this, f, k)).join();
    Frame f2 = DKV.get(k).get();
    DKV.remove(k);
    return f2;
  }

 /** Appends a named column, keeping the last Vec as the response */
  public Frame add( String name, Vec vec ) {
    if( find(name) != -1 ) throw new IllegalArgumentException("Duplicate name '"+name+"' in Frame");
    if( _vecs.length != 0 ) {
      if( !anyVec().group().equals(vec.group()) && !Arrays.equals(anyVec()._espc,vec._espc) )
        throw new IllegalArgumentException("Vector groups differs - adding vec '"+name+"' into the frame " + Arrays.toString(_names));
      if( numRows() != vec.length() )
        throw new IllegalArgumentException("Vector lengths differ - adding vec '"+name+"' into the frame " + Arrays.toString(_names));
    }
    final int len = _names != null ? _names.length : 0;
    _names = _names != null ? Arrays.copyOf(_names,len+1) : new String[len+1];
    _vecs  = _names != null ? Arrays.copyOf(_vecs ,len+1) : new Vec   [len+1];
    _keys  = _names != null ? Arrays.copyOf(_keys ,len+1) : new Key   [len+1];
    _names[len] = name;
    _vecs [len] = vec ;
    _keys [len] = vec._key;
    return this;
  }

  /** Insert a named column as the first column */
  public Frame prepend( String name, Vec vec ) {
    if( find(name) != -1 ) throw new IllegalArgumentException("Duplicate name '"+name+"' in Frame");
    if( _vecs.length != 0 ) {
      if( !anyVec().group().equals(vec.group()) && !Arrays.equals(anyVec()._espc,vec._espc) )
        throw new IllegalArgumentException("Vector groups differs - adding vec '"+name+"' into the frame " + Arrays.toString(_names));
      if( numRows() != vec.length() )
        throw new IllegalArgumentException("Vector lengths differ - adding vec '"+name+"' into the frame " + Arrays.toString(_names));
    }
    final int len = _names != null ? _names.length : 0;
    String[] _names2 = new String[len+1];
    Vec[]    _vecs2  = new Vec   [len+1];
    Key[]    _keys2  = new Key   [len+1];
    _names2[0] = name;
    _vecs2 [0] = vec ;
    _keys2 [0] = vec._key;
    System.arraycopy(_names, 0, _names2, 1, len);
    System.arraycopy(_vecs,  0, _vecs2,  1, len);
    System.arraycopy(_keys,  0, _keys2,  1, len);
    _names = _names2;
    _vecs  = _vecs2;
    _keys  = _keys2;
    return this;
  }

  /** Appends an entire Frame */
  public Frame add( Frame fr, String names[] ) {
    assert _vecs.length==0 || (anyVec().group().equals(fr.anyVec().group()) || Arrays.equals(anyVec()._espc,fr.anyVec()._espc)): "Adding a vector from different vector group. Current frame contains "+Arrays.toString(_names)+ " vectors. New frame contains "+Arrays.toString(fr.names()) + " vectors.";
    if( _names != null && fr._names != null )
      for( String name : names )
        if( find(name) != -1 ) throw new IllegalArgumentException("Duplicate name '"+name+"' in Frame");
    final int len0= _names!=null ? _names.length : 0;
    final int len1=  names!=null ?  names.length : 0;
    final int len = len0+len1;
    // Note: _names==null <=> _vecs==null <=> _keys==null
    _names = _names != null ? Arrays.copyOf(_names,len) : new String[len];
    _vecs  = _vecs  != null ? Arrays.copyOf(_vecs ,len) : new Vec   [len];
    _keys  = _keys  != null ? Arrays.copyOf(_keys ,len) : new Key   [len];
    System.arraycopy(    names,0,_names,len0,len1);
    System.arraycopy(fr._vecs ,0,_vecs ,len0,len1);
    System.arraycopy(fr._keys ,0,_keys ,len0,len1);
    return this;
  }
  public Frame add( Frame fr, boolean rename ) {
    if( !rename ) return add(fr,fr._names);
    String names[] = new String[fr._names.length];
    for( int i=0; i<names.length; i++ ) {
      String name = fr._names[i];
      int cnt=0;
      while( find(name) != -1 )
        name = fr._names[i]+"_"+(cnt++);
      names[i] = name;
    }
    return add(fr,names);
  }

  /** Removes the first column with a matching name.  */
  public Vec remove( String name ) { return remove(find(name)); }

  /** Removes a numbered column. */
  public Vec [] remove( int [] idxs ) {
    for(int i :idxs)if(i < 0 || i > _vecs.length)
      throw new ArrayIndexOutOfBoundsException();
    Arrays.sort(idxs);
    Vec [] res = new Vec[idxs.length];
    Vec [] rem = new Vec[_vecs.length-idxs.length];
    String [] names = new String[rem.length];
    Key    [] keys  = new Key   [rem.length];
    int j = 0;
    int k = 0;
    int l = 0;
    for(int i = 0; i < _vecs.length; ++i) {
      if(j < idxs.length && i == idxs[j]) {
        ++j;
        res[k++] = _vecs[i];
      } else {
        rem  [l] = _vecs [i];
        names[l] = _names[i];
        keys [l] = _keys [i];
        ++l;
      }
    }
    _vecs = rem;
    _names = names;
    _keys = keys;
    assert l == rem.length && k == idxs.length;
    return res;
  }
  /** Removes a numbered column. */
  public Vec remove( int idx ) {
    int len = _names.length;
    if( idx < 0 || idx >= len ) return null;
    Vec v = vecs()[idx];
    System.arraycopy(_names,idx+1,_names,idx,len-idx-1);
    System.arraycopy(_vecs ,idx+1,_vecs ,idx,len-idx-1);
    System.arraycopy(_keys ,idx+1,_keys ,idx,len-idx-1);
    _names = Arrays.copyOf(_names,len-1);
    _vecs  = Arrays.copyOf(_vecs ,len-1);
    _keys  = Arrays.copyOf(_keys ,len-1);
    if( v == _col0 ) _col0 = null;
    return v;
  }

  /**
   * Remove given interval of columns from frame. Motivated by R intervals.
   * @param startIdx - start index of column (inclusive)
   * @param endIdx - end index of column (exclusive)
   * @return an array of remove columns
   */
  public Vec[] remove(int startIdx, int endIdx) {
    int len = _names.length;
    int nlen = len - (endIdx-startIdx);
    String[] names = new String[nlen];
    Key[] keys = new Key[nlen];
    Vec[] vecs = new Vec[nlen];
    reloadVecs(); // force vecs reload
    if (startIdx > 0) {
      System.arraycopy(_names, 0, names, 0, startIdx);
      System.arraycopy(_vecs,  0, vecs,  0, startIdx);
      System.arraycopy(_keys,  0, keys,  0, startIdx);
    }
    nlen -= startIdx;
    if (endIdx < _names.length+1) {
      System.arraycopy(_names, endIdx, names, startIdx, nlen);
      System.arraycopy(_vecs,  endIdx, vecs,  startIdx, nlen);
      System.arraycopy(_keys,  endIdx, keys,  startIdx, nlen);
    }

    Vec[] vec = Arrays.copyOfRange(vecs(),startIdx,endIdx);
    _names = names;
    _vecs = vecs;
    _keys = keys;
    _col0 = null;
    return vec;
  }

  public Vec replace(int col, Vec nv) {
    if (col >= numCols())
      throw new IllegalArgumentException("Trying to select column "+(col+1)+" but only "+numCols()+" present.");
    Vec rv = vecs()[col];
    assert rv.group().equals(nv.group());
    _vecs[col] = nv;
    _keys[col] = nv._key;
    if( DKV.get(nv._key)==null )    // If not already in KV, put it there
      DKV.put(nv._key, nv);
    return rv;
  }

  public Vec factor(int col) {
    Vec nv = vecs()[col].toEnum();
    return replace(col, nv);
  }

  public Frame extractFrame(int startIdx, int endIdx) {
    Frame f = subframe(startIdx, endIdx);
    remove(startIdx, endIdx);
    return f;
  }

  /** Create a subframe from given interval of columns.
   *
   * @param startIdx index of first column (inclusive)
   * @param endIdx index of the last column (exclusive)
   * @return a new frame containing specified interval of columns
   */
  public Frame subframe(int startIdx, int endIdx) {
    Frame result = new Frame(Arrays.copyOfRange(_names,startIdx,endIdx),Arrays.copyOfRange(vecs(),startIdx,endIdx));
    return result;
  }

  public final String[] names() { return _names; }
  public int  numCols() { return vecs().length; }
  public long numRows() { return anyVec()==null ? 0 : anyVec().length(); }

  public boolean isRawData() {
    // Right now there is only one Vec for raw data, but imagine a Parse after a JDBC import or such.
    for (Vec v : vecs()) {
      if (v.isByteVec())
        return true;
    }
    return false;
  }

  // Number of columns when categoricals expanded.
  // Note: One level is dropped in each categorical col.
  public int numExpCols() {
    int ncols = 0;
    for(int i = 0; i < vecs().length; i++)
      ncols += vecs()[i].domain() == null ? 1 : (vecs()[i].domain().length - 1);
    return ncols;
  }

  /** All the domains for enum columns; null for non-enum columns.  */
  public String[][] domains() {
    String ds[][] = new String[vecs().length][];
    for( int i=0; i<vecs().length; i++ )
      ds[i] = vecs()[i].domain();
    return ds;
  }

  /** true/false every Vec is a UUID */
  public boolean[] uuids() {
    boolean bs[] = new boolean[vecs().length];
    for( int i=0; i<vecs().length; i++ )
      bs[i] = vecs()[i].isUUID();
    return bs;
  }

  /** Time status for every Vec */
  public byte[] times() {
    byte bs[] = new byte[vecs().length];
    for( int i=0; i<vecs().length; i++ )
      bs[i] = vecs()[i]._time;
    return bs;
  }

  private String[][] domains(int [] cols){
    Vec [] vecs = vecs();
    String [][] res = new String[cols.length][];
    for(int i = 0; i < cols.length; ++i)
      res[i] = vecs[cols[i]]._domain;
    return res;
  }

  private String [] names(int [] cols){
    if(_names == null)return null;
    String [] res = new String[cols.length];
    for(int i = 0; i < cols.length; ++i)
      res[i] = _names[cols[i]];
    return res;
  }

  public Vec lastVec() {
    final Vec [] vecs = vecs();
    return vecs[vecs.length-1];
  }
  /** Returns the first readable vector. */
  public Vec anyVec() {
    Vec c0 = _col0; // single read
    if( c0 != null ) return c0;
    for( Vec v : vecs() )
      if( v.readable() )
        return (_col0 = v);
    return null;
  }
  /* Returns the only Vector, or tosses IAE */
  public final Vec theVec(String err) {
    if( _keys.length != 1 ) throw new IllegalArgumentException(err);
    if( _vecs == null ) _vecs = new Vec[]{_col0 = DKV.get(_keys[0]).get() };
    return _vecs[0];
  }

  /** Check that the vectors are all compatible.  All Vecs have their content
   *  sharded using same number of rows per chunk.  */
  public boolean checkCompatible( ) {
    Vec v0 = anyVec();
    if( v0 == null ) return true;
    int nchunks = v0.nChunks();
    for( Vec vec : vecs() ) {
      if( vec instanceof AppendableVec ) continue; // New Vectors are endlessly compatible
      if( vec.nChunks() != nchunks )
        throw new IllegalArgumentException("Vectors different numbers of chunks, "+nchunks+" and "+vec.nChunks());
    }
    // Also check each chunk has same rows
    for( int i=0; i<nchunks; i++ ) {
      long es = v0.chunk2StartElem(i);
      for(int j = 1; j < numCols(); ++j) {
        Vec vec = vec(j);
        if (!(vec instanceof AppendableVec) && vec.chunk2StartElem(i) != es)
          throw new IllegalArgumentException("Vector chunks have different numbers of rows, " + es + " and " + vec.chunk2StartElem(i) + " at vec " + j + " and chunk " + i);
      }
    }
    // For larger Frames, verify that the layout is compatible - else we'll be
    // endlessly cache-missing the data around the cluster, pulling copies
    // local everywhere.
    if( v0.length() > 1e4 ) {
      Key gk = v0.groupKey();
      for( Vec vec : vecs() )
        assert gk.equals(vec.groupKey()) : "Vector " + vec + " has different vector group!";
    }
    return true;
  }

  public void closeAppendables() {closeAppendables(new Futures()).blockForPending(); }
  // Close all AppendableVec
  public Futures closeAppendables(Futures fs) {
    _col0 = null;               // Reset cache
    int len = vecs().length;
    for( int i=0; i<len; i++ ) {
      Vec v = _vecs[i];
      if( v instanceof AppendableVec )
        DKV.put(_keys[i],_vecs[i] = ((AppendableVec)v).close(fs),fs);
    }
    return fs;
  }

  /** Actually remove/delete all Vecs from memory, not just from the Frame. */
  @Override public Futures delete_impl(Futures fs) {
    for( Key k : _keys ) UKV.remove(k,fs);
    _names = new String[0];
    _vecs = new Vec[0];
    _keys = new Key[0];
    return fs;
  }
  @Override public String errStr() { return "Dataset"; }

  public long byteSize() {
    long sum=0;
    for( int i=0; i<vecs().length; i++ )
      sum += _vecs[i].byteSize();
    return sum;
  }


  // Allow sorting of columns based on some function
  public void swap( int lo, int hi ) {
    assert 0 <= lo && lo < _keys.length;
    assert 0 <= hi && hi < _keys.length;
    if( lo==hi ) return;
    Vec vecs[] = vecs();
    Vec v   = vecs [lo]; vecs  [lo] = vecs  [hi]; vecs  [hi] = v;
    Key k   = _keys[lo]; _keys [lo] = _keys [hi]; _keys [hi] = k;
    String n=_names[lo]; _names[lo] = _names[hi]; _names[hi] = n;
  }

  @Override public String toString() {
    // Across
    Vec vecs[] = _vecs;
    // Do Not Cache _vecs in toString lest IdeaJ variable display cause side-effects
    if( vecs == null ) vecs = vecs_impl();
    if( vecs.length==0 ) return "{}";
    String s="{"+(_names==null?"C0":_names[0]);
    long bs=vecs[0].byteSize();
    for( int i=1; i<vecs.length; i++ ) {
      s += ","+(_names==null?"C"+i:_names[i]);
      bs+= vecs[i].byteSize();
    }
    s += "}, "+PrettyPrint.bytes(bs)+"\n";
    // Down
    Vec v0 = vecs[0];          // Do Not Cache, no side-effects
    if( v0 == null ) return s;
    int nc = v0.nChunks();
    s += "Chunk starts: {";
    for( int c=0; c<nc; c++ ) s += v0.chunk2StartElem(c)+",";
    s += "}";
    return s;
  }
  public String toStringNames() { return Arrays.toString(_names); }

  // Print a row with headers inlined
  private String toStr( long idx, int col ) {
    return _names[col]+"="+(_vecs[col].isNA(idx) ? "NA" : _vecs[col].at(idx));
  }
  public String toString( long idx ) {
    String s="{"+toStr(idx,0);
    for( int i=1; i<_names.length; i++ )
       s += ","+toStr(idx,i);
    return s+"}";
  }

  public void replaceVecs(Vec [] vecs){
    if(vecs.length != _vecs.length)
      throw new IllegalArgumentException("Incompatible number of vecs");
    _vecs = vecs;
    _col0 = _vecs[0];
    for(int i = 0; i < _keys.length; ++i)
      _keys[i] = vecs[i]._key;
  }

  // Print fixed-width row & fixed-width headers (more compressed print
  // format).  Returns the column formats.
  public String[] toStringHdr( StringBuilder sb ) {
    String[] fs = new String[numCols()];
    for( int c=0; c<fs.length; c++ ) {
      String n = (_names != null && c < _names.length) ? _names[c] : ("C"+c);
      int nlen = n.length();
      if( numRows()==0 ) { sb.append(n).append(' '); continue; }
      int w=0;
      if( _vecs[c].isEnum() ) {
        String ss[] = _vecs[c]._domain;
        for( int i=0; i<ss.length; i++ )
          w = Math.max(w,ss[i].length());
        w = Math.min(w,10);
        fs[c] = "%"+w+"."+w+"s";
      } else {
        Chunk C = _vecs[c].chunkForChunkIdx(0);   // 1st Chunk
        // Possible situation: 1) vec is INT - C is has no floats => OK
        // 2) vec is INT - C has floats => IMPOSSIBLE,
        // 3) vec is FLOAT - C has floats => OK,
        // 4) vec is FLOAT - C has no floats => find the first chunk with floats
        if (!_vecs[c].isInt() &&  !C.hasFloat()) {
          for (int i=1; i<_vecs[c].nChunks(); i++) {
            C=_vecs[c].chunkForChunkIdx(i);
            if (C.hasFloat()) break;
          }
        }
        String f = fs[c] = C.pformat();  // Printable width
        for( int x=0; x<f.length(); x++ )// Get printable width from format
          if( Character.isDigit(f.charAt(x)) ) w = w*10+(f.charAt(x)-'0');
          else if( w>0 ) break;
        if( f.charAt(1)==' ' ) w++; // Leading blank is not in print-width
      }
      int len = sb.length();
      if( nlen>1 && w==1 ) {
        fs[c]=" "+fs[c];
        w=2;
      }
      if( nlen <= w ) {         // Short name, big digits
        sb.append(n);
        for( int i=nlen; i<w; i++ ) sb.append(' ');
      } else if( w==1 ) {       // First char only
        sb.append(n.charAt(0));
      } else if( w==2 ) {       // First 2 chars only
        sb.append(n.charAt(0)).append(n.charAt(1));
      } else {                  // First char dot lastchars; e.g. Compress "Interval" to "I.val"
        sb.append(n.charAt(0)).append('.');
        for( int i=nlen-(w-2); i<nlen; i++ )
          sb.append(n.charAt(i));
      }
      assert len+w==sb.length();
      sb.append(' ');           // Column seperator
    }
    sb.append('\n');
    return fs;
  }
  public StringBuilder toString( StringBuilder sb, String[] fs, long idx ) {
    Vec vecs[] = vecs();
    for( int c=0; c<fs.length; c++ ) {
      Vec vec = vecs[c];
      if( vec.isEnum() ) {
        String s = "----------";
        if( !vec.isNA(idx) ) {
          int x = (int)vec.at8(idx);
          if( x >= 0 && x < vec._domain.length ) s = vec._domain[x];
        }
        sb.append(String.format(fs[c],s));
      } else if( vec.isInt() ) {
        if( vec.isNA(idx) ) {
          Chunk C = vec.chunkForChunkIdx(0);   // 1st Chunk
          int len = C.pformat_len0();  // Printable width
          for( int i=0; i<len; i++ ) sb.append('-');
        } else {
          try {
            if( vec.isUUID() ) sb.append(PrettyPrint.UUID(vec.at16l(idx),vec.at16h(idx)));
            else sb.append(String.format(fs[c],vec.at8(idx)));
          } catch( IllegalFormatException ife ) {
            System.out.println("Format: "+fs[c]+" col="+c+" not for ints");
            ife.printStackTrace();
          }
        }
      } else {
        sb.append(String.format(fs[c],vec.at (idx)));
        if( vec.isNA(idx) ) sb.append(' ');
      }
      sb.append(' ');           // Column seperator
    }
    sb.append('\n');
    return sb;
  }
  public String toStringAll() {
    StringBuilder sb = new StringBuilder();
    String[] fs = toStringHdr(sb);
    for( int i=0; i<numRows(); i++ )
      toString(sb,fs,i);
    return sb.toString();
  }

  // Return the entire Frame as a CSV stream
  public InputStream toCSV(boolean headers) {
    return new CSVStream(headers, false);
  }

  public InputStream toCSV(boolean headers, boolean hex_string) {
    return new CSVStream(headers, hex_string);
  }

  private class CSVStream extends InputStream {
    private final boolean _hex_string;
    byte[] _line;
    int _position;
    long _row;

    CSVStream(boolean headers, boolean hex_string) {
      _hex_string = hex_string;
      StringBuilder sb = new StringBuilder();
      Vec vs[] = vecs();
      if( headers ) {
        sb.append('"' + _names[0] + '"');
        for(int i = 1; i < vs.length; i++)
          sb.append(',').append('"' + _names[i] + '"');
        sb.append('\n');
      }
      _line = sb.toString().getBytes();
    }

    @Override public int available() throws IOException {
      if(_position == _line.length) {
        if(_row == numRows())
          return 0;
        StringBuilder sb = new StringBuilder();
        Vec vs[] = vecs();
        for( int i = 0; i < vs.length; i++ ) {
          if(i > 0) sb.append(',');
          if(!vs[i].isNA(_row)) {
            if( vs[i].isEnum() ) sb.append('"' + vs[i]._domain[(int) vs[i].at8(_row)] + '"');
            else if( vs[i].isUUID() ) sb.append(PrettyPrint.UUID(vs[i].at16l(_row),vs[i].at16h(_row)));
            else if( vs[i].isInt() ) sb.append(vs[i].at8(_row));
            else {
              // R 3.1 unfortunately changed the behavior of read.csv().
              // (Really type.convert()).
              //
              // Numeric values with too much precision now trigger a type conversion in R 3.1 into a factor.
              //
              // See these discussions:
              //   https://bugs.r-project.org/bugzilla/show_bug.cgi?id=15751
              //   https://stat.ethz.ch/pipermail/r-devel/2014-April/068778.html
              //   http://stackoverflow.com/questions/23072988/preserve-old-pre-3-1-0-type-convert-behavior

              double d = vs[i].at(_row);

              String s;
              if (_hex_string) {
                // Used by R's as.data.frame().
                s = Double.toHexString(d);
              }
              else {
                // To emit CSV files that can be read by R 3.1, limit the number of significant digits.
                // s = String.format("%.15g", d);

                s = Double.toString(d);
              }

              sb.append(s);
            }
          }
        }
        sb.append('\n');
        _line = sb.toString().getBytes();
        _position = 0;
        _row++;
      }
      return _line.length - _position;
    }

    @Override public void close() throws IOException {
      super.close();
      _line = null;
    }

    @Override public int read() throws IOException {
      return available() == 0 ? -1 : _line[_position++];
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
      int n = available();
      if(n > 0) {
        n = Math.min(n, len);
        System.arraycopy(_line, _position, b, off, n);
        _position += n;
      }
      return n;
    }
  }


  // --------------------------------------------------------------------------
  // In support of R, a generic Deep Copy & Slice.
  // Semantics are a little odd, to match R's.
  // Each dimension spec can be:
  //   null - all of them
  //   a sorted list of negative numbers (no dups) - all BUT these
  //   an unordered list of positive - just these, allowing dups
  // The numbering is 1-based; zero's are not allowed in the lists, nor are out-of-range.
  final int MAX_EQ2_COLS = 100000;      // FIXME.  Put this in a better spot.
  public Frame deepSlice( Object orows, Object ocols ) {
    // ocols is either a long[] or a Frame-of-1-Vec
    long[] cols = null;
    if( ocols == null ) cols = null;
    else if (ocols instanceof long[]) cols = (long[])ocols;
    else if (ocols instanceof Frame) {
      Frame fr = (Frame) ocols;
      if (fr.numCols() != 1)
        throw new IllegalArgumentException("Columns Frame must have only one column (actually has " + fr.numCols() + " columns)");
      long n = fr.anyVec().length();
      if (n > MAX_EQ2_COLS)
        throw new IllegalArgumentException("Too many requested columns (requested " + n +", max " + MAX_EQ2_COLS + ")");
      cols = new long[(int)n];
      Vec v = fr.anyVec();
      for (long i = 0; i < v.length(); i++)
        cols[(int)i] = v.at8(i);
    } else
      throw new IllegalArgumentException("Columns is specified by an unsupported data type (" + ocols.getClass().getName() + ")");

    // Since cols is probably short convert to a positive list.
    int c2[] = null;
    if( cols==null ) {
      c2 = new int[numCols()];
      for( int i=0; i<c2.length; i++ ) c2[i]=i;
    } else if( cols.length==0 ) {
      c2 = new int[0];
    } else if( cols[0] > 0 ) {
      c2 = new int[cols.length];
      for( int i=0; i<cols.length; i++ )
        c2[i] = (int)cols[i]-1; // Convert 1-based cols to zero-based
    } else {
      c2 = new int[numCols()-cols.length];
      int j=0;
      for( int i=0; i<numCols(); i++ ) {
        if( j >= cols.length || i < (-cols[j]-1) ) c2[i-j] = i;
        else j++;
      }
    }
    for( int i=0; i<c2.length; i++ )
      if( c2[i] >= numCols() )
        throw new IllegalArgumentException("Trying to select column "+(c2[i]+1)+" but only "+numCols()+" present.");
    if( c2.length==0 )
      throw new IllegalArgumentException("No columns selected (did you try to select column 0 instead of column 1?)");

    // Do Da Slice
    // orows is either a long[] or a Vec
    if (orows == null)
      return copyRollups(new DeepSlice(null,c2,vecs()).doAll(c2.length,this).outputFrame(names(c2),domains(c2)),true);
    else if (orows instanceof long[]) {
      final long CHK_ROWS=1000000;
      final long[] rows = (long[])orows;
      if (this.numRows() == 0) {
        return this;
      }
      if( rows.length==0 || rows[0] < 0 ) {
        if (rows.length != 0 && rows[0] < 0) {
          Vec v = new MRTask2() {
            @Override public void map(Chunk cs) {
              for (long er : rows) {
                if (er >= 0) continue;
                er = Math.abs(er) - 1; // 1-based -> 0-based
                if (er < cs._start || er > (cs._len + cs._start - 1)) continue;
                cs.set0((int) (er - cs._start), 1);
              }
            }
          }.doAll(this.anyVec().makeZero()).getResult()._fr.anyVec();
          Frame slicedFrame = new DeepSlice(rows, c2, vecs()).doAll(c2.length, this.add("select_vec", v)).outputFrame(names(c2), domains(c2));
          UKV.remove(v._key);
          UKV.remove(this.remove(this.numCols()-1)._key);
          return copyRollups(slicedFrame, false);
        } else {
          return copyRollups(new DeepSlice(rows.length == 0 ? null : rows, c2, vecs()).doAll(c2.length, this).outputFrame(names(c2), domains(c2)), rows.length == 0);
        }
      }
      // Vec'ize the index array
      Futures fs = new Futures();
      AppendableVec av = new AppendableVec(Vec.newKey(Key.make("rownames")));
      int r = 0;
      int c = 0;
      while (r < rows.length) {
        NewChunk nc = new NewChunk(av, c);
        long end = Math.min(r+CHK_ROWS, rows.length);
        for (; r < end; r++) {
          nc.addNum(rows[r]);
        }
        nc.close(c++, fs);
      }
      Vec c0 = av.close(fs);   // c0 is the row index vec
      fs.blockForPending();
      Frame fr2 = new Slice(c2, this).doAll(c2.length,new Frame(new String[]{"rownames"}, new Vec[]{c0}))
              .outputFrame(names(c2), domains(c2));
      UKV.remove(c0._key);      // Remove hidden vector
      return fr2;
    }
    Frame frows = (Frame)orows;
    Vec vrows = frows.anyVec();
    // It's a compatible Vec; use it as boolean selector.
    // Build column names for the result.
    Vec [] vecs = new Vec[c2.length+1];
    String [] names = new String[c2.length+1];
    for(int i = 0; i < c2.length; ++i){
      vecs[i] = _vecs[c2[i]];
      names[i] = _names[c2[i]];
    }
    vecs[c2.length] = vrows;
    names[c2.length] = "predicate";
    return new DeepSelect().doAll(c2.length,new Frame(names,vecs)).outputFrame(names(c2),domains(c2));
  }

  // Slice and return in the form of new chunks.
  private static class Slice extends MRTask2<Slice> {
    final Frame  _base;   // the base frame to slice from
    final int[]  _cols;
    Slice(int[] cols, Frame base) { _cols = cols; _base = base; }
    @Override public void map(Chunk[] ix, NewChunk[] ncs) {
      final Vec[] vecs = new Vec[_cols.length];
      final Vec   anyv = _base.anyVec();
      final long  nrow = anyv.length();
            long  r    = ix[0].at80(0);
      int   last_ci = anyv.elem2ChunkIdx(r<nrow?r:0); // memoize the last chunk index
      long  last_c0 = anyv._espc[last_ci];            // ...         last chunk start
      long  last_c1 = anyv._espc[last_ci + 1];        // ...         last chunk end
      Chunk[] last_cs = new Chunk[vecs.length];       // ...         last chunks
      for (int c = 0; c < _cols.length; c++) {
        vecs[c] = _base.vecs()[_cols[c]];
        last_cs[c] = vecs[c].chunkForChunkIdx(last_ci);
      }
      for (int i = 0; i < ix[0]._len; i++) {
        // select one row
        r = ix[0].at80(i) - 1;   // next row to select
        if (r < 0) continue;
        if (r >= nrow) {
          for (int c = 0; c < vecs.length; c++) ncs[c].addNum(Double.NaN);
        } else {
          if (r < last_c0 || r >= last_c1) {
            last_ci = anyv.elem2ChunkIdx(r);
            last_c0 = anyv._espc[last_ci];
            last_c1 = anyv._espc[last_ci + 1];
            for (int c = 0; c < vecs.length; c++)
              last_cs[c] = vecs[c].chunkForChunkIdx(last_ci);
          }
          for (int c = 0; c < vecs.length; c++)
            if( vecs[c].isUUID() ) ncs[c].addUUID(last_cs[c],r);
            else                   ncs[c].addNum (last_cs[c].at(r));
        }
      }
    }
  }

  // Bulk (expensive) copy from 2nd cols into 1st cols.
  // Sliced by the given cols & rows
  private static class DeepSlice extends MRTask2<DeepSlice> {
    final int  _cols[];
    final long _rows[];
    final byte _isInt[];
    boolean _ex = true;
    DeepSlice( long rows[], int cols[], Vec vecs[] ) {
      _cols=cols;
      _rows=rows;
      _isInt = new byte[cols.length];
      for( int i=0; i<cols.length; i++ )
        _isInt[i] = (byte)(vecs[cols[i]].isInt() ? 1 : 0);
    }

    @Override public boolean logVerbose() { return false; }

    @Override public void map( Chunk chks[], NewChunk nchks[] ) {
      long rstart = chks[0]._start;
      int rlen = chks[0]._len;  // Total row count
      int rx = 0;               // Which row to in/ex-clude
      int rlo = 0;              // Lo/Hi for this block of rows
      int rhi = rlen;
      if (_rows != null && _rows[0] < 0) {
        // Skip any rows that have 1 in the last column!
        Chunk select_vec = chks[chks.length-1];
        for (int i = 0; i < _cols.length; i++) {
          Chunk oc = chks[_cols[i]];
          NewChunk nc = nchks[i];
          if (_isInt[i] == 1) { // Slice on integer columns
            for (int j = 0; j < oc._len; j++) {
              if (select_vec.at80(j) == 1) continue;
              if (oc._vec.isUUID()) nc.addUUID(oc, j);
              else if (oc.isNA0(j)) nc.addNA();
              else nc.addNum(oc.at80(j), 0);
            }
          } else {                // Slice on double columns
            for (int j = 0; j < oc._len; j++) {
              if (select_vec.at80(j) == 1) continue;
              nc.addNum(oc.at0(j));
            }
          }
        }
      } else {
        while (true) {           // Still got rows to include?
          if (_rows != null) {   // Got a row selector?
            if (rx >= _rows.length) break; // All done with row selections
            long r = _rows[rx++] - 1;// Next row selector
            if (r < rstart) continue;
            rlo = (int) (r - rstart);
            rhi = rlo + 1;        // Stop at the next row
            while (rx < _rows.length && (_rows[rx] - 1 - rstart) == rhi && rhi < rlen) {
              rx++;
              rhi++;      // Grab sequential rows
            }
          }
          // Process this next set of rows
          // For all cols in the new set
          for (int i = 0; i < _cols.length; i++) {
            Chunk oc = chks[_cols[i]];
            NewChunk nc = nchks[i];
            if (_isInt[i] == 1) { // Slice on integer columns
              for (int j = rlo; j < rhi; j++)
                if (oc._vec.isUUID()) nc.addUUID(oc, j);
                else if (oc.isNA0(j)) nc.addNA();
                else nc.addNum(oc.at80(j), 0);
            } else {                // Slice on double columns
              for (int j = rlo; j < rhi; j++)
                nc.addNum(oc.at0(j));
            }
          }
          rlo = rhi;
          if (_rows == null) break;
        }
      }
    }
  }


  public static Frame[] runifSplit(Frame f, float threshold, long seed) {
    if (seed == -1) seed = new Random().nextLong();
    Vec rv = new Vec(f.anyVec().group().addVecs(1)[0],f.anyVec()._espc);
    Futures fs = new Futures();
    DKV.put(rv._key,rv, fs);
    for(int i = 0; i < rv._espc.length-1; ++i)
      DKV.put(rv.chunkKey(i),new C0DChunk(0,(int)(rv._espc[i+1]-rv._espc[i])),fs);
    fs.blockForPending();
    final long zeed = seed;
    new MRTask2() {
      @Override public void map(Chunk c){
        Random rng = new Random(zeed*c.cidx());
        for(int i = 0; i < c._len; ++i)
          c.set0(i, (float)rng.nextDouble());
      }
    }.doAll(rv);
    Vec[] vecs = new Vec[f.numCols()+1];
    System.arraycopy(f.vecs(), 0, vecs,0, f.numCols());
    vecs[f.numCols()] = rv;
    Frame doAllFr = new Frame(null, vecs);
    // it would be great if there was a map call for NewChunk[][] multi frame output
    Frame left = new DeepSelectThresh(threshold,  true).doAll(f.numCols(),doAllFr).outputFrame(Key.make(), f.names(), f.domains());
    Frame rite = new DeepSelectThresh(threshold, false).doAll(f.numCols(),doAllFr).outputFrame(Key.make(), f.names(), f.domains());
    UKV.remove(rv._key);
    return new Frame[]{left,rite};
  }

  private static class DeepSelect extends MRTask2<DeepSelect> {
    @Override public void map( Chunk chks[], NewChunk nchks[] ) {
      Chunk pred = chks[chks.length-1];
      for(int i = 0; i < pred._len; ++i) {
        if(pred.at0(i) != 0) {
          for( int j = 0; j < chks.length - 1; j++ ) {
            Chunk chk = chks[j];
            if( chk._vec.isUUID() ) nchks[j].addUUID(chk,i);
            else nchks[j].addNum(chk.at0(i));
          }
        }
      }
    }
  }

  private static class DeepSelectThresh extends MRTask2<DeepSelectThresh> {
    private final float _threshold;
    private final boolean _left;
    DeepSelectThresh(float threshold, boolean left) { _threshold = threshold; _left = left; }

    private void addRow(Chunk[] cs, NewChunk[] ncs, int i) {
      for (int j = 0; j < cs.length -1; ++j) {
        Chunk c = cs[j];
        if (c._vec.isUUID()) ncs[j].addUUID(c,i);
        else ncs[j].addNum(c.at0(i)); // NewChunk will compress later ... not set0s
      }
    }

    @Override public void map(Chunk cs[], NewChunk ncs[]) {
      Chunk rv = cs[cs.length-1];
      for (int i = 0; i < rv._len; ++i) {
        if (_left) {
          if (rv.at0(i) <= _threshold) addRow(cs, ncs, i);
        } else {
          if (rv.at0(i) > _threshold)  addRow(cs, ncs, i);
        }
      }
    }
  }

  private Frame copyRollups( Frame fr, boolean isACopy ) {
    if( !isACopy ) return fr; // Not a clean copy, do not copy rollups (will do rollups "the hard way" on first ask)
    Vec vecs0[] = vecs();
    Vec vecs1[] = fr.vecs();
    for( int i=0; i<fr._names.length; i++ ) {
      assert vecs1[i]._naCnt== -1; // not computed yet, right after slice
      Vec v0 = vecs0[find(fr._names[i])];
      Vec v1 = vecs1[i];
      v1.setRollupStats(v0);
    }
    return fr;
  }

  // ------------------------------------------------------------------------------

  public
  <Y extends Flow.PerRow<Y>>      // Type parameter
  Flow.FlowPerRow<Y>              // Return type of with()
  with                            // The method name
  ( Flow.PerRow<Y> pr )           // Arguments for with()
  {
    return new Flow.FlowPerRow<Y>(pr,new Flow.FlowFrame(this));
  }

  public Flow.FlowFilter with( Flow.Filter fr ) {
    return new Flow.FlowFilter(fr,new Flow.FlowFrame(this));
  }

  public Flow.FlowGroupBy with( Flow.GroupBy fr ) {
    return new Flow.FlowGroupBy(fr,new Flow.FlowFrame(this));
  }
}
