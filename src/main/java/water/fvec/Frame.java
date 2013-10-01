package water.fvec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import water.*;
import water.fvec.Vec.VectorGroup;

/**
 * A collection of named Vecs.  Essentially an R-like data-frame.  Multiple
 * Frames can reference the same Vecs.  A Frame is a lightweight object, it is
 * meant to be cheaply created and discarded for data munging purposes.
 * E.g. to exclude a Vec from a computation on a Frame, create a new Frame that
 * references all the Vecs but this one.
 */
public class Frame extends Iced {
  public String[] _names;
  private Key[] _keys;          // Keys for the vectors
  private transient Vec[] _vecs;// The Vectors (transient to avoid network traffic)
  private transient Vec _col0;  // First readable vec; fast access to the VecGroup's Chunk layout

  public Frame( Frame fr ) { this(fr._names.clone(), fr.vecs().clone()); _col0 = fr._col0; }
  public Frame( Vec... vecs ){ this(null,vecs);}
  public Frame( String[] names, Vec[] vecs ) {
    _names=names;
    _vecs=vecs;
    _keys = new Key[vecs.length];
    for( int i=0; i<vecs.length; i++ ) {
      Key k = _keys[i] = vecs[i]._key;
      if( DKV.get(k)==null )    // If not already in KV, put it there
        DKV.put(k,vecs[i]);
    }
  }

  public final Vec[] vecs() {
    if( _vecs != null ) return _vecs;
    _vecs = new Vec[_keys.length];
    for( int i=0; i<_keys.length; i++ )
      _vecs[i] = DKV.get(_keys[i]).get();
    return _vecs;
  }
  // Force a cache-flush & reload, assuming vec mappings were altered remotely
  public final Vec[] reloadVecs() { _vecs=null; return vecs(); }

  /** Finds the first column with a matching name.  */
  public int find( String name ) {
    for( int i=0; i<_names.length; i++ )
      if( name.equals(_names[i]) )
        return i;
    return -1;
  }

 /** Appends a named column, keeping the last Vec as the response */
  public void add( String name, Vec vec ) {
    // TODO : needs a compatibility-check!!!
    final int len = _names.length;
    _names = Arrays.copyOf(_names,len+1);
    _vecs  = Arrays.copyOf(_vecs ,len+1);
    _keys  = Arrays.copyOf(_keys ,len+1);
    _names[len] = name;
    _vecs [len] = vec ;
    _keys [len] = vec._key;
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
    for(int i = 0; i < _vecs.length; ++i)
      if(j < idxs.length && i == idxs[j]){
        ++j;
        res[k++] = _vecs[i];
      } else {
        rem  [l] = _vecs [i];
        names[l] = _names[i];
        keys [l] = _keys [i];
        ++l;
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
    Vec v = _vecs[idx];
    System.arraycopy(_names,idx+1,_names,idx,len-idx-1);
    System.arraycopy(_vecs ,idx+1,_vecs ,idx,len-idx-1);
    System.arraycopy(_keys ,idx+1,_keys ,idx,len-idx-1);
    _names = Arrays.copyOf(_names,len-1);
    _vecs  = Arrays.copyOf(_vecs ,len-1);
    _keys  = Arrays.copyOf(_keys ,len-1);
    if( v == _col0 ) _col0 = null;
    return v;
  }

  public final String[] names() { return _names; }
  public int  numCols() { return _vecs.length; }
  public long numRows(){ return anyVec().length();}

  /** All the domains for enum columns; null for non-enum columns.  */
  public String[][] domains() {
    String ds[][] = new String[_vecs.length][];
    for( int i=0; i<_vecs.length; i++ )
      ds[i] = _vecs[i].domain();
    return ds;
  }

  /** Returns the first readable vector. */
  public Vec anyVec() {
    if( _col0 != null ) return _col0;
    for( Vec v : vecs() )
      if( v.readable() )
        return (_col0 = v);
    return null;
  }

  /** Check that the vectors are all compatible.  All Vecs have their content
   *  sharded using same number of rows per chunk.  */
  public void checkCompatible( ) {
    Vec v0 = anyVec();
    int nchunks = v0.nChunks();
    for( Vec vec : _vecs ) {
      if( vec instanceof AppendableVec ) continue; // New Vectors are endlessly compatible
      if( vec.nChunks() != nchunks )
        throw new IllegalArgumentException("Vectors different numbers of chunks, "+nchunks+" and "+vec.nChunks());
    }
    // Also check each chunk has same rows
    for( int i=0; i<nchunks; i++ ) {
      long es = v0.chunk2StartElem(i);
      for( Vec vec : _vecs )
        if( !(vec instanceof AppendableVec) && vec.chunk2StartElem(i) != es )
          throw new IllegalArgumentException("Vector chunks different numbers of rows, "+es+" and "+vec.chunk2StartElem(i));
    }
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

  // True if any Appendables exist
  public boolean hasAppendables() {
    for( Vec v : vecs() )
      if( v instanceof AppendableVec )
        return true;
    return false;
  }

  public void remove() {
    remove(new Futures());
  }

  /** Actually remove/delete all Vecs from memory, not just from the Frame. */
  public void remove(Futures fs){
    if(_vecs.length > 0){
      VectorGroup vg = _vecs[0].group();
      for( Vec v : _vecs )
        UKV.remove(v._key,fs);
      DKV.remove(vg._key);
    }
    _names = new String[0];
    _vecs = new Vec[0];
    _keys = new Key[0];
  }

  public long byteSize() {
    long sum=0;
    for( int i=0; i<_vecs.length; i++ )
      sum += _vecs[i].byteSize();
    return sum;
  }

  @Override public String toString() {
    // Across
    String s="{"+_names[0];
    long bs=_vecs[0].byteSize();
    for( int i=1; i<_names.length; i++ ) {
      s += ","+_names[i];
      bs+= _vecs[i].byteSize();
    }
    s += "}, "+PrettyPrint.bytes(bs)+"\n";
    // Down
    Vec v0 = anyVec();
    if( v0 == null ) return s;
    int nc = v0.nChunks();
    s += "Chunk starts: {";
    for( int i=0; i<nc; i++ ) s += v0.elem2BV(i)._start+",";
    s += "}";
    return s;
  }

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

  // Print fixed-width row & fixed-width headers (more compressed print
  // format).  Returns the column formats.
  public String[] toStringHdr( StringBuilder sb ) {
    String[] fs = new String[numCols()];
    for( int c=0; c<fs.length; c++ ) {
      String n = _names[c];
      Chunk C = _vecs[c].elem2BV(0);   // 1st Chunk
      String f = fs[c] = C.pformat();  // Printable width
      int w=0;
      for( int x=0; x<f.length(); x++ )// Get printable width from format
        if( Character.isDigit(f.charAt(x)) ) w = w*10+(f.charAt(x)-'0');
        else if( w>0 ) break;
      if( f.charAt(1)==' ' ) w++; // Leading blank is not in print-width
      int len = sb.length();
      if( n.length() <= w ) {          // Short name, big digits
        sb.append(n);
        for( int i=n.length(); i<w; i++ ) sb.append(' ');
      } else if( w==1 ) {       // First char only
        sb.append(n.charAt(0));
      } else if( w==2 ) {       // First 2 chars only
        sb.append(n.charAt(0)).append(n.charAt(1));
      } else {                  // First char dot lastchars; e.g. Compress "Interval" to "I.val"
        sb.append(n.charAt(0)).append(' ');
        for( int i=n.length()-(w-2); i<n.length(); i++ )
          sb.append(n.charAt(i));
      }
      assert len+w==sb.length();
      sb.append(' ');           // Column seperator
    }
    sb.append('\n');
    return fs;
  }
  public StringBuilder toString( StringBuilder sb, String[] fs, long idx ) {
    for( int c=0; c<fs.length; c++ ) {
      if( _vecs[c].isInt() ) {
        if( _vecs[c].isNA(idx) ) {
          Chunk C = _vecs[c].elem2BV(0);   // 1st Chunk
          int len = C.pformat_len0();  // Printable width
          for( int i=0; i<len; i++ ) sb.append('-');
        } else
          sb.append(String.format(fs[c],_vecs[c].at8(idx)));
      } else {
        sb.append(String.format(fs[c],_vecs[c].at (idx)));
        if( _vecs[c].isNA(idx) ) sb.append(' ');
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
    return new CSVStream(headers);
  }

  private class CSVStream extends InputStream {
    byte[] _line;
    int _position;
    long _row;

    CSVStream(boolean headers) {
      StringBuilder sb = new StringBuilder();
      if( headers ) {
        sb.append('"' + _names[0] + '"');
        for(int i = 1; i < _vecs.length; i++)
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
        for( int i = 0; i < _vecs.length; i++ ) {
          if(i > 0) sb.append(',');
          if(!_vecs[i].isNA(_row)) {
            if(_vecs[i].isEnum()) sb.append('"' + _vecs[i]._domain[(int) _vecs[i].at8(_row)] + '"');
            else if(_vecs[i].isInt()) sb.append(_vecs[i].at8(_row));
            else sb.append(_vecs[i].at(_row));
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
}
