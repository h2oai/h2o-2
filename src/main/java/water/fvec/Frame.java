package water.fvec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import water.*;
import water.fvec.Vec.DType;
import water.fvec.Vec.VectorGroup;

/**
 * A collection of named Vecs. Essentially an R-like data-frame. Multiple
 * Frames can reference the same Vecs. A Frame is a lightweight object, it
 * is meant to be cheaply created and discarded for data munging purposes.
 * E.g. to exclude a Vec from a computation on a Frame, create a new Frame
 * that references all the Vecs but this one.
 */
public class Frame extends Iced {
  public String[] _names;
  public Vec[] _vecs;
  public Vec _col0;             // First readable vec

  public Frame( String[] names, Vec[] vecs ) {
    _names=names; _vecs=vecs;
  }

  /**
   * Finds a named column.
   */
  public int find( String name ) {
    for( int i=0; i<_names.length; i++ )
      if( name.equals(_names[i]) )
        return i;
    return -1;
  }

  /**
   * Adds a named column.
   */
  public void add( String name, Vec vec ) {
    // needs a compatibility-check????
    _names = Arrays.copyOf(_names,_names.length+1);
    _vecs  = Arrays.copyOf(_vecs ,_vecs .length+1);
    _names[_names.length-1] = name;
    _vecs [_vecs .length-1] = vec ;
  }

  /**
   * Removes a named column.
   */
  public Vec remove( String name ) { return remove(find(name)); }

  /**
   * Removes a numbered column.
   */
  public Vec remove( int idx ) {
    int len = _names.length;
    if( idx < 0 || idx >= len ) return null;
    Vec v = _vecs[idx];
    System.arraycopy(_names,idx+1,_names,idx,len-idx-1);
    System.arraycopy(_vecs ,idx+1,_vecs ,idx,len-idx-1);
    _names = Arrays.copyOf(_names,len-1);
    _vecs  = Arrays.copyOf(_vecs ,len-1);
    return v;
  }

  public final Vec[] vecs() {
    return _vecs;
  }
  public int  numCols() { return _vecs.length; }
  public long numRows(){ return _vecs[0].length();}

  /**
   * Returns the first readable vector.
   */
  public Vec firstReadable() {
    if( _col0 != null ) return _col0;
    for( Vec v : _vecs )
      if( v != null && v.readable() )
        return (_col0 = v);
    return null;
  }

  /**
   * Check that the vectors are all compatible. All Vecs have their content sharded
   * using same number of rows per chunk.
   */
  public void checkCompatible( ) {
    Vec v0 = firstReadable();
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

  public void closeAppendables() {closeAppendables(new Futures());}
  // Close all AppendableVec
  public void closeAppendables(Futures fs) {
    _col0 = null;               // Reset cache
    for( int i=0; i<_vecs.length; i++ ) {
      Vec v = _vecs[i];
      if( v != null && v instanceof AppendableVec )
        _vecs[i] = ((AppendableVec)v).close(fs);
    }
  }

  // True if any Appendables exist
  public boolean hasAppendables() {
    for( Vec v : _vecs )
      if( v instanceof AppendableVec )
        return true;
    return false;
  }

  public void remove() {
    remove(new Futures());
  }

  /**
   * Actually remove/delete all Vecs from memory, not just from the Frame.
   */
  public void remove(Futures fs){
    if(_vecs.length > 0){
      VectorGroup vg = _vecs[0].group();
      for( Vec v : _vecs )
        UKV.remove(v._key,fs);
      DKV.remove(vg._key);
    }
    _names = new String[0];
    _vecs = new Vec[0];
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
    Vec v0 = firstReadable();
    if( v0 == null ) return s;
    int nc = v0.nChunks();
    s += "Chunk starts: {";
    for( int i=0; i<nc; i++ ) s += v0.elem2BV(i)._start+",";
    s += "}";
    return s;
  }

  private String toStr( long idx, int col ) {
    return _names[col]+"="+(_vecs[col].isNA(idx) ? "NA" : _vecs[col].at(idx));
  }
  public String toString( long idx ) {
    String s="{"+toStr(idx,0);
    for( int i=1; i<_names.length; i++ )
       s += ","+toStr(idx,i);
    return s+"}";
  }

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
            else if(_vecs[i].dtype() == DType.F) sb.append(_vecs[i].at(_row));
            else sb.append(_vecs[i].at8(_row));
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
