package water.fvec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import water.*;
import water.H2O.H2OCountedCompleter;
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
  Key[] _keys;          // Keys for the vectors
  transient Vec[] _vecs;// The Vectors (transient to avoid network traffic)
  private transient Vec _col0;  // First readable vec; fast access to the VectorGroup's Chunk layout

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
    Vec v0 = anyVec();
    if( v0 == null ) return;
    VectorGroup grp = v0.group();
    for( int i=0; i<vecs.length; i++ )
      assert grp.equals(vecs[i].group());
  }

  public final Vec[] vecs() {
    if( _vecs != null ) return _vecs;
    final Vec [] vecs = new Vec[_keys.length];
    Futures fs = new Futures();
    for( int i=0; i<_keys.length; i++ ){
      final int ii = i;
      final Key k = _keys[i];
      H2OCountedCompleter t;
      H2O.submitTask(t = new H2OCountedCompleter(){
        // we need higher priority here as there is a danger of deadlock in case of many calls from MRTask2 at once
        // (e.g. frame with many vectors invokes rollup tasks for all vectors in paralell), should probably be done in CPS style in the future
        @Override public byte priority(){return H2O.MIN_HI_PRIORITY;}
        @Override public void compute2() {
          vecs[ii] = DKV.get(k).get();
          tryComplete();
        }
      });
      fs.add(t);
    }
    fs.blockForPending();
    return _vecs = vecs;
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

  public int find( Vec vec ) {
    for( int i=0; i<_vecs.length; i++ )
      if( vec.equals(_vecs[i]) )
        return i;
    return -1;
  }

 /** Appends a named column, keeping the last Vec as the response */
  public void add( String name, Vec vec ) {
    assert anyVec().group().equals(vec.group());
    final int len = _names.length;
    _names = Arrays.copyOf(_names,len+1);
    _vecs  = Arrays.copyOf(_vecs ,len+1);
    _keys  = Arrays.copyOf(_keys ,len+1);
    _names[len] = name;
    _vecs [len] = vec ;
    _keys [len] = vec._key;
  }

  /** Appends an entire Frame */
  public Frame add( Frame fr ) {
    assert anyVec().group().equals(fr.anyVec().group());
    final int len0=    _names.length;
    final int len1= fr._names.length;
    final int len = len0+len1;
    _names = Arrays.copyOf(_names,len);
    _vecs  = Arrays.copyOf(_vecs ,len);
    _keys  = Arrays.copyOf(_keys ,len);
    System.arraycopy(fr._names,0,_names,len0,len1);
    System.arraycopy(fr._vecs ,0,_vecs ,len0,len1);
    System.arraycopy(fr._keys ,0,_keys ,len0,len1);
    return this;
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
    _vecs = vec;
    _keys = keys;
    _col0 = null;
    return vec;
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
  public long numRows(){ return anyVec().length();}

  /** All the domains for enum columns; null for non-enum columns.  */
  public String[][] domains() {
    String ds[][] = new String[vecs().length][];
    for( int i=0; i<vecs().length; i++ )
      ds[i] = vecs()[i].domain();
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

  public void remove() {
    remove(new Futures());
  }

  /** Actually remove/delete all Vecs from memory, not just from the Frame. */
  public void remove(Futures fs){
    if(vecs().length > 0){
      for( Vec v : _vecs )
        UKV.remove(v._key,fs);
    }
    _names = new String[0];
    _vecs = new Vec[0];
    _keys = new Key[0];
  }

  public long byteSize() {
    long sum=0;
    for( int i=0; i<vecs().length; i++ )
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
      if( vecs()[c].isInt() ) {
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

  // Copy over column headers & enum domains from self into fr2
  public Frame copyHeaders( Frame fr2, int cols[] ) {
    Futures fs = new Futures();
    Vec[] vec2 = fr2.vecs();
    String domains[][] = domains();
    int len = cols==null ? vec2.length : cols.length;
    String ns[]  = new String[len];
    for( int i=0; i<len; i++ ) {
      ns[i] = _names [cols==null?i:cols[i]];
      vec2[i]._domain = domains[cols==null?i:cols[i]];
      DKV.put(vec2[i]._key,vec2[i],fs);
    }
    fr2._names = ns;
    fs.blockForPending();
    return fr2;
  }

  // --------------------------------------------------------------------------
  // In support of R, a generic Deep Copy & Slice.
  // Semantics are a little odd, to match R's.
  // Each dimension spec can be:
  //   null - all of them
  //   a sorted list of negative numbers (no dups) - all BUT these
  //   an unordered list of positive - just these, allowing dups
  // The numbering is 1-based; zero's are not allowed in the lists, nor are out-of-range.
  public Frame deepSlice( long rows[], long cols[] ) {
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
        c2[i] = (int)cols[i]-1;
    } else {
      c2 = new int[numCols()-cols.length];
      int j=0;
      for( int i=0; i<numCols(); i++ ) {
        if( j >= cols.length || i < (-cols[j]-1) ) c2[i-j] = i;
        else j++;
      }
    }

    // Do Da Slice
    Frame fr2 = new DeepSlice(rows,c2).doAll(c2.length,this)._outputFrame;

    // Copy over column headers & enum domains
    return copyHeaders(fr2,c2);
  }

  // Bulk (expensive) copy from 2nd cols into 1st cols.
  // Sliced by the given cols & rows
  private static class DeepSlice extends MRTask2<DeepSlice> {
    final int  _cols[];
    final long _rows[];
    DeepSlice( long rows[], int cols[] ) { _cols=cols; _rows=rows; }
    @Override public void map( Chunk chks[], NewChunk nchks[] ) {
      long rstart = chks[0]._start;
      int rlen = chks[0]._len;  // Total row count
      int rx = 0;               // Which row to in/ex-clude
      int rlo = 0;              // Lo/Hi for this block of rows
      int rhi = rlen;
      while( rlo < rlen ) {     // Still got rows to include?
        if( _rows != null ) {   // Got a row selector?
          if( rx >= _rows.length ) break; // All done with row selections
          long r = _rows[rx++]; // Next row selector
          if( r < 0 ) {         // Row exclusion?
            throw H2O.unimpl();
          } else {              // Positive row list?
            if( r < rstart ) continue;
            rlo = (int)(r-rstart);
            rhi = rlo+1;        // Stop at the next row
            while( rx < _rows.length && (_rows[rx]-rstart)==rhi && rhi < rlen ) {
              rx++; rhi++;      // Grab sequential rows
            }
          }
        }
        // Process this next set of rows
        // For all cols in the new set
        for( int i=0; i<_cols.length; i++ ) {
          Chunk    oc =  chks[_cols[i]];
          NewChunk nc = nchks[      i ];
          if( oc._vec.isInt() ) { // Slice on integer columns
            for( int j=rlo; j<rhi; j++ )
              if( oc.isNA0(j) ) nc.addNA();
              else              nc.addNum(oc.at80(j),0);
          } else {                // Slice on double columns
            for( int j=rlo; j<rhi; j++ )
              nc.addNum(oc.at0(j));
          }
        }
        rlo=rhi;
      }
    }
  }
}
