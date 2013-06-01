package water.fvec;

import java.io.File;
import water.*;

// A distributed file-backed Vector
//
public class NFSFileVec extends Vec {
  final long _len;              // File length

  // Make a new NFSFileVec key which holds the filename implicitly.
  // This name is used by the DVecs to load data on-demand.
  public static Key make(File f) {
    long size = f.length();
    Key k1 = PersistNFS.decodeFile(f);
    byte[] bits = new byte[1+1+4+k1._kb.length];
    bits[0] = Key.VEC;
    bits[1] = 0; // Not homed
    UDP.set4(bits,2,-1); // 0xFFFFFFFF in the chunk# area
    System.arraycopy(k1._kb,0,bits,1+1+4,k1._kb.length);
    Key k = Key.make(bits);
    // Insert the top-level FileVec key into the store
    DKV.put(k,new NFSFileVec(k,size));
    return k;
  }

  private NFSFileVec(Key key, long len) {
    super(key,null);
    _len = len;
  }

  @Override long length() { return _len; }
  @Override public int nChunks() { return (int)Math.max(1,_len>>ValueArray.LOG_CHK); }
  // Convert a row# to a chunk#.  For constant-sized chunks this is a little
  // shift-and-add math.  For variable-sized chunks this is a binary search,
  // with a sane API (JDK has an insane API).
  @Override int elem2ChunkIdx( long i ) {
    assert 0 <= i && i <= _len : " "+i+" < "+_len;
    int cidx = (int)(i>>ValueArray.LOG_CHK);
    int nc = nChunks();
    if( cidx >= nc ) cidx=nc-1; // Last chunk is larger
    assert 0 <= cidx && cidx < nc;
    return cidx;    
  }
  // Convert a chunk-index into a starting row #. Constant sized chunks
  // (except for the last, which might be a little larger), and size-1 rows so
  // this is a little shift-n-add math.
  @Override public long chunk2StartElem( int cidx ) { return (long)cidx <<ValueArray.LOG_CHK; }
  // Convert a chunk-key to a file offset. Size 1 rows, so this is a direct conversion.
  static public long chunkOffset ( Key ckey ) { return (long)chunkIdx(ckey)<<ValueArray.LOG_CHK; }
  // Reverse: convert a chunk-key into a cidx
  static public int chunkIdx(Key ckey) { assert ckey._kb[0]==Key.DVEC; return UDP.get4(ckey._kb,1+1); }

  // Convert a chunk# into a chunk - does lazy-chunk creation. As chunks are
  // asked-for the first time, we make the Key and an empty backing DVec.
  // Touching the DVec will force the file load.
  @Override public Value chunkIdx( int cidx ) {
    final long nchk = nChunks();
    assert 0 <= cidx && cidx < nchk;
    Key dkey = chunkKey(cidx);
    Value val1 = DKV.get(dkey);// Check for an existing one... will fetch data as needed
    if( val1 != null ) return val1; // Found an existing one?
    // Lazily create a DVec for this chunk
    int len = (int)(cidx < nchk-1 ? ValueArray.CHUNK_SZ : (_len-chunk2StartElem(cidx)));
    // DVec is just the raw file data with a null-compression scheme
    byte[] bits = dkey._kb.clone(); // Copy the DVec key
    Value val2 = new Value(dkey,len,null,TypeMap.C0VEC,Value.NFS);
    val2.setdsk(); // It is already on disk.
    // Atomically insert: fails on a race, but then return the old version
    Value val3 = DKV.DputIfMatch(dkey,val2,null,null);
    return val3 == null ? val2 : val3;
  }  

}
