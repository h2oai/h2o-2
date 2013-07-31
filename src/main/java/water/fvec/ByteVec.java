package water.fvec;

import java.io.IOException;
import java.io.InputStream;

import water.Key;

// A vector of plain Bytes.
public class ByteVec extends Vec {

  ByteVec( Key key, long espc[] ) { super(key,espc,true,0); }

  public C1NChunk elem2BV( int cidx ) { return (C1NChunk)super.elem2BV(cidx); }

  // Open a stream view over the underlying data
  public InputStream openStream(final Key progress) {
    return new InputStream() {
      private int _cidx, _sz;
      private C1NChunk _c0;
      @Override public int available() throws IOException {
        if( _c0 == null || _sz >= _c0._len ) {
          if( _cidx >= nChunks() ) return 0;
          _c0 = elem2BV(_cidx++);
          _sz = 0;
          if( progress != null ) ParseDataset2.onProgress(_c0._len,progress);
        }
        return _c0._len-_sz;
      }
      @Override public void close() { _cidx = nChunks(); _c0 = null; _sz = 0;}
      @Override public int read() throws IOException {
        return available() == 0 ? -1 : (int)_c0.at80(_sz++);
      }
      @Override public int read(byte[] b, int off, int len) throws IOException {
        int sz = available();
        if( sz == 0 ) return -1;
        len = Math.min(len,sz);
        System.arraycopy(_c0._mem,_sz,b,off,len);
        _sz += len;
        return len;
      }
    };
  }
}
