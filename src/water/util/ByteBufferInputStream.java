package water.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;

public final class ByteBufferInputStream extends InputStream {
  private final List<ByteBuffer> _buffers;
  private int                    _current;

  public ByteBufferInputStream(List<ByteBuffer> buffers) {
    this._buffers = buffers;
  }

  @Override
  public int read() throws IOException {
    return buffer().get() & 0xff;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if( len == 0 )
      return 0;

    ByteBuffer buffer = buffer();
    int remaining = buffer.remaining();

    if( len > remaining ) {
      buffer.get(b, off, remaining);
      return remaining;
    }

    buffer.get(b, off, len);
    return len;
  }

  private ByteBuffer buffer() throws IOException {
    while( _current < _buffers.size() ) {
      ByteBuffer buffer = _buffers.get(_current);

      if( buffer.hasRemaining() )
        return buffer;

      _current++;
    }
    throw new EOFException();
  }
}