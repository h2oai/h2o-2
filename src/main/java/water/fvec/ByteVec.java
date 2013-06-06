package water.fvec;

import java.io.InputStream;
import water.Key;
import water.H2O;

// A vector of plain Bytes.
public class ByteVec extends Vec {

  ByteVec( Key key, long espc[] ) { super(key,espc); }

  public C0Vector elem2BV( long i, int cidx ) { return (C0Vector)super.elem2BV(i,cidx); }

  // Open a stream view over the underlying data
  InputStream openStream() {
    throw H2O.unimpl();
  }
}
