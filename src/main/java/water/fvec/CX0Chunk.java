package water.fvec;

import water.*;

import java.util.Arrays;

/** specialized subtype of SPARSE chunk for boolean (bitvector); no NAs.  contains just a list of rows that are non-zero. */
public class CX0Chunk extends CXIChunk {
  // Sparse constructor
  public CX0Chunk(long [] ls, int [] xs, int[] id, int len2, int len) {super(ls,xs,id,len2, len,0);}

  @Override protected long at8_impl(int idx) {return getId(findOffset(idx)) == idx?1:0;}
  @Override protected double atd_impl(int idx) { return at8_impl(idx); }
  @Override protected final boolean isNA_impl( int i ) { return false; }

  @Override boolean hasFloat ()                 { return false; }

  @Override NewChunk inflate_impl(NewChunk nc) {
    final int len = sparseLen();
    nc._ls = MemoryManager.malloc8 (len);
    Arrays.fill(nc._ls,1);
    nc._xs = MemoryManager.malloc4 (len);
    nc._id = nonzeros();
    return nc;
  }
}
