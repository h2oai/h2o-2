package hex;

import water.Iced;
import water.fvec.Chunk;
import water.fvec.Vec;

/**
 * Extend this class to obtain an aggregate on a particular column.
 */
public abstract class Aggregate<T extends Iced> extends Iced {
  Vec _vec;
  public Aggregate(Vec vec) {_vec = vec;}
  public abstract T add(T agg2);
  public abstract T add(Chunk chk);
}
