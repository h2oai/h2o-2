package water.slice;

import water.Iced;

abstract public class Slice<T extends Slice> extends Iced {
  abstract public T makeCopy();
  abstract public T merge(T that);
}
