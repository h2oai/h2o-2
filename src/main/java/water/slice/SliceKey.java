package water.slice;

import water.Iced;

abstract public class SliceKey extends Iced {
  abstract public SliceKey makeCopy();
  abstract public int hashCode();
  abstract public boolean equals(Object o);
}
