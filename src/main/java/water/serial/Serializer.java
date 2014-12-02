package water.serial;

import java.io.IOException;
import java.lang.reflect.Field;

import water.AutoBuffer;
import water.Model;

/**
 * A simple serializer interface.
 */
interface Serializer<T, O, I> {
  /**
   * Save given object into given target.
   * @param e  object to serialize
   * @param output serialization destination
   * @throws IOException
   */
  public void save(T e, O output) throws IOException;
  /**
   * Load object from given destination.
   * @param e  object to be filled from
   * @param output
   * @return
   */
  public T load(T e, I input) throws IOException;

  public T load(I input) throws IOException;
}

abstract class BinarySerializer<T,O,I> implements Serializer<Model, O, I> {
  protected int id(T m) {
    int r = m.getClass().getCanonicalName().hashCode();
    for (Field f : m.getClass().getDeclaredFields())  r ^= f.getName().hashCode();
    return r;
  }

  protected AutoBuffer saveHeader(T m, AutoBuffer ab) {
    ab.put4(id(m));
    ab.putStr(m.getClass().getName());
    return ab;
  }
  protected T loadHeader(AutoBuffer ab) {
    int smId = ab.get4(); // type hash
    String smCN = ab.getStr(); // type name
    // Load it
    Class klazz = null;
    T m = null;
    try {
      klazz = Class.forName(smCN);
      m = (T) klazz.newInstance();
    } catch( Exception e ) {
      throw new IllegalArgumentException("Cannot instantiate the type " + smCN, e);
    }
    int amId = id(m);
    if (amId != smId) throw new IllegalArgumentException("Trying to load incompatible model! Actual model id = " + amId + ", stored id = " + smId+", type="+smCN);
    return m;
  }
}
