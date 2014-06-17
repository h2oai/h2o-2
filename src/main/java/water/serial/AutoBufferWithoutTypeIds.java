package water.serial;

import java.nio.channels.FileChannel;

import water.*;
import water.util.Log;

/** Simple wrapper around {@link AutoBuffer}
 * which uses class names instead of type ids.
 *
 * @see AutoBuffer
 * @see TypeMap
 */
class AutoBufferWithoutTypeIds extends AutoBuffer {

    public AutoBufferWithoutTypeIds() { super(); }
    public AutoBufferWithoutTypeIds(byte[] b) { super(b); }

    public AutoBufferWithoutTypeIds(FileChannel fc, boolean read) {
      super(fc,read, (byte) 0);
    }

    private static String NULL = "^";

    private <T extends Freezable> T newInstance(String klazz) {
      try {
        return (T) Class.forName(klazz).newInstance();
      } catch( Exception e ) {
        throw Log.errRTExcept(e);
      }
    }
    @Override public AutoBuffer put(Iced f) {
      return put((Freezable) f);
    }
    @Override public AutoBuffer put(Freezable f) {
      if( f == null ) return putStr(NULL);
      putStr(f.getClass().getName());
      return f.write(this);
    }
    @Override public <T extends Freezable> T get(Class<T> t) {
      String klazz = getStr();
      if (NULL.equals(klazz)) return null;
      return newInstance(klazz).read(this);
    }
    @Override public <T extends Iced> T get() {
      String klazz = getStr();
      if (NULL.equals(klazz)) return null;
      return newInstance(klazz).read(this);
    }
    @Override public AutoBuffer putA(Iced[] fs) {
      return super.putA(fs);
    }
  }
