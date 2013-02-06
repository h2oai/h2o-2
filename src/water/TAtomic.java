package water;

/**
 * A typed atomic update.
 */
public abstract class TAtomic<T extends Freezable> extends Atomic {
  /** Atomically update an old value to a new one.
   * @param old  The old value, it may be null
   * @return The new value; if null if this atomic update no longer needs to be run
   */
  public abstract T atomic(T old);

  /** Allocate an instance of T */
  public abstract T alloc();

  @Override
  public byte[] atomic(byte[] bits) {
    T old = null;
    if(bits != null) old = alloc().read(new AutoBuffer(bits));
    T nnn = atomic(old);
    if( nnn == null ) return null;
    return nnn.write(new AutoBuffer()).buf();
  }
}
