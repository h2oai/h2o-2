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

  @Override
  public byte[] atomic(byte[] bits) {
    T old = null;
    if(bits != null) old = new AutoBuffer(bits).get();
    T nnn = atomic(old);
    if( nnn == null ) return null;
    return new AutoBuffer().put(nnn).buf();
  }
}
