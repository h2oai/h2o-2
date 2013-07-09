package water;

/**
 * Empty marker interface.  Used by the auto-serializer to inject implementations.
 */
public interface Freezable {
  /** Serialize the 'this' object into the AutoBuffer, returning the AutoBuffer. */
  public AutoBuffer write(AutoBuffer bb);
  /** Deserialize from the AutoBuffer into a pre-existing 'this' object. */
  public <T extends Freezable> T read(AutoBuffer bb);
  /** Make a new instance of class 'this' with the empty constructor */
  public <T extends Freezable> T newInstance();
  /** Return the cluster-wide-unique 2-byte type ID for instances of this class */
  public int frozenType();
}
