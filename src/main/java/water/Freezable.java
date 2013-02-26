package water;

/**
 * Empty marker interface.  Used by the auto-serializer to inject implementations.
 */
public interface Freezable {
  public AutoBuffer write(AutoBuffer bb);
  public <T extends Freezable> T read(AutoBuffer bb);
  public <T extends Freezable> T newInstance();
  public int frozenType();
}
