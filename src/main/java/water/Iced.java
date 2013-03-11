package water;
/**
 * Empty marker class.  Used by the auto-serializer.
 */
public abstract class Iced implements Freezable {
  // The abstract methods to be filled in by subclasses.  These are automatically
  // filled in by any subclass of Iced during class-load-time, unless one
  // is already defined.  These methods are NOT DECLARED ABSTRACT, because javac
  // thinks they will be called by subclasses relying on the auto-gen.
  private Error barf() {
    return new Error(getClass().toString()+" should be automatically overridden in the subclass by the auto-serialization code");
  }
  @Override public AutoBuffer write(AutoBuffer bb) { throw barf(); }
  @Override public <T extends Freezable> T read(AutoBuffer bb) { throw barf(); }
  @Override public <T extends Freezable> T newInstance() { throw barf(); }
  @Override public int frozenType() { throw barf(); }
}
