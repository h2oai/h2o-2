package water;
import water.H2O.H2OCountedCompleter;
import jsr166y.CountedCompleter;

/** Objects which are passed & remotely executed.<p>
 * <p>
 * Efficient serialization methods for subclasses will be automatically
 * generated, but explicit ones can be provided.  Transient fields will
 * <em>not</em> be mirrored between the VMs.
 * <ol>
 * <li>On the local vm, this task will be serialized and sent to a remote.</li>
 * <li>On the remote, the task will be deserialized.</li>
 * <li>On the remote, the {@link #invoke(H2ONode)} method will be executed.</li>
 * <li>On the remote, the task will be serialized and sent to the local vm</li>
 * <li>On the local vm, the task will be deserialized
 * <em>into the original instance</em></li>
 * <li>On the local vm, the {@link #onAck()} method will be executed.</li>
 * <li>On the remote, the {@link #onAckAck()} method will be executed.</li>
 * </ol>
 */
public abstract class DTask<T> extends H2OCountedCompleter implements Freezable {
  // this field is NOT serialized as DTask is serBase
  boolean _repliedTcp;         // Any return/reply/result was sent via TCP

  /**
   * Simple class to allow for DTask with serialized fields (DTask's fields are not serialized as it is serialization base class).
   *
   * @author tomasnykodym
   *
   * @param <T>
   */
  public static abstract class DTaskImpl<T> extends DTask<T> {
    private int _fjPriorityLvl;
    public final int priority(){return _fjPriorityLvl;}
    public final void setPriority(int p){_fjPriorityLvl = p;}
  }

  /** Top-level remote execution hook.  Called on the <em>remote</em>. */
  abstract public T invoke( H2ONode sender );



  /** 2nd top-level execution hook.  After the primary task has received a
   * result (ACK) and before we have sent an ACKACK, this method is executed
   * on the <em>local vm</em>.  Transients from the local vm are available here.
   */
  public void onAck() {}

  /** 3rd top-level execution hook.  After the original vm sent an ACKACK,
   * this method is executed on the <em>remote</em>.  Transients from the remote
   * vm are available here.
   */
  public void onAckAck() {}


  /** Is this task high priority.  Tasks that need to be serviced quickly to
   * maintain forward progress and/or prevent deadlocks should override this
   * method to return true. */

  // Oops, uncaught exception
  @Override
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
    ex.printStackTrace();
    return true;
  }

  // The abstract methods to be filled in by subclasses.  These are automatically
  // filled in by any subclass of DTask during class-load-time, unless one
  // is already defined.  These methods are NOT DECLARED ABSTRACT, because javac
  // thinks they will be called by subclasses relying on the auto-gen.
  private Error barf() {
    return new Error(getClass().toString()+" should be automatically overridden in the subclass by the auto-serialization code");
  }
  @Override public AutoBuffer write(AutoBuffer bb) { throw barf(); }
  @Override public <F extends Freezable> F read(AutoBuffer bb) { throw barf(); }
  @Override public <F extends Freezable> F newInstance() { throw barf(); }
}
