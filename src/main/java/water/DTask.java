package water;

import water.DException.DistributedException;
import water.H2O.H2OCountedCompleter;

/** Objects which are passed and remotely executed.<p>
 * <p>
 * Efficient serialization methods for subclasses will be automatically
 * generated, but explicit ones can be provided.  Transient fields will
 * <em>not</em> be mirrored between the VMs.
 * <ol>
 * <li>On the local vm, this task will be serialized and sent to a remote.</li>
 * <li>On the remote, the task will be deserialized.</li>
 * <li>On the remote, the H2ONode invoke method will be executed.</li>
 * <li>On the remote, the task will be serialized and sent to the local vm</li>
 * <li>On the local vm, the task will be deserialized
 * <em>into the original instance</em></li>
 * <li>On the local vm, the {@link #onAck()} method will be executed.</li>
 * <li>On the remote, the {@link #onAckAck()} method will be executed.</li>
 * </ol>
 *
 */
public abstract class DTask<T extends DTask> extends H2OCountedCompleter implements Freezable {
  protected DTask(){}
  public DTask(H2OCountedCompleter completer){super(completer);}

  // Return a distributed-exception
  protected DException _ex;
  public final boolean hasException() { return _ex != null; }
  public synchronized void setException(Throwable ex) { if( _ex==null ) _ex = new DException(ex); }
  public DistributedException getDException() { return _ex==null ? null : _ex.toEx(); }

  // Track if the reply came via TCP - which means a timeout on ACKing the TCP
  // result does NOT need to get the entire result again, just that the client
  // needs more time to process the TCP result.
  transient boolean _repliedTcp; // Any return/reply/result was sent via TCP

  /** Top-level remote execution hook.  Called on the <em>remote</em>. */
  public void dinvoke( H2ONode sender ) { compute2(); }

  /** 2nd top-level execution hook.  After the primary task has received a
   * result (ACK) and before we have sent an ACKACK, this method is executed on
   * the <em>local vm</em>.  Transients from the local vm are available here. */
  public void onAck() {}

  /** 3rd top-level execution hook.  After the original vm sent an ACKACK, this
   * method is executed on the <em>remote</em>.  Transients from the remote vm
   * are available here.  */
  public void onAckAck() {}

  /** Override to remove 2 lines of logging per RPC.  0.5M RPC's will lead to
   *  1M lines of logging at about 50 bytes/line produces 50M of log file,
   *  which will swamp all other logging output. */
  public boolean logVerbose() { return true; }

  @Override public AutoBuffer write(AutoBuffer bb) { return bb.put(_ex); }
  @Override public <T extends Freezable> T read(AutoBuffer bb) { _ex = bb.get(); return (T)this; }
  @Override public <F extends Freezable> F newInstance() { throw barf("newInstance"); }
  @Override public int frozenType() {throw barf("frozeType");}
  @Override public AutoBuffer writeJSONFields(AutoBuffer bb) { return bb; }
  @Override public water.api.DocGen.FieldDoc[] toDocField() { return null; }
  public void copyOver(Freezable other) {
    DTask that = (DTask)other;
    this._ex = that._ex;        // Copy verbatim semantics, replacing all fields
  }
  private RuntimeException barf(String method) {
    return new RuntimeException(H2O.SELF + ":" + getClass().toString()+ " " + method +  " should be automatically overridden in the subclass by the auto-serialization code");
  }

  /**
   * Task to be executed at home of the given key.
   * Basically a wrapper around DTask which enables us to bypass
   * remote/local distinction (RPC versus submitTask).
   */
  public static abstract class DKeyTask<T extends DKeyTask,V extends Iced> extends Iced{
    private transient H2OCountedCompleter _task;
    public DKeyTask(final Key k) {this(null,k);}
    public DKeyTask(H2OCountedCompleter cmp,final Key k) {
      final DKeyTask dk = this;

      final DTask dt = new DTask(cmp) {
        @Override
        public void compute2() {
          Value val = H2O.get(k);
          if(val != null) {
            V v = val.get();
            dk._task = this;
            dk.map(v);
          }
          tryComplete();
        }
      };
      if(k.home()) _task = dt;
      else {
        _task = new H2OCountedCompleter() {
          @Override
          public void compute2() {
            new RPC(k.home_node(),dt).addCompleter(this).call();
          }
        };
      }
    }
    protected H2OCountedCompleter getCurrentTask(){ return _task;}
    protected abstract void map(V v);
    public void submitTask() {H2O.submitTask(_task);}
    public void forkTask() {_task.fork();}

    public T invokeTask() {
      assert _task.getCompleter() == null;
      submitTask();
      _task.join();
      return (T)this;
    }
  }

}
