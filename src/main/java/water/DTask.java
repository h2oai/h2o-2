package water;

import jsr166y.CountedCompleter;
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
  public boolean logVerbose() { return false; }

  @Override public AutoBuffer write(AutoBuffer bb) { return bb.put(_ex); }
  @Override public <T extends Freezable> T read(AutoBuffer bb) { _ex = bb.get(); return (T)this; }
  @Override public <F extends Freezable> F newInstance() { throw barf("newInstance"); }
  @Override public int frozenType() {throw barf("frozenType");}
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
  public static abstract class DKeyTask<T extends DKeyTask,V extends Iced> extends DTask<DKeyTask>{
    private final Key _key;
    public DKeyTask(final Key k) {this(null,k);}
    public DKeyTask(H2OCountedCompleter cmp,final Key k) {
      super(cmp);
      _key = k;
    }
    // override this
    protected abstract void map(V v);

    @Override public final void compute2(){
      if(_key.home()){
        Value val = H2O.get(_key);
        if(val != null) {
          V v = val.get();
          map(v);
        }
        tryComplete();
      } else new RPC(_key.home_node(),this).addCompleter(this).call();
    }
    // onCompletion must be empty here, may be invoked twice (on remote and local)
    @Override public void onCompletion(CountedCompleter cc){}

    public void submitTask() {H2O.submitTask(this);}
    public void forkTask() {fork();}
    public T invokeTask() {
      H2O.submitTask(this);
      join();
      return (T)this;
    }
  }

}
