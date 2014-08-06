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
  public DTask(){}
  public DTask(H2OCountedCompleter completer){super(completer);}
  // NOTE: DTask CAN NOT have any ICED members (FetchId is DTask, causes DEADLOCK in multinode environment)
  // exception info, it must be unrolled here
  protected String _exception;
  protected String _msg;
  protected String _eFromNode; // Node where the exception originated
  // stackTrace info
  protected int [] _lineNum;
  protected String [] _cls, _mth, _fname;

  public void setException(Throwable ex){
    _exception = ex.getClass().getName();
    _msg = ex.getMessage();
    _eFromNode = H2O.SELF.toString();
    StackTraceElement[]  stk = ex.getStackTrace();
    _lineNum = new int[stk.length];
    _cls = new String[stk.length];
    _mth = new String[stk.length];
    _fname = new String[stk.length];
    for(int i = 0; i < stk.length; ++i){
      _lineNum[i] = stk[i].getLineNumber();
      _cls[i] = stk[i].getClassName();
      _mth[i] = stk[i].getMethodName();
      _fname[i] = stk[i].getFileName();
    }
  }

  public boolean hasException(){
    return _exception != null;
  }

  public DistributedException getDException() {
    if( !hasException() ) return null;
    String msg = _msg;
    if( !_exception.equals(DistributedException.class.getName()) ) {
      msg = " from " + _eFromNode + "; " + _exception;
      if( _msg != null ) msg = msg+": "+_msg;
    }
    DistributedException dex = new DistributedException(msg,null);
    StackTraceElement [] stk = new StackTraceElement[_cls.length];
    for(int i = 0; i < _cls.length; ++i)
      stk[i] = new StackTraceElement(_cls[i],_mth[i], _fname[i], _lineNum[i]);
    dex.setStackTrace(stk);
    return dex;
  }

  // Track if the reply came via TCP - which means a timeout on ACKing the TCP
  // result does NOT need to get the entire result again, just that the client
  // needs more time to process the TCP result.
  transient boolean _repliedTcp; // Any return/reply/result was sent via TCP

  /** Top-level remote execution hook.  Called on the <em>remote</em>. */
  public void dinvoke( H2ONode sender ) { compute2(); }

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

  /** Override to remove 2 lines of logging per RPC.  0.5M RPC's will lead to
   *  1M lines of logging at about 50 bytes/line produces 50M of log file, which
   *  will swamp all other logging output. */
  public boolean logVerbose() { return true; }

  

  @Override public AutoBuffer write(AutoBuffer bb) {
    return
        bb
        .putStr(_exception)
        .putStr(_msg)
        .putStr(_eFromNode)
        .putA4(_lineNum)
        .putAStr(_fname)
        .putAStr(_cls)
        .putAStr(_mth);
  }
  @Override public <F extends Freezable> F read(AutoBuffer bb) {
    _exception = bb.getStr();
    _msg       = bb.getStr();
    _eFromNode = bb.getStr();
    _lineNum   = bb.getA4();
    _fname     = bb.getAStr();
    _cls       = bb.getAStr();
    _mth       = bb.getAStr();
    return (F)this;
  }
  @Override public <F extends Freezable> F newInstance() { throw barf("newInstance"); }
  @Override public int frozenType() {throw barf("frozeType");}
  @Override public AutoBuffer writeJSONFields(AutoBuffer bb) { return bb; }
  @Override public water.api.DocGen.FieldDoc[] toDocField() { return null; }
  public void copyOver(Freezable other) {
    DTask that = (DTask)other;
    this._exception = that._exception;
    this._eFromNode = that._eFromNode;
    this._lineNum   = that._lineNum;
    this._fname     = that._fname;
    this._msg       = that._msg;
    this._cls       = that._cls;
    this._mth       = that._mth;
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
