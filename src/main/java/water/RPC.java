package water;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import jsr166y.ForkJoinPool;
import jsr166y.CountedCompleter;

/**
 * A remotely executed FutureTask.  Flow is:
 *
 * 1- Build a DTask (or subclass).  This object will be replicated remotely.
 * 2- Make a RPC object, naming the target Node.  Call (re)send.  Call get() to
 * block for result, or cancel() or isDone(), etc.
 * 3- DTask will be serialized and sent to the target; small objects via UDP
 * and large via TCP (using AutoBuffer & auto-gen serializers).
 * 4- An RPC UDP control packet will be sent to target; this will also contain
 * the DTask if its small enough.
 * 4.5- The network may replicate (or drop) the UDP packet.  Dups may arrive.
 * 4.5- Sender may timeout, and send dup control UDP packets.
 * 5- Target will capture a UDP packet, and begin filtering dups (via task#).
 * 6- Target will deserialize the DTask, and call DTask.invoke() in a F/J thread.
 * 6.5- Target continues to filter (and drop) dup UDP sends (and timeout resends)
 * 7- Target finishes call, and puts result in DTask.
 * 8- Target serializes result and sends to back to sender.
 * 9- Target sends an ACK back (may be combined with the result if small enough)
 * 10- Target puts the ACK in H2ONode.TASKS for later filtering.
 * 10.5- Target receives dup UDP request, then replies with ACK back.
 * 11- Sender recieves ACK result; deserializes; notifies waiters
 * 12- Sender sends ACKACK back
 * 12.5- Sender recieves dup ACK's, sends dup ACKACK's back
 * 13- Target recieves ACKACK, removes TASKS tracking
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class RPC<V extends DTask> implements Future<V>, Delayed, ForkJoinPool.ManagedBlocker {
  // The target remote node to pester for a response.  NULL'd out if the target
  // disappears or we cancel things (hence not final).
  H2ONode _target;

  // The distributed Task to execute.  Think: code-object+args while this RPC
  // is a call-in-progress (i.e. has an 'execution stack')
  final V _dt;

  // True if _dt contains the final answer
  volatile boolean _done;

  // A locally-unique task number; a "cookie" handed to the remote process that
  // they hand back with the response packet.  These *never* repeat, so that we
  // can tell when a reply-packet points to e.g. a dead&gone task.
  final int _tasknum;
  static AtomicInteger Locally_Unique_TaskIDs = new AtomicInteger(1);

  // Time we started this sucker up.  Controls re-send behavior.
  final long _started;
  long _retry;                  // When we should attempt a retry

  // We only send non-failing TCP info once; also if we used TCP it was large
  // so duplications are expensive.  However, we DO need to keep resending some
  // kind of "are you done yet?" UDP packet, incase the reply packet got dropped
  // (but also in case the main call was a single UDP packet and it got dropped).
  boolean _sentTcp;

  // The set of current pending tasks.  Basically a map from task# to RPC.
  static public ConcurrentHashMap<Integer,RPC<?>> TASKS = new ConcurrentHashMap<Integer,RPC<?>>();

  // Magic Cookies
  static final byte SERVER_UDP_SEND = 10;
  static final byte SERVER_TCP_SEND = 11;
  static final byte CLIENT_UDP_SEND = 12;
  static final byte CLIENT_TCP_SEND = 13;

  public static <DT extends DTask> RPC<DT> call(H2ONode target, DT dtask) {
    return new RPC(target, dtask).call();
  }

  // Make a remotely executed FutureTask.  Must name the remote target as well
  // as the remote function.  This function is expected to be subclassed.
  public RPC( H2ONode target, V dtask ) {
    _target = target;
    _dt = dtask;
    _tasknum = Locally_Unique_TaskIDs.getAndIncrement();
    _started = System.currentTimeMillis();
    _retry = RETRY_MS;
  }

  // Make an initial RPC, or re-send a packet.  Always called on 1st send; also
  // called on a timeout.
  public synchronized RPC<V> call() {
    // Keep a global record, for awhile
    TASKS.put(_tasknum,this);
    // We could be racing timeouts-vs-replies.  Blow off timeout if we have an answer.
    if( isDone() ) {
      TASKS.remove(_tasknum);
      return this;
    }
    // Default strategy: (re)fire the packet and (re)start the timeout.  We
    // "count" exactly 1 failure: just whether or not we shipped via TCP ever
    // once.  After that we fearlessly (re)send UDP-sized packets until the
    // server replies.

    // Pack classloader/class & the instance data into the outgoing
    // AutoBuffer.  If it fits in a single UDP packet, ship it.  If not,
    // finish off the current AutoBuffer (which is now going TCP style), and
    // make a new UDP-sized packet.  On a re-send of a TCP-sized hunk, just
    // send the basic UDP control packet.
    if( !_sentTcp ) {
      // Ship the UDP packet with clazz name to execute
      // totally replace me with Michal's enums!!!
      UDP.udp fjq = _dt.isHighPriority() ? UDP.udp.exechi : UDP.udp.execlo;
      AutoBuffer ab = new AutoBuffer(_target).putTask(fjq,_tasknum);
      ab.put1(CLIENT_UDP_SEND).put(_dt).close();
      if( ab.hasTCP() ) _sentTcp = true;
    }

    // Double retry until we exceed existing age.  This is the time to delay
    // until we try again.  Note that we come here immediately on creation,
    // so the first doubling happens before anybody does any waiting.  Also
    // note the generous 5sec cap: ping at least every 5 sec.
    _retry += (_retry < 5000 ) ? _retry : 5000;
    // Put self on the "TBD" list of tasks awaiting Timeout.
    // So: dont really 'forget' but remember me in a little bit.
    assert !UDPTimeOutThread.PENDING.contains(this);
    UDPTimeOutThread.PENDING.add(this);
    return this;
  }

  // Similar to FutureTask.get() but does not throw any exceptions.  Returns
  // null for canceled tasks, including those where the target dies.
  public V get() {
    if( _done ) return _dt; // Fast-path shortcut
    // Use FJP ManagedBlock for this blocking-wait - so the FJP can spawn
    // another thread if needed.
    try { ForkJoinPool.managedBlock(this); } catch( InterruptedException e ) { }
    if( _done ) return _dt; // Fast-path shortcut
    assert isCancelled();
    return null;
  }
  // Return true if blocking is unnecessary, which is true if the Task isDone.
  public boolean isReleasable() {  return isDone();  }
  // Possibly blocks the current thread.  Returns true if isReleasable would
  // return true.  Used by the FJ Pool management to spawn threads to prevent
  // deadlock is otherwise all threads would block on waits.
  public synchronized boolean block() {
    while( !isDone() ) { try { wait(); } catch( InterruptedException e ) { } }
    return true;
  }

  public final V get(long timeout, TimeUnit unit) {
    if( _done ) return _dt;     // Fast-path shortcut
    throw H2O.unimpl();
  }

  // Done if target is dead or canceled, or we have a result.
  public final boolean isDone() {  return _target==null || _done;  }
  // Done if target is dead or canceled
  public final boolean isCancelled() { return _target==null; }
  // Attempt to cancel job
  public final boolean cancel( boolean mayInterruptIfRunning ) {
    boolean did = false;
    synchronized(this) {        // Install the answer under lock
      if( !isCancelled() ) {
        did = true;             // Did cancel (was not canceled already)
        _target = null;         // Flag as canceled
        UDPTimeOutThread.PENDING.remove(this);
        TASKS.remove(_tasknum);
      }
      notifyAll();              // notify in any case
    }
    return did;
  }

  // ---
  // Handle the remote-side incoming UDP packet.  This is called on the REMOTE
  // Node, not local.  Wrong thread, wrong JVM.
  public static class RemoteHandler extends UDP {
    AutoBuffer call(AutoBuffer ab) {
      return ab.getFlag() == CLIENT_UDP_SEND // UDP vs TCP send?
        ? remexec(ab.get(DTask.class), ab._h2o, ab.getTask(), ab)
        : ab; // Else all the work is being done in the TCP thread.
    }

    // Pretty-print bytes 1-15; byte 0 is the udp_type enum
    public String print16( AutoBuffer ab ) {
      int flag = ab.getFlag();
      String clazz = "";
      if( flag == CLIENT_UDP_SEND ) {
        int tid = ab.get2();
        clazz = TypeMap.MAP.getType(tid).toString();
      }
      String fs = "";
      switch( flag ) {
      case SERVER_UDP_SEND: fs = "SERVER_UDP_SEND"; break;
      case SERVER_TCP_SEND: fs = "SERVER_TCP_SEND"; break;
      case CLIENT_UDP_SEND: fs = "CLIENT_UDP_SEND"; break;
      case CLIENT_TCP_SEND: fs = "CLIENT_TCP_SEND"; break;
      }
      return "task# "+ab.getTask()+" "+fs+" "+ clazz;
    }
  }

  // Do the remote execution in a F/J thread & send a reply packet.
  // Caller must call 'tryComplete'.
  static private AutoBuffer remexec( DTask dt, H2ONode client, int task, AutoBuffer abold) {
    abold.close();              // Closing the old guy, returning a new guy
    // Now compute on it!
    dt.invoke(client);
    // Send results back
    AutoBuffer ab = new AutoBuffer(client).putTask(UDP.udp.ack,task).put1(SERVER_UDP_SEND);
    dt.write(ab);             // Write the DTask
    dt._repliedTcp = ab.hasTCP(); // Resends do not need to repeat TCP result
    // Install answer so retries get this very answer
    client.record_task_answer(task,dt);
    return ab;
  }

  // Handle TCP traffic, from a client to this server asking for work to be
  // done.  This is called on the TCP reader thread, not a Fork/Join worker
  // thread.  We want to do the bulk TCP read in the TCP reader thread.
  static void tcp_exec( final AutoBuffer ab ) {
    final int ctrl = ab.getCtrl();
    final int task = ab.getTask();
    final int flag = ab.getFlag();
    assert flag==CLIENT_UDP_SEND; // Client sent a request to be executed?
    // Act "as if" called from the UDP packet code, by recording the task just
    // like the packet we will be receiving (eventually).  The presence of this
    // packet is used to stop dup-actions on dup-sends.  Racily inserted, keep
    // only the last one.
    DTask dt1 = ab._h2o.record_task(task);
    assert dt1==null||dt1 instanceof NOPTask : "#"+task+" "+dt1.getClass(); // For TCP, no repeats, so 1st send is only send (except for UDP timeout retries)

    // Make a remote instance of this dude from the stream, but only if the
    // racing UDP packet did not already make one.  Start the bulk TCP read.
    final DTask dt = ab.get(DTask.class);

    // Here I want to execute on this, but not block for completion in the
    // TCP reader thread.  Jam the task on some F/J thread.
    UDP.udp.UDPS[ctrl].pool().execute(new CountedCompleter() {
        public void compute() { remexec(dt, ab._h2o, task, ab).close(); tryComplete(); }
        public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) { ex.printStackTrace(); return true; }
      });
    // All done for the TCP thread!  Work continues in the FJ thread...
  }

  // TCP large RECEIVE of results.  Note that 'this' is NOT the RPC object
  // that is hoping to get the received object, nor is the current thread the
  // RPC thread blocking for the object.  The current thread is the TCP
  // reader thread.
  static void tcp_ack( final AutoBuffer ab ) {
    // Get the RPC we're waiting on
    int task = ab.getTask();
    RPC rpc = TASKS.get(task);
    // Race with canceling a large RPC fetch: Task is already dead.  Do not
    // bother reading from the TCP socket, just bail out & close socket.
    if( rpc == null ) {
      ab.drainClose();
    } else {
      assert rpc._tasknum == task;
      assert !rpc._done;
      // Here we have the result, and we're on the correct Node but wrong
      // Thread.  If we just return, the TCP reader thread will close the
      // remote, the remote will UDP ACK the RPC back, and back on the current
      // Node but in the correct Thread, we'd wake up and realize we received a
      // large result.
      rpc.response(ab);
    }
    // ACKACK the remote, telling him "we got the answer"
    new AutoBuffer(ab._h2o).putTask(UDP.udp.ackack.ordinal(),task).close(true);
  }

  // Got a response UDP packet, or completed a large TCP answer-receive.
  // Install it as The Answer packet and wake up anybody waiting on an answer.
  protected void response( AutoBuffer ab ) {
    assert _tasknum==ab.getTask();
    if( _done ) { ab.close(); return; } // Ignore duplicate response packet
    int flag = ab.getFlag();    // Must read flag also, to advance ab
    if( flag == SERVER_TCP_SEND ) { ab.close(); return; } // Ignore UDP packet for a TCP reply
    assert flag == SERVER_UDP_SEND;
    synchronized(this) {        // Install the answer under lock
      if( _done ) { ab.close(); return; } // Ignore duplicate response packet
      _dt.read(ab);             // Read the answer (under lock?)
      ab.close();               // Also finish the read (under lock?)
      _dt.onAck();              // One time only execute (before sending ACKACK)
      _done = true;
      UDPTimeOutThread.PENDING.remove(this);
      TASKS.remove(_tasknum);   // Flag as task-completed, even if the result is null
      notifyAll();              // And notify in any case
    }
  }

  // ---
  static final long RETRY_MS = 200; // Initial UDP packet retry in msec
  // How long until we should do the "timeout" action?
  public long getDelay( TimeUnit unit ) {
    long delay = (_started+_retry)-System.currentTimeMillis();
    return unit.convert( delay, TimeUnit.MILLISECONDS );
  }
  // Needed for the DelayQueue API
  public final int compareTo( Delayed t ) {
    RPC<?> dt = (RPC<?>)t;
    long nextTime = _started+_retry, dtNextTime = dt._started+dt._retry;
    return nextTime == dtNextTime ? 0 : (nextTime > dtNextTime ? 1 : -1);
  }
}
