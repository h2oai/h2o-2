package water;

import water.RPC.RPCCall;
import water.api.DocGen;
import water.api.Request.API;
import water.api.TaskStatus.GetTaskInfo;
import water.nbhm.NonBlockingHashMap;
import water.nbhm.NonBlockingHashMapLong;
import water.util.Log;
import water.util.UnsafeUtils;

import java.io.IOException;
import java.net.*;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A <code>Node</code> in an <code>H2O</code> Cloud.
 * Basically a worker-bee with CPUs, Memory and Disk.
 * One of this is the self-Node, but the rest are remote Nodes.
 *
 * @author <a href="mailto:cliffc@h2o.ai"></a>
 * @version 1.0
 */

public class H2ONode extends Iced implements Comparable {
  public int _unique_idx; // Dense integer index, skipping 0.  NOT cloud-wide unique.
  public long _last_heard_from; // Time in msec since we last heard from this Node
  public boolean _announcedLostContact;  // True if heartbeat published a no-contact msg
  public volatile HeartBeat _heartbeat;  // My health info.  Changes 1/sec.
  public int _tcp_readers;               // Count of started TCP reader threads
  public boolean _node_healthy;

  // A JVM is uniquely named by machine IP address and port#
  public H2Okey _key;
  public static final class H2Okey extends InetSocketAddress implements Comparable {
    final int _ipv4;     // cheapo ipv4 address
    public H2Okey(InetAddress inet, int port) {
      super(inet,port);
      byte[] b = inet.getAddress();
      _ipv4 = ((b[0]&0xFF)<<0)+((b[1]&0xFF)<<8)+((b[2]&0xFF)<<16)+((b[3]&0xFF)<<24);
    }
    public int htm_port() { return getPort()-1; }
    public int udp_port() { return getPort()  ; }
    @Override public String toString() { return getAddress()+":"+htm_port(); }
    AutoBuffer write( AutoBuffer ab ) {
      return ab.put4(_ipv4).put2((char)udp_port());
    }
    static H2Okey read( AutoBuffer ab ) {
      InetAddress inet;
      try { inet = InetAddress.getByAddress(ab.getA1(4)); }
      catch( UnknownHostException e ) { throw  Log.errRTExcept(e); }
      int port = ab.get2();
      return new H2Okey(inet,port);
    }
    // Canonical ordering based on inet & port
    @Override public int compareTo( Object x ) {
      if( x == null ) return -1;   // Always before null
      if( x == this ) return 0;
      H2Okey key = (H2Okey)x;
      // Must be unsigned long-math, or overflow will make a broken sort
      long res = (_ipv4&0xFFFFFFFFL) - (key._ipv4&0xFFFFFFFFL);
      if( res != 0 ) return res < 0 ? -1 : 1;
      return udp_port() - key.udp_port();
    }
  }

  public final int ip4() { return _key._ipv4; }

  // These are INTERN'd upon construction, and are uniquely numbered within the
  // same run of a JVM.  If a remote Node goes down, then back up... it will
  // come back with the SAME IP address, and the same unique_idx and history
  // relative to *this* Node.  They can be compared with pointer-equality.  The
  // unique idx is used to know which remote Nodes have cached which Keys, even
  // if the Home#/Replica# change for a Key due to an unrelated change in Cloud
  // membership.  The unique_idx is *per Node*; not all Nodes agree on the same
  // indexes.
  private H2ONode( H2Okey key, int unique_idx ) {
    _key = key;
    _unique_idx = unique_idx;
    _last_heard_from = System.currentTimeMillis();
    _heartbeat = new HeartBeat();
    _node_healthy = true;
  }

  // ---------------
  // A dense integer index for every unique IP ever seen, since the JVM booted.
  // Used to track "known replicas" per-key across Cloud change-ups.  Just use
  // an array-of-H2ONodes, and a limit of 255 unique H2ONodes
  static private final NonBlockingHashMap<H2Okey,H2ONode> INTERN = new NonBlockingHashMap<H2Okey,H2ONode>();
  static private final AtomicInteger UNIQUE = new AtomicInteger(1);
  static public H2ONode IDX[] = new H2ONode[1];

  // Create and/or re-use an H2ONode.  Each gets a unique dense index, and is
  // *interned*: there is only one per InetAddress.
  public static final H2ONode intern( H2Okey key ) {
    H2ONode h2o = INTERN.get(key);
    if( h2o != null ) return h2o;
    final int idx = UNIQUE.getAndIncrement();
    h2o = new H2ONode(key,idx);
    H2ONode old = INTERN.putIfAbsent(key,h2o);
    if( old != null ) return old;
    synchronized(H2O.class) {
      while( idx >= IDX.length )
        IDX = Arrays.copyOf(IDX,IDX.length<<1);
      IDX[idx] = h2o;
    }
    return h2o;
  }
  public static final H2ONode intern( InetAddress ip, int port ) { return intern(new H2Okey(ip,port)); }

  public static H2ONode intern( byte[] bs, int off ) {
    byte[] b = new byte[4];
    UnsafeUtils.set4(b, 0, UnsafeUtils.get4(bs, off));
    int port = UnsafeUtils.get2(bs,off+4)&0xFFFF;
    try { return intern(InetAddress.getByAddress(b),port); } 
    catch( UnknownHostException e ) { throw Log.errRTExcept(e); }
  }

  public static final H2ONode intern( int ip, int port ) {
    byte[] b = new byte[4];
    b[0] = (byte)(ip>> 0);
    b[1] = (byte)(ip>> 8);
    b[2] = (byte)(ip>>16);
    b[3] = (byte)(ip>>24);
    try {
      return intern(InetAddress.getByAddress(b),port);
    } catch( UnknownHostException e ) {
      Log.err(e);
      return null;
    }
  }

  // Read & return interned from wire
  @Override public AutoBuffer write( AutoBuffer ab ) { return _key.write(ab); }
  @Override public H2ONode read( AutoBuffer ab ) { return intern(H2Okey.read(ab));  }
  public H2ONode( ) { }

  // Get a nice Node Name for this Node in the Cloud.  Basically it's the
  // InetAddress we use to communicate to this Node.
  static H2ONode self(InetAddress local) {
    assert H2O.H2O_PORT != 0;
    try {
      // Figure out which interface matches our IP address
      List<NetworkInterface> matchingIfs = new ArrayList();
      Enumeration<NetworkInterface> netIfs = NetworkInterface.getNetworkInterfaces();
      while( netIfs.hasMoreElements() ) {
        NetworkInterface netIf = netIfs.nextElement();
        Enumeration<InetAddress> addrs = netIf.getInetAddresses();
        while( addrs.hasMoreElements() ) {
          InetAddress addr = addrs.nextElement();
          if( addr.equals(local) ) {
            matchingIfs.add(netIf);
            break;
          }
        }
      }
      switch( matchingIfs.size() ) {
      case 0: H2O.CLOUD_MULTICAST_IF = null; break;
      case 1: H2O.CLOUD_MULTICAST_IF = matchingIfs.get(0); break;
      default:
        String msg = "Found multiple network interfaces for ip address " + local;
        for( NetworkInterface ni : matchingIfs ) {
          msg +="\n\t" + ni;
        }
        msg +="\nUsing " + matchingIfs.get(0) + " for UDP broadcast";
        Log.warn(msg);
        H2O.CLOUD_MULTICAST_IF = matchingIfs.get(0);
      }
    } catch( SocketException e ) {
      throw  Log.errRTExcept(e);
    }
    try {
      assert H2O.CLOUD_DGRAM == null;
      H2O.CLOUD_DGRAM = DatagramChannel.open();
    } catch( Exception e ) {
      throw  Log.errRTExcept(e);
    }
    return intern(new H2Okey(local,H2O.H2O_PORT));
  }

  // Happy printable string
  @Override public String toString() { return _key.toString(); }
  @Override public int hashCode() { return _key.hashCode(); }
  @Override public boolean equals(Object o) { return _key.equals   (((H2ONode)o)._key); }
  @Override public int compareTo( Object o) { return _key.compareTo(((H2ONode)o)._key); }

  // index of this node in the current cloud... can change at the next cloud.
  public int index() { return H2O.CLOUD.nidx(this); }

  // max memory for this node.
  // no need to ask the (possibly not yet populated) heartbeat if we want to know the local max memory.
  public long get_max_mem() { return this == H2O.SELF ? Runtime.getRuntime().maxMemory() : _heartbeat.get_max_mem(); }

  // ---------------
  // A queue of available TCP sockets
  // Public re-usable TCP socket opened to this node, or null.
  // This is essentially a BlockingQueue/Stack that allows null.
  private SocketChannel _socks[] = new SocketChannel[2];
  private int _socksAvail=_socks.length;
  // Count of concurrent TCP requests both incoming and outgoing
  public static final AtomicInteger TCPS = new AtomicInteger(0);
  public SocketChannel getTCPSocket() throws IOException {
    // Under lock, claim an existing open socket if possible
    synchronized(this) {
      // Limit myself to the number of open sockets from node-to-node
      while( _socksAvail == 0 )
        try { wait(); } catch( InterruptedException ie ) { }
      // Claim an open socket
      SocketChannel sock = _socks[--_socksAvail];
      if( sock != null ) {
        if( sock.isOpen() ) return sock; // Return existing socket!
        // Else its an already-closed socket, lower open TCP count
        assert TCPS.get() > 0;
        TCPS.decrementAndGet();
      }
    }
    // Must make a fresh socket
    SocketChannel sock2 = SocketChannel.open();
    sock2.socket().setReuseAddress(true);
    sock2.socket().setSendBufferSize(AutoBuffer.BBSIZE);
    boolean res = sock2.connect( _key );
    assert res && !sock2.isConnectionPending() && sock2.isBlocking() && sock2.isConnected() && sock2.isOpen();
    TCPS.incrementAndGet();     // Cluster-wide counting
    return sock2;
  }
  public synchronized void freeTCPSocket( SocketChannel sock ) {
    assert 0 <= _socksAvail && _socksAvail < _socks.length;
    if( sock != null && !sock.isOpen() ) sock = null;
    _socks[_socksAvail++] = sock;
    assert TCPS.get() > 0;
    if( sock == null ) TCPS.decrementAndGet();
    notify();
  }

  // ---------------
  // The *outgoing* client-side calls; pending tasks this Node wants answered.
  private final NonBlockingHashMapLong<RPC> _tasks = new NonBlockingHashMapLong();
  public void taskPut(int tnum, RPC rpc ) { _tasks.put(tnum,rpc); }
  public RPC taskGet(int tnum) { return _tasks.get(tnum); }
  public void taskRemove(int tnum) { _tasks.remove(tnum); }
  public Collection<RPC> tasks() { return _tasks.values(); }
  public int taskSize() { return _tasks.size(); }

  // The next unique task# sent *TO* the 'this' Node.
  private final AtomicInteger _created_task_ids = new AtomicInteger(1);
  public int nextTaskNum() { return _created_task_ids.getAndIncrement(); }


  // ---------------
  // The Work-In-Progress list.  Each item is a UDP packet's worth of work.
  // When the RPCCall to _computed, then it's Completed work instead
  // work-in-progress.  Completed work can be short-circuit replied-to by
  // resending the RPC._dt back.  Work that we're sure the this Node has seen
  // the reply to can be removed - but we must remember task-completion for all
  // time (because UDP packets can be dup'd and arrive very very late and
  // should not be confused with new work).
  private final NonBlockingHashMapLong<RPC.RPCCall> _work = new NonBlockingHashMapLong();

  // We must track even dead/completed tasks for All Time (lest a very very
  // delayed UDP packet look like New Work).  The easy way to do this is leave
  // all work packets/RPCs in the _work HashMap for All Time - but this amounts
  // to a leak.  Instead we "roll up" the eldest completed work items, just
  // remembering their completion status.  Task id's older (smaller) than the
  // _removed_task_ids are both completed, and rolled-up to a single integer.
  private final AtomicInteger _removed_task_ids = new AtomicInteger(0);
  // A Golden Completed Task: it's a shared completed task used to represent
  // all instances of tasks that have been completed and are no longer being
  // tracked separately.
  private final RPC.RPCCall _removed_task = new RPC.RPCCall(null,this,0);

  RPC.RPCCall has_task( int tnum ) {
    if( tnum <= _removed_task_ids.get() ) return _removed_task;
    return _work.get(tnum);
  }

  // Record a task-in-progress, or return the prior RPC if one already exists.
  // The RPC will flip to "_completed" once the work is done.  The RPC._dtask
  // can be repeatedly ACKd back to the caller, and the _dtask is removed once
  // an ACKACK appears - and the RPC itself is removed once all prior RPCs are
  // also ACKACK'd.
  RPC.RPCCall record_task( RPC.RPCCall rpc ) {
    // Task removal (and roll-up) suffers from classic race-condition, which we
    // fix by a classic Dekker's algo; a task# is always in either the _work
    // HashMap, or rolled-up in the _removed_task_ids counter, or both (for
    // short intervals during the handoff).  We can never has a cycle where
    // it's in neither or else a late UDP may attempt to "resurrect" the
    // already completed task.  Hence we must always check the "removed ids"
    // AFTER we insert in the HashMap (we can check before also, but that's a
    // simple optimization and not sufficient for correctness).
    final RPC.RPCCall x = _work.putIfAbsent(rpc._tsknum,rpc);
    if( x != null ) return x;   // Return pre-existing work
    // If this RPC task# is very old, we just return a Golden Completed task.
    // The task is not just completed, but also we have already received
    // verification that the client got the answer.  So this is just a really
    // old attempt to restart a long-completed task.
    if( rpc._tsknum > _removed_task_ids.get() ) return null; // Task is new
    _work.remove(rpc._tsknum); // Bogus insert, need to remove it
    return _removed_task;      // And return a generic Golden Completed object
  }
  // Record the final return value for a DTask.  Should happen only once.
  // Recorded here, so if the client misses our ACK response we can resend the
  // same answer back.
  void record_task_answer( RPC.RPCCall rpcall ) {
    assert rpcall._started == 0 || rpcall._dt.hasException();
    rpcall._started = System.currentTimeMillis();
    rpcall._retry = RPC.RETRY_MS; // Start the timer on when to resend
    AckAckTimeOutThread.PENDING.add(rpcall);
  }
  // Stop tracking a remote task, because we got an ACKACK.
  void remove_task_tracking( int task ) {
    RPC.RPCCall rpc = _work.get(task);
    if( rpc == null ) return;   // Already stopped tracking

    // Atomically attempt to remove the 'dt'.  If we win, we are the sole
    // thread running the dt.onAckAck.  Also helps GC: the 'dt' is done (sent
    // to client and we received the ACKACK), but the rpc might need to stick
    // around a long time - and the dt might be big.
    DTask dt = rpc._dt;         // The existing DTask, if any
    if( dt != null && RPC.RPCCall.CAS_DT.compareAndSet(rpc,dt,null) ) {
      assert rpc._computed : "Still not done #"+task+" "+dt.getClass()+" from "+rpc._client;
      AckAckTimeOutThread.PENDING.remove(rpc);
      dt.onAckAck();            // One-time call on stop-tracking
    }

    // Roll-up as many done RPCs as we can, into the _removed_task_ids list
    while( true ) {
      int t = _removed_task_ids.get();   // Last already-removed ID
      RPC.RPCCall rpc2 = _work.get(t+1); // RPC of 1st not-removed ID
      if( rpc2 == null || rpc2._dt != null || !_removed_task_ids.compareAndSet(t,t+1) )
        break;                  // Stop when we hit in-progress tasks
      _work.remove(t+1);        // Else we can remove the tracking now
    }
  }

  // Resend ACK's, in case the UDP ACKACK got dropped.  Note that even if the
  // ACK was sent via TCP, the ACKACK might be dropped.  Further: even if we
  // *know* the client got our TCP response, we do not know *when* he'll
  // process it... so we cannot e.g. eagerly do an ACKACK on this side.  We
  // must wait for the real ACKACK - which can drop.  So we *must* resend ACK's
  // occasionally to force a resend of ACKACKs.

  static public class AckAckTimeOutThread extends Thread {
    public AckAckTimeOutThread() { super("ACKTimeout"); }
    // List of DTasks with results ready (and sent!), and awaiting an ACKACK.
    static DelayQueue<RPC.RPCCall> PENDING = new DelayQueue<RPC.RPCCall>();
    // Started by main() on a single thread, handle timing-out UDP packets
    @Override public void run() {
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
      while( true ) {
        RPC.RPCCall r;
        try { r = PENDING.take(); }
        // Interrupted while waiting for a packet?
        // Blow it off and go wait again...
        catch( InterruptedException e ) { continue; }
        assert r._computed : "Found RPCCall not computed "+r._tsknum;
        r._ackResendCnt++;
        if(r._ackResendCnt % 50 == 0)
          Log.err("Possibly broken network, can not send ack through, got " + r._ackResendCnt + " resends.");
        if( !H2O.CLOUD.contains(r._client) ) { // RPC from somebody who dropped out of cloud?
          r._client.remove_task_tracking(r._tsknum);
          continue;
        }
        if( r._dt != null ) {   // Not yet run the ACKACK?
          r.resend_ack();       // Resend ACK, hoping for ACKACK
          PENDING.add(r);       // And queue up to send again
        }
      }
    }
  }

  // This Node rebooted recently; we can quit tracking prior work history
  void rebooted() {
    _work.clear();
  }

  /** Returns run time for this node based on last heartbeat. */
  public long runtime() {
    return _heartbeat!=null ? _heartbeat._jvm_boot_msec==0 ? 0 : System.currentTimeMillis()-_heartbeat._jvm_boot_msec : -1;
  }

  public enum task_status {INIT, CMP, DONE, RTCP,RUDP}

  public static class TaskInfo extends Iced {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    @API(help="Task name")
    public final String task;
    @API(help="Task Id, unique id per pair of nodes")
    public final long taskId;
    @API(help="")
    public final int  nodeId;
    @API(help="")
    public final int  retriesCnt;

    @API(help="")
    public final task_status taskStatus;

    public TaskInfo(DTask task,long tid, int nid, task_status ts, int retriesCnt){
      this.task = task == null?"null":task.toString();
      taskId = tid;
      nodeId = nid;
      taskStatus = ts;
      this.retriesCnt = retriesCnt;
    }

    @Override
    public String toString(){
      return task +"#" + taskId +" [" + taskStatus + ", " + retriesCnt+"]";
    }
  }
  public TaskInfo [] currentTasksInfo() {
    Set<Entry<Long,RPCCall>> s = _work.entrySet();
    TaskInfo [] res = new TaskInfo[s.size()];
    int i = 0;
    for(Entry<Long,RPCCall> e:s){
      RPCCall rpc = e.getValue();
      if(rpc._dt instanceof GetTaskInfo)
        continue;
      if(i < res.length) {
        DTask dt = rpc._dt;
        if(dt != null) // else we got ackack -> not interested!
          res[i++] = new TaskInfo(rpc._dt, e.getKey(), _unique_idx, rpc._computedAndReplied ? (dt._repliedTcp ? task_status.RTCP : task_status.RUDP) : rpc._computed ? task_status.DONE : rpc._cmpStarted > 0 ? task_status.CMP : task_status.INIT,(rpc._callCnt+rpc._ackResendCnt));
      }
    }
    return Arrays.copyOf(res,i);
  }

}
