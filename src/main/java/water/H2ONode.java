package water;

import java.net.*;
import java.nio.channels.DatagramChannel;
import java.util.*;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

import water.nbhm.NonBlockingHashMap;
import water.nbhm.NonBlockingHashMapLong;

/**
 * A <code>Node</code> in an <code>H2O</code> Cloud.
 * Basically a worker-bee with CPUs, Memory and Disk.
 * One of this is the self-Node, but the rest are remote Nodes.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class H2ONode extends Iced implements Comparable {

  // A JVM is uniquely named by machine IP address and port#
  public static final class H2Okey extends InetSocketAddress implements Comparable {
    final int _ipv4;     // cheapo ipv4 address
    final String name;
    public H2Okey(InetAddress inet, int port) {
      super(inet,port);
      byte[] b = inet.getAddress();
      _ipv4 = ((b[0]&0xFF)<<0)+((b[1]&0xFF)<<8)+((b[2]&0xFF)<<16)+((b[3]&0xFF)<<24);
      name = inet.toString();
    }
    public int htm_port() { return getPort()-1; }
    public int udp_port() { return getPort()  ; }
    public String toString() { return getAddress()+":"+htm_port(); }
    AutoBuffer write( AutoBuffer ab ) {
      return ab.put4(_ipv4).put2((char)udp_port());
    }
    static H2Okey read( AutoBuffer ab ) {
      InetAddress inet;
      try { inet = InetAddress.getByAddress(ab.getA1(4)); }
      catch( UnknownHostException e ) { throw new Error(e); }
      int port = ab.get2();
      return new H2Okey(inet,port);
    }
    // Canonical ordering based on inet & port
    @Override public int compareTo( Object x ) {
      if( x == null ) return -1;   // Always before null
      H2Okey key = ((H2ONode)x)._key;
      if( key == this ) return 0;
      // Note: long-math does not matter here, all we need is a reliable ordering.
//FIXME: Jan hack...
      //  int res = _ipv4 - key._ipv4;
//      if( res != 0 ) return res;
      // return udp_port() - key.udp_port();
      return name.compareTo(key.name);
    }
  }
  public H2Okey _key;

  public int _unique_idx; // Dense integer index, skipping 0.
  public long _last_heard_from; // Time in msec since we last heard from this Node

  public final int ip4() { return _key._ipv4; }

  public volatile HeartBeat _heartbeat;  // My health info.  Changes 1/sec.

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

  public static final H2ONode intern( int ip, int port ) {
    byte[] b = new byte[4];
    b[0] = (byte)(ip>> 0);
    b[1] = (byte)(ip>> 8);
    b[2] = (byte)(ip>>16);
    b[3] = (byte)(ip>>24);
    try {
      return intern(InetAddress.getByAddress(b),port);
    } catch( UnknownHostException uhe ) {
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
    assert H2O.UDP_PORT != 0;
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
        System.err.print("Found multiple network interfaces for ip address " + local);
        for( NetworkInterface ni : matchingIfs ) {
          System.err.println("\t" + ni);
        }
        System.err.println("Using " + matchingIfs.get(0) + " for UDP broadcast");
        H2O.CLOUD_MULTICAST_IF = matchingIfs.get(0);
      }
    } catch( SocketException e ) {
      throw new RuntimeException(e);
    }
    try {
      assert H2O.CLOUD_DGRAM == null;
      H2O.CLOUD_DGRAM = DatagramChannel.open();
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
    return intern(new H2Okey(local,H2O.UDP_PORT));
  }

  // Happy printable string
  @Override public String toString() { return _key.toString (); }
  @Override public int hashCode() { return _key.hashCode(); }
  @Override public boolean equals(Object o) { return _key.equals(o); }
  @Override public int compareTo( Object o) { return _key.compareTo(o); }

  // index of this node in the current cloud... can change at the next cloud.
  public int index() { return H2O.CLOUD.nidx(this); }

  // ---------------
  // The Work-In-Progress list.  Each item is a UDP packet's worth of work.
  // When the mapping changes from a NOPTask to a result DTask, then it's
  // Completed work instead work-in-progress.  Completed work can be
  // short-circuit replied-to by resending this DTask back.  Work that we're
  // sure the this Node has seen the reply to can be removed.
  private NonBlockingHashMapLong<RPC.RPCCall> WORK = new NonBlockingHashMapLong();

  RPC.RPCCall has_task( int tnum ) { return WORK.get(tnum); }

  // Record a task-in-progress, or return the prior DTask if one already
  // exists.  Initial mappings are always to "NOPTask", a placeholder.  Once
  // the task is complete, the mapping is changed to a completed DTask.  The
  // DTask can be repeatedly ACKd back to the caller, and is removed once an
  // ACKACK appears.
  RPC.RPCCall record_task( RPC.RPCCall rpc ) {
    return WORK.putIfAbsent(rpc._tsknum,rpc);
  }
  // Record the final return value for a DTask.  Should happen only once.
  // Recorded here, so if the client misses our ACK response we can resend the
  // same answer back.
  void record_task_answer( RPC.RPCCall rpcall ) {
    rpcall._started = System.currentTimeMillis();
    rpcall._retry = RPC.RETRY_MS; // Start the timer on when to resend
    AckAckTimeOutThread.PENDING.add(rpcall);
  }
  // Stop tracking a remote task, because we got an ACKACK.
  void remove_task_tracking( int task ) {
    RPC.RPCCall old;
    synchronized( this ) {
      old = WORK.remove(task);
      if( old == null ) return; // Already stopped tracking
      AckAckTimeOutThread.PENDING.remove(old);
    }
    assert old._computed : "Still not done #"+task+" "+old.getClass()+" from "+old._client;
    old._dt.onAckAck();         // One-time call stop-tracking
  }

  // Resend ACK's, in case the UDP ACKACK got dropped.  Note that even if the
  // ACK was sent via TCP, the ACKACK might be dropped.  Further: even if we
  // *know* the client got our TCP response, we do not know *when* he'll
  // process it... so we cannot e.g. eagerly do an ACKACK on this side.  We
  // must wait for the real ACKACK - which can drop.  So we *must* resend ACK's
  // occasionally to force a resend of ACKACKs.

  static public class AckAckTimeOutThread extends Thread {
    public AckAckTimeOutThread() { super("ACKACK Timeout"); }
    // List of DTasks with results ready (and sent!), and awaiting an ACKACK.
    static DelayQueue<RPC.RPCCall> PENDING = new DelayQueue<RPC.RPCCall>();
    // Started by main() on a single thread, handle timing-out UDP packets
    public void run() {
      Thread.currentThread().setPriority(Thread.NORM_PRIORITY);
      while( true ) {
        try {
          RPC.RPCCall r = PENDING.take();
          assert r._computed : "Found RPCCall not computed "+r._tsknum;
          if( H2O.CLOUD.contains(r._client) ) {
            synchronized( r._client ) {
              if( r._client.has_task(r._tsknum) == r ) {
                r.resend_ack();
                PENDING.add(r);
              }
            }
          } else
            r._client.remove_task_tracking(r._tsknum);
        } catch( InterruptedException e ) {
          // Interrupted while waiting for a packet?
          // Blow it off and go wait again...
        }
      }
    }
  }

  // This Node rebooted recently; we can quit tracking prior work history
  void rebooted() {
    WORK.clear();
    AckAckTimeOutThread.PENDING.clear();
  }
}
