package water;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.*;

import jsr166y.*;
import water.Job.JobCancelledException;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.ga.EventHit;
import water.ga.GoogleAnalytics;
import water.nbhm.NonBlockingHashMap;
import water.persist.*;
import water.util.*;
import water.util.Log.Tag.Sys;
import water.license.LicenseManager;
import java.nio.channels.ServerSocketChannel;

/**
* Start point for creating or joining an <code>H2O</code> Cloud.
*
* @author <a href="mailto:cliffc@h2o.ai"></a>
* @version 1.0
*/
public final class H2O {
  public static volatile AbstractEmbeddedH2OConfig embeddedH2OConfig;
  public static volatile ApiIpPortWatchdogThread apiIpPortWatchdog;
  public static volatile LicenseManager licenseManager;

  public static String VERSION = "(unknown)";
  public static long START_TIME_MILLIS = -1;

  // User name for this Cloud (either the username or the argument for the option -name)
  public static String NAME;

  // The default port for finding a Cloud
  public static int DEFAULT_PORT = 54321;
  public static int H2O_PORT; // Fast/small UDP transfers
  public static int API_PORT; // RequestServer and the new API HTTP port

  // Whether to toggle to single precision as upper limit for storing floating point numbers
  public static boolean SINGLE_PRECISION = false;

  // Max. number of factor levels ber column (before flipping all to NAs)
  public static int DATA_MAX_FACTOR_LEVELS = 1000000;

  public static int LOG_CHK = 22; // Chunks are 1<<22, or 4Meg

  // The multicast discovery port
  static MulticastSocket  CLOUD_MULTICAST_SOCKET;
  static NetworkInterface CLOUD_MULTICAST_IF;
  static InetAddress      CLOUD_MULTICAST_GROUP;
  static int              CLOUD_MULTICAST_PORT ;
  // Default NIO Datagram channel
  static DatagramChannel  CLOUD_DGRAM;

  // Myself, as a Node in the Cloud
  public static H2ONode SELF = null;
  public static InetAddress SELF_ADDRESS;

  public static String DEFAULT_ICE_ROOT() {
    String username = System.getProperty("user.name");
    if (username == null) username = "";
    String u2 = username.replaceAll(" ", "_");
    if (u2.length() == 0) u2 = "unknown";
    return "/tmp/h2o-" + u2;
  }

  public static URI ICE_ROOT;

  // Initial arguments
  public static String[] ARGS;

  public static final PrintStream OUT = System.out;
  public static final PrintStream ERR = System.err;
  public static final int NUMCPUS = Runtime.getRuntime().availableProcessors();

  // Convenience error
  public static RuntimeException unimpl(String msg) { return new RuntimeException("unimplemented: " + msg); }
  public static RuntimeException unimpl() { return new RuntimeException("unimplemented"); }
  public static RuntimeException fail() { return new RuntimeException("do not call"); }
  public static RuntimeException fail(String msg) { return new RuntimeException("FAILURE: " + msg); }

  // Central /dev/null for ignored exceptions
  public static void ignore(Throwable e)             { ignore(e,"[h2o] Problem ignored: "); }
  public static void ignore(Throwable e, String msg) { ignore(e, msg, true); }
  public static void ignore(Throwable e, String msg, boolean printException) { Log.debug(Sys.WATER, msg + (printException? e.toString() : "")); }

  //Google analytics performance measurement
  public static GoogleAnalytics GA;
  public static int CLIENT_TYPE_GA_CUST_DIM = 1;

  // --------------------------------------------------------------------------
  // Embedded configuration for a full H2O node to be implanted in another
  // piece of software (e.g. Hadoop mapper task).
  /**
   * Register embedded H2O configuration object with H2O instance.
   */
  public static void setEmbeddedH2OConfig(AbstractEmbeddedH2OConfig c) { embeddedH2OConfig = c; }
  public static AbstractEmbeddedH2OConfig getEmbeddedH2OConfig() { return embeddedH2OConfig; }

  /**
   * Tell the embedding software that this H2O instance belongs to
   * a cloud of a certain size.
   * This may be nonblocking.
   *
   * @param ip IP address this H2O can be reached at.
   * @param port Port this H2O can be reached at (for REST API and browser).
   * @param size Number of H2O instances in the cloud.
   */
  public static void notifyAboutCloudSize(InetAddress ip, int port, int size) {
    if (embeddedH2OConfig == null) { return; }
    embeddedH2OConfig.notifyAboutCloudSize(ip, port, size);
  }

  /**
   * Notify embedding software instance H2O wants to exit.
   * @param status H2O's requested process exit value.
   */
  public static void exit(int status) {
    // embeddedH2OConfig is only valid if this H2O node is living inside
    // another software instance (e.g. a Hadoop mapper task).
    //
    // Expect embeddedH2OConfig to be null if H2O is run standalone.

    // Cleanly shutdown internal H2O services.
    if (apiIpPortWatchdog != null) {
      apiIpPortWatchdog.shutdown();
    }

    if (embeddedH2OConfig == null) {
      // Standalone H2O path.
      System.exit (status);
    }

    // Embedded H2O path (e.g. inside Hadoop mapper task).
    embeddedH2OConfig.exit(status);

    // Should never reach here.
    System.exit(222);
  }

  /** Shutdown itself by sending a shutdown UDP packet. */
  public void shutdown() {
    UDPRebooted.T.shutdown.send(H2O.SELF);
    H2O.exit(0);
  }

  // --------------------------------------------------------------------------
  // The Current Cloud. A list of all the Nodes in the Cloud. Changes if we
  // decide to change Clouds via atomic Cloud update.
  static public volatile H2O CLOUD = new H2O(new H2ONode[0],0,0);

  // ---
  // A dense array indexing all Cloud members. Fast reversal from "member#" to
  // Node.  No holes.  Cloud size is _members.length.
  public final H2ONode[] _memary;
  public final int _hash;
  //public boolean _healthy;

  // A dense integer identifier that rolls over rarely. Rollover limits the
  // number of simultaneous nested Clouds we are operating on in-parallel.
  // Really capped to 1 byte, under the assumption we won't have 256 nested
  // Clouds. Capped at 1 byte so it can be part of an atomically-assigned
  // 'long' holding info specific to this Cloud.
  public final char _idx; // no unsigned byte, so unsigned char instead

  // Is nnn larger than old (counting for wrap around)? Gets confused if we
  // start seeing a mix of more than 128 unique clouds at the same time. Used
  // to tell the order of Clouds appearing.
  static public boolean larger( int nnn, int old ) {
    assert (0 <= nnn && nnn <= 255);
    assert (0 <= old && old <= 255);
    return ((nnn-old)&0xFF) < 64;
  }

  static public boolean isHealthy() {
      H2O cloud = H2O.CLOUD;
      for (H2ONode h2o : cloud._memary) {
          if(!h2o._node_healthy) return false;
      }
      return true;
  }

  // Static list of acceptable Cloud members
  public static HashSet<H2ONode> STATIC_H2OS = null;

  // Reverse cloud index to a cloud; limit of 256 old clouds.
  static private final H2O[] CLOUDS = new H2O[256];

  // Enables debug features like more logging and multiple instances per JVM
  public static final String DEBUG_ARG = "h2o.debug";
  public static final boolean DEBUG = System.getProperty(DEBUG_ARG) != null;

  // Construct a new H2O Cloud from the member list
  public H2O( H2ONode[] h2os, int hash, int idx ) {
    _memary = h2os;             // Need to clone?
    Arrays.sort(_memary);       // ... sorted!
    _hash = hash;               // And record hash for cloud rollover
    _idx = (char)(idx&0x0ff);   // Roll-over at 256
  }

  // One-shot atomic setting of the next Cloud, with an empty K/V store.
  // Called single-threaded from Paxos. Constructs the new H2O Cloud from a
  // member list.
  void set_next_Cloud( H2ONode[] h2os, int hash ) {
    synchronized(this) {
      int idx = _idx+1; // Unique 1-byte Cloud index
      if( idx == 256 ) idx=1; // wrap, avoiding zero
      CLOUDS[idx] = CLOUD = new H2O(h2os,hash,idx);
    }
    SELF._heartbeat._cloud_size=(char)CLOUD.size();
  }

  public final int size() { return _memary.length; }
  public final H2ONode leader() { return _memary[0]; }

  public static void waitForCloudSize(int x) {
    waitForCloudSize(x, 10000);
  }

  public static void waitForCloudSize(int x, long ms) {
    long start = System.currentTimeMillis();
    while( System.currentTimeMillis() - start < ms ) {
      if( CLOUD.size() >= x && Paxos._commonKnowledge )
        break;
      try { Thread.sleep(100); } catch( InterruptedException ie ) { }
    }
    if( H2O.CLOUD.size() < x )
      throw new RuntimeException("Cloud size under " + x);
  }

  // Find the node index for this H2ONode, or a negative number on a miss
  public int nidx( H2ONode h2o ) { return Arrays.binarySearch(_memary,h2o); }
  public boolean contains( H2ONode h2o ) { return nidx(h2o) >= 0; }
  // BIG WARNING: do you not change this toString() method since cloud hash value depends on it
  @Override public String toString() {
    return Arrays.toString(_memary);
  }
  public String toPrettyString() {
    if (_memary==null || _memary.length==0) return "[]";
    int iMax = _memary.length - 1;
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int i = 0; ; i++) {
      sb.append(String.valueOf(_memary[i]));
      if (_memary[i]!=null) sb.append(" (").append(PrettyPrint.msecs(_memary[i].runtime(),false)).append(')');
      if (i==iMax) return sb.append(']').toString();
      sb.append(", ");
    }
  }

  /**
   * Return a list of interfaces sorted by importance (most important first).
   * This is the order we want to test for matches when selecting an interface.
   */
  private static ArrayList<NetworkInterface> calcPrioritizedInterfaceList() {
    ArrayList<NetworkInterface> networkInterfaceList = null;
    try {
      Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
      ArrayList<NetworkInterface> tmpList = Collections.list(nis);

      Comparator<NetworkInterface> c = new Comparator<NetworkInterface>() {
        @Override public int compare(NetworkInterface lhs, NetworkInterface rhs) {
          // Handle null inputs.
          if ((lhs == null) && (rhs == null)) { return 0; }
          if (lhs == null) { return 1; }
          if (rhs == null) { return -1; }

          // If the names are equal, then they are equal.
          if (lhs.getName().equals (rhs.getName())) { return 0; }

          // If both are bond drivers, choose a precedence.
          if (lhs.getName().startsWith("bond") && (rhs.getName().startsWith("bond"))) {
            Integer li = lhs.getName().length();
            Integer ri = rhs.getName().length();

            // Bond with most number of characters is always highest priority.
            if (li.compareTo(ri) != 0) {
              return li.compareTo(ri);
            }

            // Otherwise, sort lexicographically by name.
            return lhs.getName().compareTo(rhs.getName());
          }

          // If only one is a bond driver, give that precedence.
          if (lhs.getName().startsWith("bond")) { return -1; }
          if (rhs.getName().startsWith("bond")) { return 1; }

          // Everything that isn't a bond driver is equal.
          return 0;
        }
      };

      Collections.sort(tmpList, c);
      networkInterfaceList = tmpList;
    } catch( SocketException e ) { Log.err(e); }

    return networkInterfaceList;
  }

  /**
   * Return a list of internet addresses sorted by importance (most important first).
   * This is the order we want to test for matches when selecting an internet address.
   */
  public static ArrayList<java.net.InetAddress> calcPrioritizedInetAddressList() {
    ArrayList<java.net.InetAddress> ips = new ArrayList<java.net.InetAddress>();
    {
      ArrayList<NetworkInterface> networkInterfaceList = calcPrioritizedInterfaceList();

      for (int i = 0; i < networkInterfaceList.size(); i++) {
        NetworkInterface ni = networkInterfaceList.get(i);
        Enumeration<InetAddress> ias = ni.getInetAddresses();
        while( ias.hasMoreElements() ) {
          InetAddress ia;
          ia = ias.nextElement();
          ips.add(ia);
          Log.info("Possible IP Address: " + ni.getName() + " (" + ni.getDisplayName() + "), " + ia.getHostAddress());
        }
      }
    }

    return ips;
  }

  public static InetAddress findInetAddressForSelf() throws Error {
    if(SELF_ADDRESS == null) {
      if ((OPT_ARGS.ip != null) && (OPT_ARGS.network != null)) {
        Log.err("ip and network options must not be used together");
        H2O.exit(-1);
      }

      ArrayList<UserSpecifiedNetwork> networkList = UserSpecifiedNetwork.calcArrayList(OPT_ARGS.network);
      if (networkList == null) {
        Log.err("Exiting.");
        H2O.exit(-1);
      }

      // Get a list of all valid IPs on this machine.
      ArrayList<InetAddress> ips = calcPrioritizedInetAddressList();

      InetAddress local = null;   // My final choice

      // Check for an "-ip xxxx" option and accept a valid user choice; required
      // if there are multiple valid IP addresses.
      InetAddress arg = null;
      if (OPT_ARGS.ip != null) {
        try{
          arg = InetAddress.getByName(OPT_ARGS.ip);
        } catch( UnknownHostException e ) {
          Log.err(e);
          H2O.exit(-1);
        }
        if( !(arg instanceof Inet4Address) ) {
          Log.warn("Only IP4 addresses allowed.");
          H2O.exit(-1);
        }
        if( !ips.contains(arg) ) {
          Log.warn("IP address not found on this machine");
          H2O.exit(-1);
        }
        local = arg;
      } else if (networkList.size() > 0) {
        // Return the first match from the list, if any.
        // If there are no matches, then exit.
        Log.info("Network list was specified by the user.  Searching for a match...");
        for( InetAddress ip : ips ) {
          Log.info("    Considering " + ip.getHostAddress() + " ...");
          for ( UserSpecifiedNetwork n : networkList ) {
            if (n.inetAddressOnNetwork(ip)) {
              Log.info("    Matched " + ip.getHostAddress());
              local = ip;
              SELF_ADDRESS = local;
              return SELF_ADDRESS;
            }
          }
        }

        Log.err("No interface matches the network list from the -network option.  Exiting.");
        H2O.exit(-1);
      }
      else {
        // No user-specified IP address.  Attempt auto-discovery.  Roll through
        // all the network choices on looking for a single Inet4.
        ArrayList<InetAddress> validIps = new ArrayList();
        for( InetAddress ip : ips ) {
          // make sure the given IP address can be found here
          if( ip instanceof Inet4Address &&
              !ip.isLoopbackAddress() &&
              !ip.isLinkLocalAddress() ) {
            validIps.add(ip);
          }
        }
        if( validIps.size() == 1 ) {
          local = validIps.get(0);
        } else {
          local = guessInetAddress(validIps);
        }
      }

      // The above fails with no network connection, in that case go for a truly
      // local host.
      if( local == null ) {
        try {
          Log.warn("Failed to determine IP, falling back to localhost.");
          // set default ip address to be 127.0.0.1 /localhost
          local = InetAddress.getByName("127.0.0.1");
        } catch( UnknownHostException e ) {
          throw  Log.errRTExcept(e);
        }
      }
      SELF_ADDRESS = local;
    }
    return SELF_ADDRESS;
  }

  private static InetAddress guessInetAddress(List<InetAddress> ips) {
    String m = "Multiple local IPs detected:\n";
    for(InetAddress ip : ips) m+="  " + ip;
    m+="\nAttempting to determine correct address...\n";
    Socket s = null;
    try {
      // using google's DNS server as an external IP to find
      // Add a timeout to the touch of google.
      // https://0xdata.atlassian.net/browse/HEX-743
      s = new Socket();
      // only 3000 milliseconds before giving up
      // Exceptions: IOException, SocketTimeoutException, plus two Illegal* exceptions
      s.connect(new InetSocketAddress("8.8.8.8", 53), 3000);
      m+="Using " + s.getLocalAddress() + "\n";
      return s.getLocalAddress();
    } catch( java.net.SocketException se ) {
      return null;           // No network at all?  (Laptop w/wifi turned off?)
    } catch( java.net.SocketTimeoutException se ) {
      return null;           // could be firewall?
    } catch( Throwable t ) {
      Log.err(t);
      return null;
    } finally {
      Log.info(m);
      Utils.close(s);
    }
  }



  // --------------------------------------------------------------------------
  // The (local) set of Key/Value mappings.
  static final NonBlockingHashMap<Key,Value> STORE = new NonBlockingHashMap<Key, Value>();

  // Dummy shared volatile for ordering games
  static public volatile int VOLATILE;

  // PutIfMatch
  // - Atomically update the STORE, returning the old Value on success
  // - Kick the persistence engine as needed
  // - Return existing Value on fail, no change.
  //
  // Keys are interned here: I always keep the existing Key, if any. The
  // existing Key is blind jammed into the Value prior to atomically inserting
  // it into the STORE and interning.
  //
  // Because of the blind jam, there is a narrow unusual race where the Key
  // might exist but be stale (deleted, mapped to a TOMBSTONE), a fresh put()
  // can find it and jam it into the Value, then the Key can be deleted
  // completely (e.g. via an invalidate), the table can resize flushing the
  // stale Key, an unrelated weak-put can re-insert a matching Key (but as a
  // new Java object), and delete it, and then the original thread can do a
  // successful put_if_later over the missing Key and blow the invariant that a
  // stored Value always points to the physically equal Key that maps to it
  // from the STORE. If this happens, some of replication management bits in
  // the Key will be set in the wrong Key copy... leading to extra rounds of
  // replication.
  public static Value putIfMatch( Key key, Value val, Value old ) {
    if( old != null ) // Have an old value?
      key = old._key; // Use prior key
    if( val != null )
      val._key = key;

    // Insert into the K/V store
    Value res = STORE.putIfMatchUnlocked(key,val,old);
    if( res != old ) return res; // Return the failure cause
    // Persistence-tickle.
    // If the K/V mapping is going away, remove the old guy.
    // If the K/V mapping is changing, let the store cleaner just overwrite.
    // If the K/V mapping is new, let the store cleaner just create
    if( old != null && val == null ) old.removeIce(); // Remove the old guy
    if( val != null ) {
      dirty_store();            // Start storing the new guy
      Scope.track(key);
    }
    return old; // Return success
  }

  // Raw put; no marking the memory as out-of-sync with disk. Used to import
  // initial keys from local storage, or to intern keys.
  public static Value putIfAbsent_raw( Key key, Value val ) {
    Value res = STORE.putIfMatchUnlocked(key,val,null);
    assert res == null;
    return res;
  }

  // Get the value from the store
  public static Value get( Key key ) { return STORE.get(key); }
  public static Value raw_get( Key key ) { return STORE.get(key); }
  public static Key getk( Key key ) { return STORE.getk(key); }
  public static Set<Key> localKeySet( ) { return STORE.keySet(); }
  public static Collection<Value> values( ) { return STORE.values(); }
  public static int store_size() { return STORE.size(); }





  // --------------------------------------------------------------------------
  // The worker pools - F/J pools with different priorities.

  // These priorities are carefully ordered and asserted for... modify with
  // care.  The real problem here is that we can get into cyclic deadlock
  // unless we spawn a thread of priority "X+1" in order to allow progress
  // on a queue which might be flooded with a large number of "<=X" tasks.
  //
  // Example of deadlock: suppose TaskPutKey and the Invalidate ran at the same
  // priority on a 2-node cluster.  Both nodes flood their own queues with
  // writes to unique keys, which require invalidates to run on the other node.
  // Suppose the flooding depth exceeds the thread-limit (e.g. 99); then each
  // node might have all 99 worker threads blocked in TaskPutKey, awaiting
  // remote invalidates - but the other nodes' threads are also all blocked
  // awaiting invalidates!
  //
  // We fix this by being willing to always spawn a thread working on jobs at
  // priority X+1, and guaranteeing there are no jobs above MAX_PRIORITY -
  // i.e., jobs running at MAX_PRIORITY cannot block, and when those jobs are
  // done, the next lower level jobs get unblocked, etc.
  public static final byte        MAX_PRIORITY = Byte.MAX_VALUE-1;
  public static final byte    ACK_ACK_PRIORITY = MAX_PRIORITY-0;
  public static final byte  FETCH_ACK_PRIORITY = MAX_PRIORITY-1;
  public static final byte        ACK_PRIORITY = MAX_PRIORITY-2;
  public static final byte   DESERIAL_PRIORITY = MAX_PRIORITY-3;
  public static final byte INVALIDATE_PRIORITY = MAX_PRIORITY-3;
  public static final byte    GET_KEY_PRIORITY = MAX_PRIORITY-4;
  public static final byte    PUT_KEY_PRIORITY = MAX_PRIORITY-5;
  public static final byte     ATOMIC_PRIORITY = MAX_PRIORITY-6;
  public static final byte        GUI_PRIORITY = MAX_PRIORITY-7;
  public static final byte     MIN_HI_PRIORITY = MAX_PRIORITY-7;
  public static final byte        MIN_PRIORITY = 0;

  // F/J threads that remember the priority of the last task they started
  // working on.
  public static class FJWThr extends ForkJoinWorkerThread {
    public int _priority;
    FJWThr(ForkJoinPool pool) {
      super(pool);
      _priority = ((ForkJoinPool2)pool)._priority;
      setPriority( _priority == Thread.MIN_PRIORITY
                   ? Thread.NORM_PRIORITY-1
                   : Thread. MAX_PRIORITY-1 );
      setName("FJ-"+_priority+"-"+getPoolIndex());
    }
  }
  // Factory for F/J threads, with cap's that vary with priority.
  static class FJWThrFact implements ForkJoinPool.ForkJoinWorkerThreadFactory {
    private final int _cap;
    FJWThrFact( int cap ) { _cap = cap; }
    @Override public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
      int cap = 4 * NUMCPUS;
      return pool.getPoolSize() <= cap ? new FJWThr(pool) : null;
    }
  }

  // A standard FJ Pool, with an expected priority level.
  static class ForkJoinPool2 extends ForkJoinPool {
    final int _priority;
    private ForkJoinPool2(int p, int cap) {
      super((OPT_ARGS == null || OPT_ARGS.nthreads <= 0) ? NUMCPUS : OPT_ARGS.nthreads,
            new FJWThrFact(cap),
            null,
            p<MIN_HI_PRIORITY);
      _priority = p;
    }
    private H2OCountedCompleter poll2() { return (H2OCountedCompleter)pollSubmission(); }
  }

  // Hi-priority work, sorted into individual queues per-priority.
  // Capped at a small number of threads per pool.
  private static final ForkJoinPool2 FJPS[] = new ForkJoinPool2[MAX_PRIORITY+1];
  static {
    // Only need 1 thread for the AckAck work, as it cannot block
    FJPS[ACK_ACK_PRIORITY] = new ForkJoinPool2(ACK_ACK_PRIORITY,1);
    for( int i=MIN_HI_PRIORITY+1; i<MAX_PRIORITY; i++ )
      FJPS[i] = new ForkJoinPool2(i,NUMCPUS); // All CPUs, but no more for blocking purposes
    FJPS[GUI_PRIORITY] = new ForkJoinPool2(GUI_PRIORITY,2);
  }

  // Easy peeks at the FJ queues
  static int getWrkQueueSize  (int i) { return FJPS[i]==null ? -1 : FJPS[i].getQueuedSubmissionCount();}
  static int getWrkThrPoolSize(int i) { return FJPS[i]==null ? -1 : FJPS[i].getPoolSize();             }

  // Submit to the correct priority queue
  public static H2OCountedCompleter submitTask( H2OCountedCompleter task ) {
    int priority = task.priority();
    assert MIN_PRIORITY <= priority && priority <= MAX_PRIORITY:"priority " + priority + " is out of range, expected range is < " + MIN_PRIORITY + "," + MAX_PRIORITY + ">";
    if( FJPS[priority]==null )
      synchronized( H2O.class ) { if( FJPS[priority] == null ) FJPS[priority] = new ForkJoinPool2(priority,-1); }
    FJPS[priority].submit(task);
    return task;
  }

  // Simple wrapper over F/J CountedCompleter to support priority queues.  F/J
  // queues are simple unordered (and extremely light weight) queues.  However,
  // we frequently need priorities to avoid deadlock and to promote efficient
  // throughput (e.g. failure to respond quickly to TaskGetKey can block an
  // entire node for lack of some small piece of data).  So each attempt to do
  // lower-priority F/J work starts with an attempt to work & drain the
  // higher-priority queues.
  public static abstract class
    H2OCountedCompleter<T extends H2OCountedCompleter> extends CountedCompleter implements Cloneable {
    public H2OCountedCompleter(){}
    protected H2OCountedCompleter(H2OCountedCompleter completer){super(completer);}

    // Once per F/J task, drain the high priority queue before doing any low
    // priority work.
    @Override public final void compute() {
      FJWThr t = (FJWThr)Thread.currentThread();
      int pp = ((ForkJoinPool2)t.getPool())._priority;
      // Drain the high priority queues before the normal F/J queue
      H2OCountedCompleter h2o = null;
      try {
        assert  priority() == pp; // Job went to the correct queue?
        assert t._priority <= pp; // Thread attempting the job is only a low-priority?
        final int p2 = Math.max(pp,MIN_HI_PRIORITY);
        for( int p = MAX_PRIORITY; p > p2; p-- ) {
          if( FJPS[p] == null ) continue;
          h2o = FJPS[p].poll2();
          if( h2o != null ) {     // Got a hi-priority job?
            t._priority = p;      // Set & do it now!
            t.setPriority(Thread.MAX_PRIORITY-1);
            h2o.compute2();       // Do it ahead of normal F/J work
            p++;                  // Check again the same queue
          }
        }
      } catch( Throwable ex ) {
        // If the higher priority job popped an exception, complete it
        // exceptionally...  but then carry on and do the lower priority job.
        if( h2o != null ) h2o.onExceptionalCompletion(ex, h2o.getCompleter());
        else ex.printStackTrace();
      } finally {
        t._priority = pp;
        if( pp == MIN_PRIORITY ) t.setPriority(Thread.NORM_PRIORITY-1);
      }
      // Now run the task as planned
      compute2();
    }
    // Do the actually intended work
    public abstract void compute2();
    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
      if(!(ex instanceof JobCancelledException) && !(ex instanceof IllegalArgumentException) && this.getCompleter() == null)
        ex.printStackTrace();
      return true;
    }
    // In order to prevent deadlock, threads that block waiting for a reply
    // from a remote node, need the remote task to run at a higher priority
    // than themselves.  This field tracks the required priority.
    public byte priority() { return MIN_PRIORITY; }
    @Override public T clone(){
      try { return (T)super.clone(); }
      catch( CloneNotSupportedException e ) { throw water.util.Log.errRTExcept(e); }
    }
  }


  public static abstract class H2OCallback<T extends H2OCountedCompleter> extends H2OCountedCompleter{
    public H2OCallback(){}
    public H2OCallback(H2OCountedCompleter cc){super(cc);}
    @Override public void compute2(){throw new UnsupportedOperationException();}
    @Override public void onCompletion(CountedCompleter caller){callback((T) caller);}
    public abstract void callback(T t);
  }

  public static class H2OEmptyCompleter extends H2OCountedCompleter{
    public H2OEmptyCompleter(){}
    public H2OEmptyCompleter(H2OCountedCompleter cc){super(cc);}
    @Override public void compute2(){throw new UnsupportedOperationException();}
  }


  // --------------------------------------------------------------------------
  public static OptArgs OPT_ARGS = new OptArgs();
  public static class OptArgs extends Arguments.Opt {
    public String name; // set_cloud_name_and_mcast()
    public String flatfile; // set_cloud_name_and_mcast()
    public int baseport; // starting number to search for open ports
    public int port; // set_cloud_name_and_mcast()
    public String ip; // Named IP4/IP6 address instead of the default
    public String network; // Network specification for acceptable interfaces to bind to.
    public String ice_root; // ice root directory
    public String hdfs; // HDFS backend
    public String hdfs_version; // version of the filesystem
    public String hdfs_config; // configuration file of the HDFS
    public String hdfs_skip = null; // used by hadoop driver to not unpack and load any hdfs jar file at runtime.
    public String aws_credentials; // properties file for aws credentials
    public String keepice; // Do not delete ice on startup
    public String soft = null; // soft launch for demos
    public String random_udp_drop = null; // test only, randomly drop udp incoming
    public int pparse_limit = Integer.MAX_VALUE;
    public String no_requests_log = null; // disable logging of Web requests
    public boolean check_rest_params = true; // enable checking unused/unknown REST params e.g., -check_rest_params=false disable control of unknown rest params
    public int    nthreads=NUMCPUS; // desired F/J parallelism level for low priority queues.
    public String license; // License file
    public String h = null;
    public String help = null;
    public String version = null;
    public String single_precision = null;
    public int data_max_factor_levels;
    public String many_cols = null;
    public int chunk_bytes;
    public String beta = null;
    public String mem_watchdog = null; // For developer debugging
    public boolean md5skip = false;
    public boolean ga_opt_out = false;
    public String ga_hadoop_ver = null;
    public boolean no_ice = false;
  }

  public static void printHelp() {
    String s =
    "Start an H2O node.\n" +
    "\n" +
    "Usage:  java [-Xmx<size>] -jar h2o.jar [options]\n" +
    "        (Note that every option has a default and is optional.)\n" +
    "\n" +
    "    -h | -help\n" +
    "          Print this help.\n" +
    "\n" +
    "    -version\n" +
    "          Print version info and exit.\n" +
    "\n" +
    "    -name <h2oCloudName>\n" +
    "          Cloud name used for discovery of other nodes.\n" +
    "          Nodes with the same cloud name will form an H2O cloud\n" +
    "          (also known as an H2O cluster).\n" +
    "\n" +
    "    -flatfile <flatFileName>\n" +
    "          Configuration file explicitly listing H2O cloud node members.\n" +
    "\n" +
    "    -ip <ipAddressOfNode>\n" +
    "          IP address of this node.\n" +
    "\n" +
    "    -port <port>\n" +
    "          Port number for this node (note: port+1 is also used).\n" +
    "          (The default port is " + DEFAULT_PORT + ".)\n" +
    "\n" +
    "    -network <IPv4network1Specification>[,<IPv4network2Specification> ...]\n" +
    "          The IP address discovery code will bind to the first interface\n" +
    "          that matches one of the networks in the comma-separated list.\n" +
    "          Use instead of -ip when a broad range of addresses is legal.\n" +
    "          (Example network specification: '10.1.2.0/24' allows 256 legal\n" +
    "          possibilities.)\n" +
    "\n" +
    "    -ice_root <fileSystemPath>\n" +
    "          The directory where H2O spills temporary data to disk.\n" +
    "          (The default is '" + DEFAULT_ICE_ROOT() + "'.)\n" +
    "\n" +
    "    -single_precision\n" +
    "          Reduce the max. (storage) precision for floating point numbers\n" +
    "          from double to single precision to save memory of numerical data.\n" +
    "          (The default is double precision.)\n" +
    "\n" +
    "    -many_cols\n" +
    "          Enables improved handling of high-dimensional datasets.  Same as -chunk_bytes 24.\n" +
    "\n" +
    "    -chunk_bytes <integer>\n" +
    "          Experimental option. Not in combination with -many_cols. The log (base 2) of chunk size in bytes.\n" +
    "          (The default is " + LOG_CHK + ", which leads to a chunk size of " + PrettyPrint.bytes(1<<LOG_CHK) + ".)\n" +
    "\n" +
    "    -data_max_factor_levels <integer>\n" +
    "          The maximum number of factor levels for categorical columns.\n" +
    "          Columns with more than the specified number of factor levels\n" +
    "          are converted into all missing values.\n" +
    "          (The default is " + DATA_MAX_FACTOR_LEVELS + ".)\n" +
    "\n" +
    "    -nthreads <#threads>\n" +
    "          Maximum number of threads in the low priority batch-work queue.\n" +
    "          (The default is 4*numcpus.)\n" +
    "\n" +
    "    -license <licenseFilePath>\n" +
    "          Path to license file on local filesystem.\n" +
    "\n" +
    "Cloud formation behavior:\n" +
    "\n" +
    "    New H2O nodes join together to form a cloud at startup time.\n" +
    "    Once a cloud is given work to perform, it locks out new members\n" +
    "    from joining.\n" +
    "\n" +
    "Examples:\n" +
    "\n" +
    "    Start an H2O node with 4GB of memory and a default cloud name:\n" +
    "        $ java -Xmx4g -jar h2o.jar\n" +
    "\n" +
    "    Start an H2O node with 6GB of memory and a specify the cloud name:\n" +
    "        $ java -Xmx6g -jar h2o.jar -name MyCloud\n" +
    "\n" +
    "    Start an H2O cloud with three 2GB nodes and a default cloud name:\n" +
    "        $ java -Xmx2g -jar h2o.jar &\n" +
    "        $ java -Xmx2g -jar h2o.jar &\n" +
    "        $ java -Xmx2g -jar h2o.jar &\n" +
    "\n";

    System.out.print(s);
  }

  public static boolean IS_SYSTEM_RUNNING = false;

  /** Load a h2o build version or return default unknown version
   * @return never returns null
   */
  public static AbstractBuildVersion getBuildVersion() {
    try {
      Class klass = Class.forName("water.BuildVersion");
      java.lang.reflect.Constructor constructor = klass.getConstructor();
      AbstractBuildVersion abv = (AbstractBuildVersion) constructor.newInstance();
      return abv;
      // it exists on the classpath
    } catch (Exception e) {
      return AbstractBuildVersion.UNKNOWN_VERSION;
    }
  }

  /**
   * If logging has not been setup yet, then Log.info will only print to stdout.
   * This allows for early processing of the '-version' option without unpacking
   * the jar file and other startup stuff.
   */
  public static void printAndLogVersion() {
    // Try to load a version
    AbstractBuildVersion abv = getBuildVersion();
    String build_branch = abv.branchName();
    String build_hash = abv.lastCommitHash();
    String build_describe = abv.describe();
    String build_project_version = abv.projectVersion();
    String build_by = abv.compiledBy();
    String build_on = abv.compiledOn();

    Log.info ("----- H2O started -----");
    Log.info ("Build git branch: " + build_branch);
    Log.info ("Build git hash: " + build_hash);
    Log.info ("Build git describe: " + build_describe);
    Log.info ("Build project version: " + build_project_version);
    Log.info ("Built by: '" + build_by + "'");
    Log.info ("Built on: '" + build_on + "'");

    Runtime runtime = Runtime.getRuntime();
    double ONE_GB = 1024 * 1024 * 1024;
    Log.info ("Java availableProcessors: " + runtime.availableProcessors());
    Log.info ("Java heap totalMemory: " + String.format("%.2f gb", runtime.totalMemory() / ONE_GB));
    Log.info ("Java heap maxMemory: " + String.format("%.2f gb", runtime.maxMemory() / ONE_GB));
    Log.info ("Java version: " + String.format("Java %s (from %s)", System.getProperty("java.version"), System.getProperty("java.vendor")));
    Log.info ("OS   version: " + String.format("%s %s (%s)", System.getProperty("os.name"), System.getProperty("os.version"), System.getProperty("os.arch")));
    long totalMemory = OSUtils.getTotalPhysicalMemory();
    Log.info ("Machine physical memory: " + (totalMemory==-1 ? "NA" : String.format("%.2f gb", totalMemory / ONE_GB)));
  }

  /**
   * We had a report from a user that H2O didn't start properly on MacOS X in a
   * case where the user was part of the root group.  So warn about it.
   */
  public static void printWarningIfRootOnMac() {
    String os_name = System.getProperty("os.name");
    if (os_name.equals("Mac OS X")) {
      String user_name = System.getProperty("user.name");
      if (user_name.equals("root")) {
        Log.warn("Running as root on MacOS; check if java binary is unintentionally setuid");
      }
    }
  }

  public static String getVersion() {
    String build_project_version = "(unknown)";
    try {
      Class klass = Class.forName("water.BuildVersion");
      java.lang.reflect.Constructor constructor = klass.getConstructor();
      AbstractBuildVersion abv = (AbstractBuildVersion) constructor.newInstance();
      build_project_version = abv.projectVersion();
      // it exists on the classpath
    } catch (Exception e) {
      // it does not exist on the classpath
    }
    return build_project_version;
  }

  // Start up an H2O Node and join any local Cloud
  public static void main( String[] args ) {
    Log.POST(300,"");
    // To support launching from JUnit, JUnit expects to call main() repeatedly.
    // We need exactly 1 call to main to startup all the local services.
    if (IS_SYSTEM_RUNNING) return;
    IS_SYSTEM_RUNNING = true;

    VERSION = getVersion();   // Pick this up from build-specific info.
    START_TIME_MILLIS = System.currentTimeMillis();

    // Parse args
    Arguments arguments = new Arguments(args);
    arguments.extract(OPT_ARGS);
    ARGS = arguments.toStringArray();

    printAndLogVersion();
    printWarningIfRootOnMac();

    GA = new GoogleAnalytics("UA-56665317-2","H2O",H2O.getVersion());
    if((new File(".h2o_no_collect")).exists()
            || (new File(System.getProperty("user.home")+File.separator+".h2o_no_collect")).exists()
            || OPT_ARGS.ga_opt_out ) {
      GA.setEnabled(false);
      Log.info("Opted out of sending usage metrics.");
    }

    if (OPT_ARGS.baseport != 0) {
      DEFAULT_PORT = OPT_ARGS.baseport;
    }
    SINGLE_PRECISION = OPT_ARGS.single_precision != null;
    if (SINGLE_PRECISION) Log.info("Using single precision for floating-point numbers.");

    if (OPT_ARGS.data_max_factor_levels != 0) {
      DATA_MAX_FACTOR_LEVELS = OPT_ARGS.data_max_factor_levels;
      Log.info("Max. number of factor levels per column: " + DATA_MAX_FACTOR_LEVELS);
    }

    if (OPT_ARGS.chunk_bytes != 0 || OPT_ARGS.many_cols != null) {
      if (OPT_ARGS.many_cols != null) {
        LOG_CHK = 24;
        if (OPT_ARGS.chunk_bytes > 0) {
          Log.warn("-chunk_bytes is ignored since -many_cols was set.");
        }
      } else if (OPT_ARGS.chunk_bytes > 0) {
        LOG_CHK = OPT_ARGS.chunk_bytes;
        if (OPT_ARGS.chunk_bytes < 22) {
          Log.warn("-chunk_bytes < 22 is not officially supported. Use at your own risk.");
        }
        if (OPT_ARGS.chunk_bytes > 24) {
          Log.warn("-chunk_bytes > 24 is not officially supported. Use at your own risk.");
        }
      }
    }
    Log.info("Chunk size: " + PrettyPrint.bytes(1<<LOG_CHK));

    // Get ice path before loading Log or Persist class
    String ice = DEFAULT_ICE_ROOT();
    if( OPT_ARGS.ice_root != null ) ice = OPT_ARGS.ice_root.replace("\\", "/");
    try {
      ICE_ROOT = new URI(ice);
    } catch(URISyntaxException ex) {
      throw new RuntimeException("Invalid ice_root: " + ice + ", " + ex.getMessage());
    }

    Log.info ("ICE root: '" + ICE_ROOT + "'");

    findInetAddressForSelf();

    //if (OPT_ARGS.rshell.equals("false"))
    Log.POST(310,"");
    Log.wrap(); // Logging does not wrap when the rshell is on.

    // Start the local node
    startLocalNode();
    Log.POST(320,"");

    String logDir = (Log.getLogDir() != null) ? Log.getLogDir() : "(unknown)";
    Log.info ("Log dir: '" + logDir + "'");

    // Load up from disk and initialize the persistence layer
    initializePersistence();
    Log.POST(340, "");
    initializeLicenseManager();
    Log.POST(345, "");
    // Start network services, including heartbeats & Paxos
    startNetworkServices();   // start server services
    Log.POST(350,"");
    startApiIpPortWatchdog(); // Check if the API port becomes unreachable
    Log.POST(360,"");
    if (OPT_ARGS.mem_watchdog != null) {
      startMemoryWatchdog();
      Log.POST(370, "");
    }
    startupFinalize(); // finalizes the startup & tests (if any)
    Log.POST(380,"");

    startGAStartupReport();
  }

  /** Starts the local k-v store.
   * Initializes the local k-v store, local node and the local cloud with itself
   * as the only member.
   */
  private static void startLocalNode() {
    // Print this first, so if any network stuff is affected it's clear this is going on.
    if (OPT_ARGS.random_udp_drop != null) {
      Log.warn("Debugging option RANDOM UDP DROP is ENABLED, make sure you really meant it");
    }

    // Figure self out; this is surprisingly hard
    initializeNetworkSockets();
    // Do not forget to put SELF into the static configuration (to simulate
    // proper multicast behavior)
    if( STATIC_H2OS != null && !STATIC_H2OS.contains(SELF)) {
      Log.warn("Flatfile configuration does not include self: " + SELF+ " but contains " + STATIC_H2OS);
      STATIC_H2OS.add(SELF);
    }

    Log.info ("H2O cloud name: '" + NAME + "'");
    Log.info("(v"+VERSION+") '"+NAME+"' on " + SELF+(OPT_ARGS.flatfile==null
        ? (", discovery address "+CLOUD_MULTICAST_GROUP+":"+CLOUD_MULTICAST_PORT)
            : ", static configuration based on -flatfile "+OPT_ARGS.flatfile));

    Log.info("If you have trouble connecting, try SSH tunneling from your local machine (e.g., via port 55555):\n" +
            "  1. Open a terminal and run 'ssh -L 55555:localhost:"
            + API_PORT + " " + System.getProperty("user.name") + "@" + SELF_ADDRESS.getHostAddress() + "'\n" +
            "  2. Point your browser to http://localhost:55555");


    // Create the starter Cloud with 1 member
    SELF._heartbeat._jar_md5 = Boot._init._jarHash;
    Paxos.doHeartbeat(SELF);
    assert SELF._heartbeat._cloud_hash != 0;
  }

  /** Initializes the network services of the local node.
*
* Starts the worker threads, receiver threads, heartbeats and all other
* network related services.
*/
  private static void startNetworkServices() {
    // We've rebooted the JVM recently. Tell other Nodes they can ignore task
    // prior tasks by us. Do this before we receive any packets
    UDPRebooted.T.reboot.broadcast();

    // Start the UDPReceiverThread, to listen for requests from other Cloud
    // Nodes. There should be only 1 of these, and it never shuts down.
    // Started first, so we can start parsing UDP packets
    new UDPReceiverThread().start();

    // Start the MultiReceiverThread, to listen for multi-cast requests from
    // other Cloud Nodes. There should be only 1 of these, and it never shuts
    // down. Started soon, so we can start parsing multicast UDP packets
    new MultiReceiverThread().start();

    // Start the Persistent meta-data cleaner thread, which updates the K/V
    // mappings periodically to disk. There should be only 1 of these, and it
    // never shuts down.  Needs to start BEFORE the HeartBeatThread to build
    // an initial histogram state.
    new Cleaner().start();

    // Start the heartbeat thread, to publish the Clouds' existence to other
    // Clouds. This will typically trigger a round of Paxos voting so we can
    // join an existing Cloud.
    new HeartBeatThread().start();

    // Start a UDP timeout worker thread. This guy only handles requests for
    // which we have not recieved a timely response and probably need to
    // arrange for a re-send to cover a dropped UDP packet.
    new UDPTimeOutThread().start();
    new H2ONode.AckAckTimeOutThread().start();

    // Start the TCPReceiverThread, to listen for TCP requests from other Cloud
    // Nodes. There should be only 1 of these, and it never shuts down.
    new TCPReceiverThread().start();
    // Start the Nano HTTP server thread
    water.api.RequestServer.start();
  }

  /** Initializes a watchdog thread to make sure the API IP:Port is reachable.
   *
   * The IP and port are meant to be accessible from outside this
   * host, much less inside.  The real reason behind this check is the
   * one-node cloud case where people move their laptop around and
   * DHCP assigns them a new IP address.
   */
  private static void startApiIpPortWatchdog() {
    apiIpPortWatchdog = new ApiIpPortWatchdogThread();
    apiIpPortWatchdog.start();
  }

  private static void startMemoryWatchdog() {
    new MemoryWatchdogThread().start();
  }

  private static void startGAStartupReport() {
    new GAStartupReportThread().start();
  }

  // Used to update the Throwable detailMessage field.
  private static java.lang.reflect.Field DETAILMESSAGE;
  public static <T extends Throwable> T setDetailMessage( T t, String s ) {
    try { if( DETAILMESSAGE != null )  DETAILMESSAGE.set(t,s); }
    catch( IllegalAccessException iae) {}
    return t;
  }


  /** Finalizes the node startup.
   *
   * Displays the startup message and runs the tests (if applicable).
   */
  private static void startupFinalize() {
    // Allow Throwable detailMessage's to be updated on the fly.  Ugly, ugly,
    // but I want to add info without rethrowing/rebuilding whole exceptions.
    try {
      DETAILMESSAGE = Throwable.class.getDeclaredField("detailMessage");
      DETAILMESSAGE.setAccessible(true);
    } catch( NoSuchFieldException nsfe ) { }

    // Sleep a bit so all my other threads can 'catch up'
    try { Thread.sleep(100); } catch( InterruptedException e ) { }
  }

  public static DatagramChannel _udpSocket;
  public static ServerSocket _apiSocket;


  // Parse arguments and set cloud name in any case. Strip out "-name NAME"
  // and "-flatfile <filename>". Ignore the rest. Set multi-cast port as a hash
  // function of the name. Parse node ip addresses from the filename.
  static void initializeNetworkSockets( ) {
    // Assign initial ports
    API_PORT = OPT_ARGS.port != 0 ? OPT_ARGS.port : DEFAULT_PORT;

    while (true) {
      H2O_PORT = API_PORT+1;
      if( API_PORT<0 || API_PORT>65534 ) // 65535 is max, implied for udp port
        Log.die("Attempting to use system illegal port, either "+API_PORT+" or "+ H2O_PORT);
      try {
        // kbn. seems like we need to set SO_REUSEADDR before binding?
        // http://www.javadocexamples.com/java/net/java.net.ServerSocket.html#setReuseAddress:boolean
        // When a TCP connection is closed the connection may remain in a timeout state
        // for a period of time after the connection is closed (typically known as the
        // TIME_WAIT state or 2MSL wait state). For applications using a well known socket address
        // or port it may not be possible to bind a socket to the required SocketAddress
        // if there is a connection in the timeout state involving the socket address or port.
        // Enabling SO_REUSEADDR prior to binding the socket using bind(SocketAddress)
        // allows the socket to be bound even though a previous connection is in a timeout state.
        // cnc: this is busted on windows.  Back to the old code.

        // If the user specified the -ip flag, honor it for the Web UI interface bind.
        // Otherwise bind to all interfaces.
        _apiSocket = OPT_ARGS.ip == null
          ? new ServerSocket(API_PORT)
          : new ServerSocket(API_PORT, -1/*defaultBacklog*/, SELF_ADDRESS);
        _apiSocket.setReuseAddress(true);
        // Bind to the UDP socket
        _udpSocket = DatagramChannel.open();
        _udpSocket.socket().setReuseAddress(true);
        InetSocketAddress isa = new InetSocketAddress(H2O.SELF_ADDRESS, H2O_PORT);
        _udpSocket.socket().bind(isa);
        // Bind to the TCP socket also
        TCPReceiverThread.SOCK = ServerSocketChannel.open();
        TCPReceiverThread.SOCK.socket().setReceiveBufferSize(water.AutoBuffer.TCP_BUF_SIZ);
        TCPReceiverThread.SOCK.socket().bind(isa);
        break;
      } catch (IOException e) {
        try { if( _apiSocket != null ) _apiSocket.close(); } catch( IOException ohwell ) { Log.err(ohwell); }
        Utils.close(_udpSocket);
        if( TCPReceiverThread.SOCK != null ) try { TCPReceiverThread.SOCK.close(); } catch( IOException ie ) { }
        _apiSocket = null;
        _udpSocket = null;
        TCPReceiverThread.SOCK = null;
        if( OPT_ARGS.port != 0 )
          Log.die("On " + SELF_ADDRESS +
              " some of the required ports " + (OPT_ARGS.port+0) +
              ", " + (OPT_ARGS.port+1) +
              " are not available, change -port PORT and try again.");
      }
      API_PORT += 2;
    }
    SELF = H2ONode.self(SELF_ADDRESS);
    Log.info("Internal communication uses port: ", H2O_PORT,"\nListening for HTTP and REST traffic on  http://",SELF_ADDRESS.getHostAddress(),":"+_apiSocket.getLocalPort()+"/");

    String embeddedConfigFlatfile = null;
    AbstractEmbeddedH2OConfig ec = getEmbeddedH2OConfig();
    if (ec != null) {
      ec.notifyAboutEmbeddedWebServerIpPort (SELF_ADDRESS, API_PORT);
      if (ec.providesFlatfile()) {
        try {
          embeddedConfigFlatfile = ec.fetchFlatfile();
        }
        catch (Exception e) {
          Log.err("Failed to get embedded config flatfile");
          Log.err(e);
          H2O.exit(1);
        }
      }
    }

    NAME = OPT_ARGS.name==null? System.getProperty("user.name") : OPT_ARGS.name;
    // Read a flatfile of allowed nodes
    if (embeddedConfigFlatfile != null) {
      STATIC_H2OS = parseFlatFileFromString(embeddedConfigFlatfile);
    }
    else {
      STATIC_H2OS = parseFlatFile(OPT_ARGS.flatfile);
    }

    // Multi-cast ports are in the range E1.00.00.00 to EF.FF.FF.FF
    int hash = NAME.hashCode()&0x7fffffff;
    int port = (hash % (0xF0000000-0xE1000000))+0xE1000000;
    byte[] ip = new byte[4];
    for( int i=0; i<4; i++ )
      ip[i] = (byte)(port>>>((3-i)<<3));
    try {
      CLOUD_MULTICAST_GROUP = InetAddress.getByAddress(ip);
    } catch( UnknownHostException e ) { throw  Log.errRTExcept(e); }
    CLOUD_MULTICAST_PORT = (port>>>16);
  }

  // Multicast send-and-close.  Very similar to udp_send, except to the
  // multicast port (or all the individuals we can find, if multicast is
  // disabled).
  static void multicast( ByteBuffer bb ) {
    try { multicast2(bb); }
    catch (Exception xe) {}
  }

  static private void multicast2( ByteBuffer bb ) {
    if( H2O.STATIC_H2OS == null ) {
      byte[] buf = new byte[bb.remaining()];
      bb.get(buf);

      synchronized( H2O.class ) { // Sync'd so single-thread socket create/destroy
        assert H2O.CLOUD_MULTICAST_IF != null;
        try {
          if( CLOUD_MULTICAST_SOCKET == null ) {
            CLOUD_MULTICAST_SOCKET = new MulticastSocket();
            // Allow multicast traffic to go across subnets
            CLOUD_MULTICAST_SOCKET.setTimeToLive(2);
            CLOUD_MULTICAST_SOCKET.setNetworkInterface(H2O.CLOUD_MULTICAST_IF);
          }
          // Make and send a packet from the buffer
          CLOUD_MULTICAST_SOCKET.send(new DatagramPacket(buf, buf.length, CLOUD_MULTICAST_GROUP,CLOUD_MULTICAST_PORT));
        } catch( Exception e ) {  // On any error from anybody, close all sockets & re-open
          // and if not a soft launch (hibernate mode)
          if(H2O.OPT_ARGS.soft == null)
            Log.err("Multicast Error ",e);
          if( CLOUD_MULTICAST_SOCKET != null )
            try { CLOUD_MULTICAST_SOCKET.close(); }
            catch( Exception e2 ) { Log.err("Got",e2); }
            finally { CLOUD_MULTICAST_SOCKET = null; }
        }
      }
    } else {                    // Multicast Simulation
      // The multicast simulation is little bit tricky. To achieve union of all
      // specified nodes' flatfiles (via option -flatfile), the simulated
      // multicast has to send packets not only to nodes listed in the node's
      // flatfile (H2O.STATIC_H2OS), but also to all cloud members (they do not
      // need to be specified in THIS node's flatfile but can be part of cloud
      // due to another node's flatfile).
      //
      // Furthermore, the packet have to be send also to Paxos proposed members
      // to achieve correct functionality of Paxos.  Typical situation is when
      // this node receives a Paxos heartbeat packet from a node which is not
      // listed in the node's flatfile -- it means that this node is listed in
      // another node's flatfile (and wants to create a cloud).  Hence, to
      // allow cloud creation, this node has to reply.
      //
      // Typical example is:
      //    node A: flatfile (B)
      //    node B: flatfile (C), i.e., A -> (B), B-> (C), C -> (A)
      //    node C: flatfile (A)
      //    Cloud configuration: (A, B, C)
      //

      // Hideous O(n) algorithm for broadcast - avoid the memory allocation in
      // this method (since it is heavily used)
      HashSet<H2ONode> nodes = (HashSet<H2ONode>)H2O.STATIC_H2OS.clone();
      nodes.addAll(Paxos.PROPOSED.values());
      bb.mark();
      for( H2ONode h2o : nodes ) {
        bb.reset();
        try {
          H2O.CLOUD_DGRAM.send(bb, h2o._key);
        } catch( IOException e ) {
          Log.warn("Multicast Error to "+h2o+e);
        }
      }
    }
  }


  /**
   * Read a set of Nodes from a file. Format is:
   *
   * name/ip_address:port
   * - name is unused and optional
   * - port is optional
   * - leading '#' indicates a comment
   *
   * For example:
   *
   * 10.10.65.105:54322
   * # disabled for testing
   * # 10.10.65.106
   * /10.10.65.107
   * # run two nodes on 108
   * 10.10.65.108:54322
   * 10.10.65.108:54325
   */
  private static HashSet<H2ONode> parseFlatFile( String fname ) {
    if( fname == null ) return null;
    File f = new File(fname);
    if( !f.exists() ) {
      Log.warn("-flatfile specified but not found: " + fname);
      return null; // No flat file
    }
    HashSet<H2ONode> h2os = new HashSet<H2ONode>();
    List<FlatFileEntry> list = parseFlatFile(f);
    for(FlatFileEntry entry : list)
      h2os.add(H2ONode.intern(entry.inet, entry.port+1));// use the UDP port here
    return h2os;
  }

  public static HashSet<H2ONode> parseFlatFileFromString( String s ) {
    HashSet<H2ONode> h2os = new HashSet<H2ONode>();
    InputStream is = new ByteArrayInputStream(s.getBytes());
    List<FlatFileEntry> list = parseFlatFile(is);
    for(FlatFileEntry entry : list)
      h2os.add(H2ONode.intern(entry.inet, entry.port+1));// use the UDP port here
    return h2os;
  }

  public static class FlatFileEntry {
    public InetAddress inet;
    public int port;
  }

  public static List<FlatFileEntry> parseFlatFile( File f ) {
    InputStream is = null;
    try {
      is = new FileInputStream(f);
    }
    catch (Exception e) { Log.die(e.toString()); }
    return parseFlatFile(is);
  }

  public static List<FlatFileEntry> parseFlatFile( InputStream is ) {
    List<FlatFileEntry> list = new ArrayList<FlatFileEntry>();
    BufferedReader br = null;
    int port = DEFAULT_PORT;
    try {
      br = new BufferedReader(new InputStreamReader(is));
      String strLine = null;
      while( (strLine = br.readLine()) != null) {
        strLine = strLine.trim();
        // be user friendly and skip comments and empty lines
        if (strLine.startsWith("#") || strLine.isEmpty()) continue;

        String ip = null, portStr = null;
        int slashIdx = strLine.indexOf('/');
        int colonIdx = strLine.indexOf(':');
        if( slashIdx == -1 && colonIdx == -1 ) {
          ip = strLine;
        } else if( slashIdx == -1 ) {
          ip = strLine.substring(0, colonIdx);
          portStr = strLine.substring(colonIdx+1);
        } else if( colonIdx == -1 ) {
          ip = strLine.substring(slashIdx+1);
        } else if( slashIdx > colonIdx ) {
          Log.die("Invalid format, must be name/ip[:port], not '"+strLine+"'");
        } else {
          ip = strLine.substring(slashIdx+1, colonIdx);
          portStr = strLine.substring(colonIdx+1);
        }

        InetAddress inet = InetAddress.getByName(ip);
        if( !(inet instanceof Inet4Address) )
          Log.die("Only IP4 addresses allowed: given " + ip);
        if( portStr!=null && !portStr.equals("") ) {
          try {
            port = Integer.decode(portStr);
          } catch( NumberFormatException nfe ) {
            Log.die("Invalid port #: "+portStr);
          }
        }
        FlatFileEntry entry = new FlatFileEntry();
        entry.inet = inet;
        entry.port = port;
        list.add(entry);
      }
    } catch( Exception e ) { Log.die(e.toString()); }
    finally { Utils.close(br); }
    return list;
  }

  static void initializePersistence() {
    Log.POST(3001);
    HdfsLoader.loadJars();
    Log.POST(3002);
    if( OPT_ARGS.aws_credentials != null ) {
      try {
        Log.POST(3003);
        PersistS3.getClient();
        Log.POST(3004);
      } catch( IllegalArgumentException e ) {
        Log.POST(3005);
        Log.err(e);
      }
    }
    Log.POST(3006);
    Persist.initialize();
    Log.POST(3007);
  }

  static void initializeLicenseManager() {
    licenseManager = new LicenseManager();
    if (OPT_ARGS.license != null) {
      LicenseManager.Result r = licenseManager.readLicenseFile(OPT_ARGS.license);
      if (r == LicenseManager.Result.OK) {
        Log.info("Successfully read license file ("+ OPT_ARGS.license + ")");
        licenseManager.logLicensedFeatures();
      }
      else {
        Log.err("readLicenseFile failed (" + r + ")");
      }
    }
  }

  // Cleaner ---------------------------------------------------------------

  // msec time at which the STORE was dirtied.
  // Long.MAX_VALUE if clean.
  static private volatile long _dirty; // When was store dirtied

  static void dirty_store() { dirty_store(System.currentTimeMillis()); }
  static void dirty_store( long x ) {
    // Keep earliest dirty time seen
    if( x < _dirty ) _dirty = x;
  }

  public abstract static class KVFilter {
    public abstract boolean filter(KeyInfo k);
  }

  public static final class KeyInfo extends Iced implements Comparable<KeyInfo>{
    public final Key _key;
    public final int _type;
    public final boolean _rawData;
    public final int _sz;
    public final int _ncols;
    public final long _nrows;
    public final byte _backEnd;

    public KeyInfo(Key k, Value v){
      assert k!=null : "Key should be not null!";
      assert v!=null : "Value should be not null!";
      _key = k;
      _type = v.type();
      _rawData = v.isRawData();
      if(v.isFrame()){
        Frame f = v.get();
        // NOTE: can't get byteSize here as it may invoke RollupStats! :(

//        _sz = f.byteSize();
        _sz = v._max;
        // do at least nrows/ncols instead
        _ncols = f.numCols();
        _nrows = f.numRows();
      } else {
        _sz = v._max;
        _ncols = 0;
        _nrows = 0;
      }
      _backEnd = v.backend();
    }
    @Override public int compareTo(KeyInfo ki){ return _key.compareTo(ki._key);}

    public boolean isFrame(){
      return _type == TypeMap.onIce(Frame.class.getName());
    }
    public boolean isLockable(){
      return TypeMap.newInstance(_type) instanceof Lockable;
    }
  }

  public static class KeySnapshot extends Iced {
    private static volatile long _lastUpdate;
    private static final long _updateInterval = 1000;
    private static volatile KeySnapshot _cache;
    public final KeyInfo [] _keyInfos;

    public long lastUpdated(){return _lastUpdate;}
    public KeySnapshot cache(){return _cache;}

    public KeySnapshot filter(KVFilter kvf){
      ArrayList<KeyInfo> res = new ArrayList<KeyInfo>();
      for(KeyInfo kinfo: _keyInfos)
        if(kvf.filter(kinfo))res.add(kinfo);
      return new KeySnapshot(res.toArray(new KeyInfo[res.size()]));
    }

    KeySnapshot(KeyInfo [] snapshot){
      _keyInfos = snapshot;}

    public Key [] keys(){
      Key [] res = new Key[_keyInfos.length];
      for(int i = 0; i < _keyInfos.length; ++i)
        res[i] = _keyInfos[i]._key;
      return res;
    }

    public <T extends Iced> Map<String, T> fetchAll(Class<T> c)                { return fetchAll(c,false,0,Integer.MAX_VALUE);}
    public <T extends Iced> Map<String, T> fetchAll(Class<T> c, boolean exact) { return fetchAll(c,exact,0,Integer.MAX_VALUE);}
    public <T extends Iced> Map<String, T> fetchAll(Class<T> c, boolean exact, int offset, int limit) {
      TreeMap<String, T> res = new TreeMap<String, T>();
      final int typeId = TypeMap.onIce(c.getName());
      for (KeyInfo kinfo : _keyInfos) {
        if (kinfo._type == typeId || (!exact && c.isAssignableFrom(TypeMap.clazz(kinfo._type)))) {
          if (offset > 0) {
            --offset;
            continue;
          }
          Value v = DKV.get(kinfo._key);
          if (v != null) {
            T t = v.get();
            res.put(kinfo._key.toString(), t);
            if (res.size() == limit)
              break;
          }
        }
      }
      return res;
    }
    public static KeySnapshot localSnapshot(){return localSnapshot(false);}
    public static KeySnapshot localSnapshot(boolean homeOnly){
      Object [] kvs = STORE.raw_array();
      ArrayList<KeyInfo> res = new ArrayList<KeyInfo>();
      for(int i = 2; i < kvs.length; i+= 2){
        Object ok = kvs[i], ov = kvs[i+1];
        if( !(ok instanceof Key  ) || ov==null ) continue; // Ignore tombstones or deleted values
        Key key = (Key) ok;
        if(!key.user_allowed())continue;
        if(homeOnly && !key.home())continue;
        // Raw array can contain regular and also wrapped values into Prime marker class:
        //  - if we see Value object, create instance of KeyInfo
        //  - if we do not see Value object, try to unwrap it via calling STORE.get and then
        // look at wrapped value again.
        if (!(ov instanceof Value)) {
          ov = H2O.get(key); // H2Oget returns already Value object or null
          if (ov==null) continue;
        }
        res.add(new KeyInfo(key,(Value)ov));
      }
      final KeyInfo [] arr = res.toArray(new KeyInfo[res.size()]);
      Arrays.sort(arr);
      return new KeySnapshot(arr);
    }
    public static KeySnapshot globalSnapshot(){ return globalSnapshot(-1);}
    public static KeySnapshot globalSnapshot(long timeTolerance){
      KeySnapshot res = _cache;
      final long t = System.currentTimeMillis();
      if(res == null || (t - _lastUpdate) > timeTolerance)
        res = new KeySnapshot(new GlobalUKeySetTask().invokeOnAllNodes()._res);
      else if(t - _lastUpdate > _updateInterval)
        H2O.submitTask(new H2OCountedCompleter() {
          @Override
          public void compute2() {
            new GlobalUKeySetTask().invokeOnAllNodes();
          }
        });
      return res;
    }
    private static class GlobalUKeySetTask extends DRemoteTask<GlobalUKeySetTask> {
      KeyInfo [] _res;

      @Override public byte priority(){return H2O.GET_KEY_PRIORITY;}
      @Override public void lcompute(){
        _res = localSnapshot(true)._keyInfos;
        tryComplete();
      }
      @Override public void reduce(GlobalUKeySetTask gbt){
        if(_res == null)_res = gbt._res;
        else if(gbt._res != null){ // merge sort keys together
          KeyInfo [] res = new KeyInfo[_res.length + gbt._res.length];
          int j = 0, k = 0;
          for(int i = 0; i < res.length; ++i)
            res[i] = j < gbt._res.length && (k == _res.length || gbt._res[j].compareTo(_res[k]) < 0)?gbt._res[j++]:_res[k++];
          _res = res;
        }
      }
      @Override public void postGlobal(){
        _cache = new KeySnapshot(_res);
        _lastUpdate = System.currentTimeMillis();
      }
    }
  }

  // Periodically write user keys to disk
  public static class Cleaner extends Thread {
    // Desired cache level. Set by the MemoryManager asynchronously.
    static public volatile long DESIRED;
    // Histogram used by the Cleaner
    private final Histo _myHisto;

    boolean _diskFull = false;

    public Cleaner() {
      super("MemCleaner");
      setDaemon(true);
      setPriority(MAX_PRIORITY-2);
      _dirty = Long.MAX_VALUE;  // Set to clean-store
      _myHisto = new Histo();   // Build/allocate a first histogram
      _myHisto.compute(0);      // Compute lousy histogram; find eldest
      H = _myHisto;             // Force to be the most recent
      _myHisto.histo(true);     // Force a recompute with a good eldest
      MemoryManager.set_goals("init",false);
    }

    static boolean lazyPersist(){ // free disk > our DRAM?
      return !H2O.OPT_ARGS.no_ice && H2O.SELF._heartbeat.get_free_disk() > MemoryManager.MEM_MAX;
    }
    static boolean isDiskFull(){ // free disk space < 5K?
      long space = Persist.getIce().getUsableSpace();
      return space != Persist.UNKNOWN && space < (5 << 10);
    }

    @Override public void run() {
      boolean diskFull = false;
      while( true ) {
        // Sweep the K/V store, writing out Values (cleaning) and free'ing
        // - Clean all "old" values (lazily, optimistically)
        // - Clean and free old values if above the desired cache level
        // Do not let optimistic cleaning get in the way of emergency cleaning.

        // Get a recent histogram, computing one as needed
        Histo h = _myHisto.histo(false);
        long now = System.currentTimeMillis();
        long dirty = _dirty; // When things first got dirtied

        // Start cleaning if: "dirty" was set a "long" time ago, or we beyond
        // the desired cache levels. Inverse: go back to sleep if the cache
        // is below desired levels & nothing has been dirty awhile.
        if( h._cached < DESIRED && // Cache is low and
            (now-dirty < 5000) ) { // not dirty a long time
          // Block asleep, waking every 5 secs to check for stuff, or when poked
          Boot.block_store_cleaner();
          continue; // Awoke; loop back and re-check histogram.
        }

        now = System.currentTimeMillis();
        _dirty = Long.MAX_VALUE; // Reset, since we are going write stuff out
        MemoryManager.set_goals("preclean",false);

        // The age beyond which we need to toss out things to hit the desired
        // caching levels. If forced, be exact (toss out the minimal amount).
        // If lazy, store-to-disk things down to 1/2 the desired cache level
        // and anything older than 5 secs.
        boolean force = (h._cached >= DESIRED); // Forced to clean
        if( force && diskFull )
          diskFull = isDiskFull();
        long clean_to_age = h.clean_to(force ? DESIRED : (DESIRED>>1));
        // If not forced cleaning, expand the cleaning age to allows Values
        // more than 5sec old
        if( !force ) clean_to_age = Math.max(clean_to_age,now-5000);

        // No logging if under memory pressure: can deadlock the cleaner thread
        if( Log.flag(Sys.CLEAN) ) {
          String s = h+" DESIRED="+(DESIRED>>20)+"M dirtysince="+(now-dirty)+" force="+force+" clean2age="+(now-clean_to_age);
          if( MemoryManager.canAlloc() ) Log.debug(Sys.CLEAN ,s);
          else                           Log.unwrap(System.err,s);
        }
        long cleaned = 0;
        long freed = 0;

        // For faster K/V store walking get the NBHM raw backing array,
        // and walk it directly.
        Object[] kvs = STORE.raw_array();

        // Start the walk at slot 2, because slots 0,1 hold meta-data
        for( int i=2; i<kvs.length; i += 2 ) {
          // In the raw backing array, Keys and Values alternate in slots
          Object ok = kvs[i], ov = kvs[i+1];
          if( !(ok instanceof Key  ) ) continue; // Ignore tombstones and Primes and null's
          Key key = (Key )ok;
          if( !(ov instanceof Value) ) continue; // Ignore tombstones and Primes and null's
          Value val = (Value)ov;
          byte[] m = val.rawMem();
          Object p = val.rawPOJO();
          if( m == null && p == null ) continue; // Nothing to throw out

          if( val.isLockable() ) continue; // we do not want to throw out Lockables.
          boolean isChunk = p instanceof Chunk;

          // Ignore things younger than the required age.  In particular, do
          // not spill-to-disk all dirty things we find.
          long touched = val._lastAccessedTime;
          if( touched > clean_to_age ) { // Too recently touched?
            // But can toss out a byte-array if already deserialized & on disk
            // (no need for both forms).  Note no savings for Chunks, for which m==p._mem
            if( val.isPersisted() && m != null && p != null && !isChunk ) {
              val.freeMem();      // Toss serialized form, since can rebuild from POJO
              freed += val._max;
            }
            dirty_store(touched); // But may write it out later
            continue;             // Too young
          }

          // Should I write this value out to disk?
          // Should I further force it from memory?
          if( !val.isPersisted() && !diskFull && (force || (lazyPersist() && lazy_clean(key)))) {
            try {
              val.storePersist(); // Write to disk
              if( m == null ) m = val.rawMem();
              if( m != null ) cleaned += m.length;
            } catch(IOException e) {
              if( isDiskFull() )
                Log.warn(Sys.CLEAN,"Disk full! Disabling swapping to disk." + (force?" Memory low! Please free some space in " + Persist.getIce().getPath() + "!":""));
              else
                Log.warn(Sys.CLEAN,"Disk swapping failed! " + e.getMessage());
              // Something is wrong so mark disk as full anyways so we do not
              // attempt to write again.  (will retry next run when memory is low)
              diskFull = true;
            }
          }
          // And, under pressure, free all
          if( force && val.isPersisted() ) {
            val.freeMem ();  if( m != null ) freed += val._max;  m = null;
            val.freePOJO();  if( p != null ) freed += val._max;  p = null;
            if( isChunk ) freed -= val._max; // Double-counted freed mem for Chunks since val._pojo._mem & val._mem are the same.
          }
          // If we have both forms, toss the byte[] form - can be had by
          // serializing again.
          if( m != null && p != null && !isChunk ) {
            val.freeMem();
            freed += val._max;
          }
        }

        h = _myHisto.histo(true); // Force a new histogram
        MemoryManager.set_goals("postclean",false);
        // No logging if under memory pressure: can deadlock the cleaner thread
        if( Log.flag(Sys.CLEAN) ) {
          String s = h+" cleaned="+(cleaned>>20)+"M, freed="+(freed>>20)+"M, DESIRED="+(DESIRED>>20)+"M";
          if( MemoryManager.canAlloc() ) Log.debug(Sys.CLEAN ,s);
          else                           Log.unwrap(System.err,s);
        }
      }
    }

    // Rules on when to write & free a Key, when not under memory pressure.
    boolean lazy_clean( Key key ) {
      // Only data chunks are worth tossing out even lazily.
      if( !key.isChunkKey() ) // Not arraylet?
        return false; // Not enough savings to write it with mem-pressure to force us
      // If this is a chunk of a system-defined array, then assume it has
      // short lifetime, and we do not want to spin the disk writing it
      // unless we're under memory pressure.
      Key veckey = key.getVecKey();
      return veckey.user_allowed(); // Write user keys but not system keys
    }

    // Current best histogram
    static private volatile Histo H;

    // Histogram class
    public static class Histo {
      final long[] _hs = new long[128];
      long _oldest; // Time of the oldest K/V discovered this pass
      long _eldest; // Time of the eldest K/V found in some prior pass
      long _hStep; // Histogram step: (now-eldest)/histogram.length
      long _cached; // Total alive data in the histogram
      long _when; // When was this histogram computed
      Value _vold; // For assertions: record the oldest Value
      boolean _clean; // Was "clean" K/V when built?

      // Return the current best histogram
      static Histo best_histo() { return H; }

      // Return the current best histogram, recomputing in-place if it is
      // getting stale. Synchronized so the same histogram can be called into
      // here and will be only computed into one-at-a-time.
      synchronized Histo histo( boolean force ) {
        final Histo h = H; // Grab current best histogram
        if( !force && System.currentTimeMillis() < h._when+100 )
          return h; // It is recent; use it
        if( h._clean && _dirty==Long.MAX_VALUE )
          return h; // No change to the K/V store, so no point
        compute(h._oldest); // Use last oldest value for computing the next histogram in-place
        return (H = this);      // Record current best histogram & return it
      }

      // Compute a histogram
      public void compute( long eldest ) {
        Arrays.fill(_hs, 0);
        _when = System.currentTimeMillis();
        _eldest = eldest; // Eldest seen in some prior pass
        _hStep = Math.max(1,(_when-eldest)/_hs.length);
        boolean clean = _dirty==Long.MAX_VALUE;
        // Compute the hard way
        Object[] kvs = STORE.raw_array();
        long cached = 0; // Total K/V cached in ram
        long oldest = Long.MAX_VALUE; // K/V with the longest time since being touched
        Value vold = null;
        // Start the walk at slot 2, because slots 0,1 hold meta-data
        for( int i=2; i<kvs.length; i += 2 ) {
          // In the raw backing array, Keys and Values alternate in slots
          Object ok = kvs[i+0], ov = kvs[i+1];
          if( !(ok instanceof Key  ) ) continue; // Ignore tombstones and Primes and null's
          if( !(ov instanceof Value) ) continue; // Ignore tombstones and Primes and null's
          Value val = (Value)ov;
          int len = 0;
          byte[] m = val.rawMem();
          Object p = val.rawPOJO();
          if( m != null ) len += val._max;
          if( p != null ) len += val._max;
          if( p instanceof Chunk ) len -= val._max; // Do not double-count Chunks
          if( len == 0 ) continue;
          cached += len; // Accumulate total amount of cached keys

          if( val._lastAccessedTime < oldest ) { // Found an older Value?
            vold = val; // Record oldest Value seen
            oldest = val._lastAccessedTime;
          }
          // Compute histogram bucket
          int idx = (int)((val._lastAccessedTime - eldest)/_hStep);
          if( idx < 0 ) idx = 0;
          else if( idx >= _hs.length ) idx = _hs.length-1;
          _hs[idx] += len;      // Bump histogram bucket
        }
        _cached = cached; // Total cached; NOTE: larger than sum of histogram buckets
        _oldest = oldest; // Oldest seen in this pass
        _vold = vold;
        _clean = clean && _dirty==Long.MAX_VALUE; // Looks like a clean K/V the whole time?
      }

      // Compute the time (in msec) for which we need to throw out things
      // to throw out enough things to hit the desired cached memory level.
      long clean_to( long desired ) {
        long age = _eldest;     // Age of bucket zero
        if( _cached < desired ) return age; // Already there; nothing to remove
        long s = 0;             // Total amount toss out
        for( long t : _hs ) {   // For all buckets...
          s += t;               // Raise amount tossed out
          age += _hStep;        // Raise age beyond which you need to go
          if( _cached - s < desired ) break;
        }
        return age;
      }

      // Pretty print
      @Override
      public String toString() {
        long x = _eldest;
        long now = System.currentTimeMillis();
        return "H("+(_cached>>20)+"M, "+x+"ms < +"+(_oldest-x)+"ms <...{"+_hStep+"ms}...< +"+(_hStep*128)+"ms < +"+(now-x)+")";
      }
    }
  }

  // API IP Port Watchdog ---------------------------------------------------------------

  // Monitor API IP:Port for availability.
  //
  // This thread is only a watchdog.  You can comment this thread out
  // so it does not run without affecting any service functionality.
  public static class ApiIpPortWatchdogThread extends Thread {
    final private String threadName = "ApiPortWatchdog";

    private volatile boolean gracefulShutdownInitiated;         // Thread-safe.

    // Failure-tracking.
    private int consecutiveFailures;
    private long failureStartTimestampMillis;

    // Timing things that can be tuned if needed.
    final private int maxFailureSeconds = 180;
    final private int maxConsecutiveFailures = 20;
    final private int checkIntervalSeconds = 10;
    final private int timeoutSeconds = 30;
    final private int millisPerSecond = 1000;
    final private int timeoutMillis = timeoutSeconds * millisPerSecond;
    final private int sleepMillis = checkIntervalSeconds * millisPerSecond;

    // Constructor.
    public ApiIpPortWatchdogThread() {
      super("ApiWatch");        // Only 9 characters get printed in the log.
      setDaemon(true);
      setPriority(MAX_PRIORITY-2);
      reset();
      gracefulShutdownInitiated = false;
    }

    // Exit this watchdog thread.
    public void shutdown() {
      gracefulShutdownInitiated = true;
    }

    // Sleep method.
    private void mySleep(int millis) {
      try {
        Thread.sleep (sleepMillis);
      }
      catch (Exception xe)
        {}
    }

    // Print some help for the user if a failure occurs.
    private void printPossibleCauses() {
      Log.info(threadName + ": A possible cause is DHCP (e.g. changing WiFi networks)");
      Log.info(threadName + ": A possible cause is your laptop going to sleep (if running on a laptop)");
      Log.info(threadName + ": A possible cause is the network interface going down");
      Log.info(threadName + ": A possible cause is this host being overloaded");
    }

    // Reset the failure counting when a successful check() occurs.
    private void reset() {
      consecutiveFailures = 0;
      failureStartTimestampMillis = 0;
    }

    // Count the impact of one failure.
    @SuppressWarnings("unused")
    private void failed() {
      printPossibleCauses();
      if (consecutiveFailures == 0) {
        failureStartTimestampMillis = System.currentTimeMillis();
      }
      consecutiveFailures++;
    }

    // Check if enough failures have occurred or time has passed to
    // shut down this node.
    private void testForFailureShutdown() {
      if (consecutiveFailures >= maxConsecutiveFailures) {
        Log.err(threadName + ": Too many failures (>= " + maxConsecutiveFailures + "), H2O node shutting down");
        H2O.exit(1);
      }

      if (consecutiveFailures > 0) {
        final long now = System.currentTimeMillis();
        final long deltaMillis = now - failureStartTimestampMillis;
        final long thresholdMillis = (maxFailureSeconds * millisPerSecond);
        if (deltaMillis > thresholdMillis) {
          Log.err(threadName + ": Failure time threshold exceeded (>= " +
                  thresholdMillis +
                  " ms), H2O node shutting down");
          H2O.exit(1);
        }
      }
    }

    // Do the watchdog check.
    private void check() {
      final Socket s = new Socket();
      final InetSocketAddress apiIpPort = new InetSocketAddress(H2O.SELF_ADDRESS, H2O.API_PORT);
      Exception e=null;
      String msg=null;
      try {
        s.connect (apiIpPort, timeoutMillis);
        reset();
      }
      catch (SocketTimeoutException se) { e= se; msg=": Timed out"; }
      catch (IOException           ioe) { e=ioe; msg=": Failed"; }
      catch (Exception              ee) { e= ee; msg=": Failed unexpectedly"; }
      finally {
        if (gracefulShutdownInitiated) { return; }
        if( e != null ) {
          Log.err(threadName+msg+" trying to connect to REST API IP and Port (" +
                  H2O.SELF_ADDRESS + ":" + H2O.API_PORT + ", " + timeoutMillis + " ms)");
          fail();
        }
        testForFailureShutdown();
        try { s.close(); } catch (Exception xe) {}
      }
    }

    // Class main thread.
    @Override
    public void run() {
      Log.debug (threadName + ": Thread run() started");
      reset();

      while (true) {
        mySleep (sleepMillis);
        if (gracefulShutdownInitiated) { break; }
        check();
        if (gracefulShutdownInitiated) { break; }
      }
    }
  }

  /**
   * Log physical (RSS) memory usage periodically.
   * Used by developers to look for memory leaks.
   * Currently this only works for Linux.
   */
  private static class MemoryWatchdogThread extends Thread {
    final private String threadName = "MemoryWatchdog";

    private volatile boolean gracefulShutdownInitiated;         // Thread-safe.

    // Timing things that can be tuned if needed.
    final private int checkIntervalSeconds = 5;
    final private int millisPerSecond = 1000;
    final private int sleepMillis = checkIntervalSeconds * millisPerSecond;

    // Constructor.
    public MemoryWatchdogThread() {
      super("MemWatch");        // Only 9 characters get printed in the log.
      setDaemon(true);
      setPriority(MAX_PRIORITY - 2);
      gracefulShutdownInitiated = false;
    }

    // Exit this watchdog thread.
    public void shutdown() {
      gracefulShutdownInitiated = true;
    }

    // Sleep method.
    private void mySleep(int millis) {
      try {
        Thread.sleep (sleepMillis);
      }
      catch (Exception xe)
      {}
    }

    // Do the watchdog check.
    private void check() {
      water.util.LinuxProcFileReader r = new LinuxProcFileReader();
      r.read();
      long rss = -1;
      try {
        rss = r.getProcessRss();
      }
      catch (AssertionError xe) {}
      Log.info("RSS: " + rss);
    }

    // Class main thread.
    @Override
    public void run() {
      Log.debug(threadName + ": Thread run() started");

      while (true) {
        mySleep (sleepMillis);
        if (gracefulShutdownInitiated) { break; }
        check();
        if (gracefulShutdownInitiated) { break; }
      }
    }
  }

  public static class GAStartupReportThread extends Thread {
    final private String threadName = "GAStartupReport";
    final private int sleepMillis = 150 * 1000; //2.5 min

    // Constructor.
    public GAStartupReportThread() {
      super("GAStartupReport");        // Only 9 characters get printed in the log.
      setDaemon(true);
      setPriority(MAX_PRIORITY - 2);
    }

    // Class main thread.
    @Override
    public void run() {
      try {
        Thread.sleep (sleepMillis);
      }
      catch (Exception ignore) {};
      if (H2O.SELF == H2O.CLOUD._memary[0]) {
        if (OPT_ARGS.ga_hadoop_ver != null)
          H2O.GA.postAsync(new EventHit("System startup info", "Hadoop version", OPT_ARGS.ga_hadoop_ver, 1));
        H2O.GA.postAsync(new EventHit("System startup info", "Cloud", "Cloud size", CLOUD.size()));
      }
    }
  }
}
