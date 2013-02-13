package water;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.Notification;
import javax.management.NotificationEmitter;

/**
 * Manages memory assigned to key/value pairs. All byte arrays used in
 * keys/values should be allocated through this class - otherwise we risking
 * running out of java memory, and throw unexpected OutOfMemory errors. The
 * theory here is that *most* allocated bytes are allocated in large chunks by
 * allocating new Values - with large backing arrays. If we intercept these
 * allocation points, we cover most Java allocations. If such an allocation
 * might trigger an OOM error we first free up some other memory.
 *
 * MemoryManager monitors memory used by the K/V store (by walking through the
 * store (see Cleaner) and overall heap usage by hooking into gc.
 *
 * Memory is freed if either the cached memory is above the limit or if the
 * overall heap usage is too high (in which case we want to use less mem for
 * cache). There is also a lower limit on the amount of cache so that we never
 * delete all the cache and therefore some computation should always be able to
 * progress.
 *
 * The amount of memory to be freed is determined as the max of cached mem above
 * the limit and heap usage above the limit.
 *
 * One of the primary control inputs is FullGC cycles: we check heap usage and
 * set guidance for cache levels. We assume after a FullGC that the heap only
 * has POJOs (Plain Old Java Objects, unknown size) and K/V Cached stuff
 * (counted by us). We compute the free heap as MEM_MAX-heapUsage (after GC),
 * and we compute POJO size as (heapUsage - K/V cache usage).
 *
 * @author tomas
 * @author cliffc
 */
public abstract class MemoryManager {

  // max heap memory
  static final long MEM_MAX = Runtime.getRuntime().maxMemory();

  // Callbacks from GC
  static final HeapUsageMonitor HEAP_USAGE_MONITOR = new HeapUsageMonitor();
  static volatile long HEAP_USED_AT_LAST_GC;
  static volatile long POJO_USED_AT_LAST_GC;
  static volatile long TIME_AT_LAST_GC=System.currentTimeMillis();

  // Keep the K/V store below this threshold AND this is the FullGC call-back
  // threshold - which is limited in size to the old-gen pool size.
  static long MEM_CRITICAL = HEAP_USAGE_MONITOR._gc_callback;

  // Block allocations?
  private static volatile boolean CAN_ALLOC = true;

  // Lock for blocking on allocations
  private static Object _lock = new Object();

  // My Histogram. Called from any thread calling into the MM.
  // Singleton, allocated now so I do not allocate during an OOM event.
  static private final H2O.Cleaner.Histo myHisto = new H2O.Cleaner.Histo();


  public static void setMemGood() {
    if( CAN_ALLOC ) return;
    synchronized(_lock) {
      CAN_ALLOC = true;
      System.err.println("Continuing after swapping");
      _lock.notifyAll();
    }
  }
  public static void setMemLow() {
    if( !CAN_ALLOC ) return;
    synchronized(_lock) {
      CAN_ALLOC = false;
      System.err.println("Pausing to swap to disk; more memory may help");
    }
  }

  // Set K/V cache goals.
  // Allow (or disallow) allocations.
  // Called from the Cleaner, when "cacheUsed" has changed significantly.
  // Called from any FullGC notification, and HEAP/POJO_USED changed.
  // Called on any OOM allocation
  public static void set_goals( String msg, boolean oom ) {
    // Our best guess of free memory, as of the last GC cycle
    long freeHeap = MEM_MAX - HEAP_USED_AT_LAST_GC;
    assert freeHeap >= 0 : "I am really confused about the heap usage";
    // Current memory held in the K/V store.
    long cacheUsage = myHisto.histo(false)._cached;

    // Block allocations if:
    // the cache is > 7/8 MEM_MAX, OR
    // we cannot allocate an equal amount of POJOs, POJO_USED_AT_LAST_GC > freeHeap.
    // Decay POJOS_USED by 1/8th every 5 sec: assume we got hit with a single
    // large allocation which is not repeating - so we do not need to have
    // double the POJO amount.
    // Keep at least 1/8th heap for caching.
    // Emergency-clean the cache down to the blocking level.
    long d = MEM_CRITICAL;
    // Decay POJO amount
    long p = POJO_USED_AT_LAST_GC;
    long age = (System.currentTimeMillis() - TIME_AT_LAST_GC); // Age since last FullGC
    age = Math.min(age,10*60*1000 ); // Clip at 10mins
    while( (age-=5000) > 0 ) p = p-(p>>3); // Decay effective POJO by 1/8th every 5sec
    d -= 2*p; // Allow for the effective POJO, and again to throttle GC rate
    d = Math.max(d,MEM_MAX>>3); // Keep at least 1/8th heap
    H2O.Cleaner.DESIRED = d;

    if( cacheUsage > H2O.Cleaner.DESIRED ) {
      if( H2O.Cleaner.VERBOSE ) {
        System.out.print(CAN_ALLOC?"Blocking! ":"blocked: ");
        System.out.println(msg+", KV="+(cacheUsage>>20)+"M"+
                           ", POJO="+(POJO_USED_AT_LAST_GC>>20)+"M"+
                           ", free="+(freeHeap>>20)+"M"+
                           ", MAX="+(MEM_MAX>>20)+"M"+
                           ", DESIRED="+(H2O.Cleaner.DESIRED>>20)+"M");
      }
      setMemLow(); // Stop allocations; trigger emergency clean
      H2O.kick_store_cleaner();
    } else { // Else we are not *emergency* cleaning, but may be lazily cleaning.
      if( H2O.Cleaner.VERBOSE && !CAN_ALLOC )
        System.out.println("Unblocking: "+msg+", KV="+(cacheUsage>>20)+"M"+
                           ", POJO="+(POJO_USED_AT_LAST_GC>>20)+"M"+
                           ", free="+(freeHeap>>20)+"M"+
                           ", MAX="+(MEM_MAX>>20)+"M"+
                           ", DESIRED="+(H2O.Cleaner.DESIRED>>20)+"M");
      setMemGood();
      assert !oom; // Confused? OOM should have FullGCd should have set low-mem goals
    }
  }

  /**
   * Monitors the heap usage after full gc run and tells Cleaner to free memory
   * if mem usage is too high. Stops new allocation if mem usage is critical.
   * @author tomas
   */
  private static class HeapUsageMonitor implements javax.management.NotificationListener {
    MemoryMXBean _allMemBean = ManagementFactory.getMemoryMXBean(); // general
    MemoryPoolMXBean _oldGenBean;
    public long _gc_callback;

    HeapUsageMonitor() {
      int c = 0;
      for( MemoryPoolMXBean m : ManagementFactory.getMemoryPoolMXBeans() ) {
        if( m.getType() != MemoryType.HEAP ) // only interested in HEAP
          continue;
        if( m.isCollectionUsageThresholdSupported()
            && m.isUsageThresholdSupported()) {
          // should be Old pool, get called when memory is critical
          _oldGenBean = m;
          _gc_callback = MEM_MAX;
          // Really idiotic API: no idea what the usageThreshold is, so I have
          // to guess. Start high, catch IAE & lower by 1/8th and try again.
          while( true ) {
            try {
              m.setCollectionUsageThreshold(_gc_callback);
              break;
            } catch( IllegalArgumentException iae ) {
              _gc_callback = _gc_callback - (_gc_callback>>3);
            }
          }
          NotificationEmitter emitter = (NotificationEmitter) _allMemBean;
          emitter.addNotificationListener(this, null, m);
          ++c;
        }
      }
      assert c == 1;
    }

    /**
     * Callback routine called by JVM after full gc run. Has two functions:
     * 1) sets the amount of memory to be cleaned from the cache by the Cleaner
     * 2) sets the CAN_ALLOC flag to false if memory level is critical
     */
    public void handleNotification(Notification notification, Object handback) {
      String notifType = notification.getType();
      if( notifType.equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {
        // Memory used after this FullGC
        TIME_AT_LAST_GC = System.currentTimeMillis();
        HEAP_USED_AT_LAST_GC = _allMemBean.getHeapMemoryUsage().getUsed();
        assert HEAP_USED_AT_LAST_GC <= MEM_MAX;
        // Our best guess of POJO object usage: Heap_used minus cache used
        long cacheUsage = myHisto.histo(false)._cached;
        POJO_USED_AT_LAST_GC = HEAP_USED_AT_LAST_GC - cacheUsage;
        assert POJO_USED_AT_LAST_GC > 0;
        set_goals("FullGC",false);
      }
    }
  }


  // Allocates memory with cache management
  // Will block until there is enough available memory.
  // Catches OutOfMemory, clears cache & retries.
  public static Object malloc(int elems, long bytes, int type, byte[] orig, int from ) {
    // Do not assert on large-size here.  RF's temp internal datastructures are
    // single very large arrays.
    //assert bytes < Value.MAX : "malloc size=0x"+Long.toHexString(bytes);
    while( true ) {
      if( !CAN_ALLOC && bytes > 256 ) {
        synchronized(_lock) {
          try { _lock.wait(1000); } catch (InterruptedException ex) { }
        }
      }
      try {
        switch( type ) {
        case  1: return new byte   [elems];
        case  2: return new short  [elems];
        case  4: return new int    [elems];
        case -4: return new float  [elems];
        case -8: return new double [elems];
        case  0: return new boolean[elems];
        case -1: return Arrays.copyOfRange(orig,from,elems);
        default: throw H2O.unimpl();
        }
      }
      catch( OutOfMemoryError e ) { }
      set_goals("OOM",true); // Low memory; block for swapping
    }
  }

  // Allocates memory with cache management
  public static byte   [] malloc1 (int size) { return (byte   [])malloc(size,size*1, 1,null,0); }
  public static short  [] malloc2 (int size) { return (short  [])malloc(size,size*2, 2,null,0); }
  public static float  [] malloc4f(int size) { return (float  [])malloc(size,size*4,-4,null,0); }
  public static int    [] malloc4 (int size) { return (int    [])malloc(size,size*4, 4,null,0); }
  public static double [] malloc8d(int size) { return (double [])malloc(size,size*8,-8,null,0); }
  public static boolean[] mallocZ (int size) { return (boolean[])malloc(size,size*1, 0,null,0); }
  public static byte[] arrayCopyOfRange(byte[] orig, int from, int sz) {
    return (byte[]) malloc(sz,(sz-from),-1,orig,from);
  }
  public static byte[] arrayCopyOf( byte[] orig, int sz) {
    return arrayCopyOfRange(orig,0,sz);
  }
}
