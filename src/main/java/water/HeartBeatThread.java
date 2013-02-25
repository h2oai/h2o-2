package water;
import java.io.File;
//import org.hyperic.sigar.Udp;

/**
 * Starts a thread publishing multicast HeartBeats to the local subnet: the
 * Leader of this Cloud.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class HeartBeatThread extends Thread {
  public HeartBeatThread() {
    super("Heartbeat Thread");
    setDaemon(true);
  }

  // Time between heartbeats.  Strictly several iterations less than the
  // timeout.
  static final int SLEEP = 1000;

  // Timeout in msec before we decide to not include a Node in the next round
  // of Paxos Cloud Membership voting.
  static final int TIMEOUT = 60000;

  // Timeout in msec before we decide a Node is suspect, and call for a vote
  // to remove him.  This must be strictly greater than the TIMEOUT.
  static final int SUSPECT = TIMEOUT+500;

  // Receive queue depth count before we decide a Node is suspect, and call for a vote
  // to remove him.
  static public final int QUEUEDEPTH = 100;

  // My Histogram. Called from any thread calling into the MM.
  // Singleton, allocated now so I do not allocate during an OOM event.
  static private final H2O.Cleaner.Histo myHisto = new H2O.Cleaner.Histo();

  // uniquely number heartbeats for better timelines
  static private int HB_VERSION;

  // The Run Method.
  // Started by main() on a single thread, this code publishes Cloud membership
  // to the Cloud once a second (across all members).  If anybody disagrees
  // with the membership Heartbeat, they will start a round of Paxos group
  // discovery.
  public void run() {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
    while( true ) {
      // Once per second, for the entire cloud a Node will multi-cast publish
      // itself, so other unrelated Clouds discover each other and form up.
      try { Thread.sleep(SLEEP); } // Only once-sec per entire Cloud
      catch( InterruptedException e ) { }

      // Update the interesting health self-info for publication also
      HeartBeat hb = new HeartBeat();
      hb._hb_version = HB_VERSION++;
      hb._jvm_boot_msec= TimeLine.JVM_BOOT_MSEC;
      final Runtime run = Runtime.getRuntime();
      hb.set_free_mem  (run. freeMemory());
      hb.set_max_mem   (run.  maxMemory());
      hb.set_tot_mem   (run.totalMemory());
      hb._keys       = (H2O.STORE.size ());
      hb.set_valsz     (myHisto.histo(false)._cached);
      hb._num_cpus   = (char)run.availableProcessors();
      hb._rpcs       = (char)RPC.TASKS.size();
      hb._fjthrds_hi = (char)H2O.FJP_HI  .getPoolSize();
      hb._fjthrds_lo = (char)H2O.FJP_NORM.getPoolSize();
      hb._fjqueue_hi = (char)H2O.FJP_HI  .getQueuedSubmissionCount();
      hb._fjqueue_lo = (char)H2O.FJP_NORM.getQueuedSubmissionCount();
      hb._tcps_active= (char)TCPReceiverThread.TCPS_IN_PROGRESS.get();
      // get the usable and total disk storage for the partition where the
      // persistent KV pairs are stored
      if (PersistIce.ROOT==null) {
        hb.set_free_disk(0); // not applicable
        hb.set_max_disk(0); // not applicable
      } else {
        File f = new File(PersistIce.ROOT);
        hb.set_free_disk(f.getUsableSpace());
        hb.set_max_disk(f.getTotalSpace());
      }

      // Announce what Cloud we think we are in.
      // Publish our health as well.
      H2O cloud = H2O.CLOUD;
      UDPHeartbeat.build_and_multicast(cloud, hb);

      // If we have no internet connection, then the multicast goes
      // nowhere and we never receive a heartbeat from ourselves!
      // Fake it now.
      long now = System.currentTimeMillis();
      H2O.SELF._last_heard_from = now;

      // Look for napping Nodes & propose removing from Cloud
      for( H2ONode h2o : cloud._memary ) {
        if( now - h2o._last_heard_from > SUSPECT ) {  // We suspect this Node has taken a dirt nap
          Paxos.print("hart: announce suspect node",cloud._memset,h2o.toString());
          Paxos.doChangeAnnouncement(cloud);
          break;
        }
      }
    }
  }
}
