package water;
import java.io.File;
import java.net.NetworkInterface;

import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.NetStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.Tcp;
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

  // The Run Method.
  // Started by main() on a single thread, this code publishes Cloud membership
  // to the Cloud once a second (across all members).  If anybody disagrees
  // with the membership Heartbeat, they will start a round of Paxos group
  // discovery.
  public void run() {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
    Sigar sigar = null;
    try { sigar = new Sigar(); } catch( Error e ) { }

    while( true ) {
      // Once per second, for the entire cloud a Node will multi-cast publish
      // itself, so other unrelated Clouds discover each other and form up.
      try { Thread.sleep(SLEEP); } // Only once-sec per entire Cloud
      catch( InterruptedException e ) { }

      // Update the interesting health self-info for publication also
      HeartBeat hb = new HeartBeat();
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

      // Disable collecting Sigar-based statistics if the command line contains --nosigar
      if( sigar != null && H2O.OPT_ARGS.nosigar == null) { // --nosigar is not specified on command line

        // get cpu utilization from sigar if available
        try {
          hb.set_cpu_util(1.0 - sigar.getCpuPerc().getIdle());
        } catch (SigarException ex) {
          hb.set_cpu_util(-1.0);
        }

        // get cpu load from sigar if available
        try {
          double [] cpu_load = sigar.getLoadAverage();
          hb.set_cpu_load(cpu_load[0],cpu_load[1],cpu_load[2]);
        } catch (SigarException ex) {
          hb.set_cpu_load(-1.0,-1.0,-1.0);
        }
        // Get network statistics from sigar
        fillNetworkStatistics(sigar, hb);
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

  // Last value of received bytes on the interface.
  long _last_rx_bytes = -1;
  // Last value of transmitted bytes on the interface.
  long _last_tx_bytes = -1;
  // Sum of received bytes on the interface.
  long _sum_rx_bytes;
  // Sum of transmitted bytes on the interface.
  long _sum_tx_bytes;
  // Time of last collection.
  long _last_stat_collection_time;

  // Prepare network statistics with help of Sigar.
  private void fillNetworkStatistics(final Sigar sigar, final HeartBeat hb) {
    // Setup number of IN and OUT connections.
    try {
      final NetStat netStats = sigar.getNetStat();
      hb._total_in_conn = (netStats.getAllInboundTotal());
      hb._total_out_conn= (netStats.getAllOutboundTotal());
      hb._tcp_in_conn   = (netStats.getTcpInboundTotal());
      hb._tcp_out_conn  = (netStats.getTcpOutboundTotal());
      //hb.set_udp_in_conn(netStats.getUdpInboundTotal());
      //hb.set_udp_out_conn(netStats.getUdpOutboundTotal());
    } catch (SigarException e) {
      hb._total_in_conn = -1;
      hb._total_out_conn = -1;
      hb._tcp_in_conn = -1;
      hb._tcp_out_conn = -1;
      hb._udp_in_conn = -1;
      hb._udp_out_conn = -1;
    }

    // Setup overall statistics of a network interface.
    // Total transmitted bytes are computed for network interface which the node
    // utilizes for connecting to the cloud.
    try {
      // Get interface to which the node IP address is bound to.
      // It is not fully correct since IP can be bound to multiple networks.
      // However, the method NetworkInterface.getByInetAddress returns one of the interfaces.
      // Another possibility is to compute traffic over all interfaces.
      final String netIfaceName = NetworkInterface.getByInetAddress(H2O.SELF._key.getAddress()).getName();
      // Get interface statistics.
      final NetInterfaceStat netInterfaceStat = sigar.getNetInterfaceStat(netIfaceName);

      final long _rx_bytes = netInterfaceStat.getRxBytes();
      final long _tx_bytes = netInterfaceStat.getTxBytes();
      final long _delta_rx_bytes = _last_rx_bytes < 0 ? 0 : _rx_bytes - _last_rx_bytes;
      final long _delta_tx_bytes = _last_tx_bytes < 0 ? 0 : _tx_bytes - _last_tx_bytes;

      _last_rx_bytes = _rx_bytes;
      _last_tx_bytes = _tx_bytes;
      _sum_rx_bytes += _delta_rx_bytes;
      _sum_tx_bytes += _delta_tx_bytes;

      // Compute traffic rate -- the rate is computed
      // Based on user preferences, this rate can be replaced by averaged traffic rate.
      final long _now = System.currentTimeMillis();
      final long _rx_bytes_rate = 1000 * _delta_rx_bytes / (_now - _last_stat_collection_time);
      final long _tx_bytes_rate = 1000 * _delta_tx_bytes / (_now - _last_stat_collection_time);
      _last_stat_collection_time = _now;

      // Setup overall traffic statistics.
      // TODO: decide if it is better to show total number of packets/bytes reported by Sigar
      // or compute their sum manually.
      hb._total_packets_recv = (netInterfaceStat.getRxPackets());
      hb._total_packets_sent = (netInterfaceStat.getTxPackets());
      hb._total_bytes_recv = (_rx_bytes);
      hb._total_bytes_sent = (_tx_bytes);
      hb._total_bytes_recv_rate = ((int) _rx_bytes_rate);
      hb._total_bytes_sent_rate = ((int) _tx_bytes_rate);

    } catch (Exception e) {
      hb._total_packets_recv = -1;
      hb._total_packets_sent = -1;
      hb._total_bytes_recv = -1;
      hb._total_bytes_sent = -1;
      hb._total_bytes_recv_rate = -1;
      hb._total_bytes_recv_rate = -1;
    }

    // Setup TCP statistics.
    try {
      final Tcp tcpStats = sigar.getTcp();
      hb._tcp_packets_recv = (tcpStats.getInSegs());
      hb._tcp_packets_sent = (tcpStats.getOutSegs());
      hb._tcp_bytes_recv = -1;
      hb._tcp_bytes_sent = -1;
    } catch (SigarException e) {
      hb._tcp_packets_recv = -1;
      hb._tcp_packets_sent = -1;
      hb._tcp_bytes_recv = -1;
      hb._tcp_bytes_sent = -1;
    }

    // Setup UDP statistics.
    hb._udp_packets_recv = -1;
    hb._udp_packets_sent = -1;
    hb._udp_bytes_recv = -1;
    hb._udp_bytes_sent = -1;
  }
}
