package water;

import H2OInit.Boot;

import java.util.Arrays;

/**
 * Struct holding H2ONode health info.
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
public class HeartBeat extends Iced {
  public long _cloud_id_lo, _cloud_id_hi; // UUID for this Cloud
  public byte[] _cloud_md5;
  public char _num_cpus;        // Number of CPUs for this Node, limit of 65535

  // Scaled by K or by M setters & getters.
  private int _free_mem;         // Free memory in K (goes up and down with GC)
  public void set_free_mem (long n) {  _free_mem = (int)(n>>10); }
  public long get_free_mem ()  { return ((long) _free_mem)<<10 ; }
  int _tot_mem;          // Total memory in K (should track virtual mem?)
  public void set_tot_mem  (long n) {   _tot_mem = (int)(n>>10); }
  public long get_tot_mem  ()  { return ((long)  _tot_mem)<<10 ; }
  int _max_mem;          // Max memory in K (max mem limit for JVM)
  public void set_max_mem  (long n) {   _max_mem = (int)(n>>10); }
  public long get_max_mem  ()  { return ((long)  _max_mem)<<10 ; }
  public int _keys;      // Number of LOCAL keys in this node, cached or homed
  int _valsz;            // Sum of value bytes used, in K
  public void set_valsz(long n) { _valsz = (int)(n>>10); }
  public long get_valsz()  { return ((long)_valsz)<<10 ; }
  int _free_disk;        // Free disk (internally stored in megabyte precision)
  public void set_free_disk(long n) { _free_disk = (int)(n>>20); }
  public long get_free_disk()  { return ((long)_free_disk)<<20 ; }
  int _max_disk;         // Disk size (internally stored in megabyte precision)
  public void set_max_disk (long n) {  _max_disk = (int)(n>>20); }
  public long get_max_disk ()  { return  ((long)_max_disk)<<20 ; }

  // -1 for no-info, or cpu load/util as 1000*scaled
  char _cpu_util;        // CPU utilization, from 0 to 1
  public void set_cpu_util (double d) { _cpu_util = (char)(d < 0 ? 0xFFFF : ((long)(1000*d))&0xFFFF); }
  public double get_cpu_util () { return _cpu_util == 0xFFFFL ? -1.0 : _cpu_util/1000.0; }
  char _cpu_load_1;      // CPU load over last 1 minute
  char _cpu_load_5;      // CPU load over last 5 minutes
  char _cpu_load_15;     // CPU load over last 15 minutes
  public void set_cpu_load (double oneMinute, double fiveMinutes, double fifteenMinutes) {
    _cpu_load_1 =(char)(    oneMinute  >= 0 ? ((long)(1000*    oneMinute )) & 0xFFFF : 0xFFFF);
    _cpu_load_5 =(char)(   fiveMinutes >= 0 ? ((long)(1000*   fiveMinutes)) & 0xFFFF : 0xFFFF);
    _cpu_load_15=(char)(fifteenMinutes >= 0 ? ((long)(1000*fifteenMinutes)) & 0xFFFF : 0xFFFF);
  }
  public double get_cpu_load1() { return _cpu_load_1 == 0xFFFF ? -1.0 : _cpu_load_1 /1000.0; }
  public double get_cpu_load5() { return _cpu_load_5 == 0xFFFF ? -1.0 : _cpu_load_5 /1000.0; }
  public double get_cpu_load15(){ return _cpu_load_15== 0xFFFF ? -1.0 : _cpu_load_15/1000.0; }

  public boolean check_cloud_md5() {
    return Arrays.equals(Boot._init._jarHash, _cloud_md5) || true;
  }

  public char _rpcs;            // Outstanding DFutureTasks
  public char _fjthrds_hi;      // Number of threads (not all are runnable)
  public char _fjthrds_lo;      // Number of threads (not all are runnable)
  public char _fjqueue_hi;      // Number of elements in FJ work queue
  public char _fjqueue_lo;      // Number of elements in FJ work queue
  public char _tcps_active;     // Threads trying do a TCP send
  public int _total_in_conn;    // Total number of IN connections
  public int _total_out_conn;   // Total number of OUT connections
  public int _tcp_in_conn;      // Total number of TCP IN connections
  public int _tcp_out_conn;     // Total number of TCP OUT connections
  public int _udp_in_conn; // Total number of UDP IN  "connections" (i.e, opened server UDP socket)
  public int _udp_out_conn; // Total number of UDP OUT "connections" (i.e, opened client UDP socket)
  public int _total_bytes_recv_rate; // Incoming traffic rate
  public int _total_bytes_sent_rate; // Outgoing traffic rate
  public long _total_packets_recv;   // Total packets received
  public long _total_packets_sent;   // Total packets sent
  public long _total_bytes_recv;     // Total bytes received
  public long _total_bytes_sent;     // Total bytes sent
  public long _tcp_packets_recv;     // TCP segments received
  public long _tcp_packets_sent;     // TCP segments sent
  public long _tcp_bytes_recv;       // TCP bytes received
  public long _tcp_bytes_sent;       // TCP bytes sent
  public long _udp_packets_recv;     // UDP packets received
  public long _udp_packets_sent;     // UDP packets sent
  public long _udp_bytes_recv;       // UDP packets received
  public long _udp_bytes_sent;       // UDP packets sent
}
