package water;

import H2OInit.Boot;

/**
 * A UDP Heartbeat packet.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class UDPHeartbeat extends UDP {
  @Override AutoBuffer call(AutoBuffer ab) {
    ab._h2o._heartbeat = new HeartBeat().read(ab);
    Paxos.doHeartbeat(ab._h2o);
    return ab;
  }

  static void build_and_multicast( H2O cloud, HeartBeat hb ) {
    // Paxos.print_debug("send: heartbeat ",cloud._memset);
    hb._cloud_id_lo = cloud._id.getLeastSignificantBits();
    hb._cloud_id_hi = cloud._id. getMostSignificantBits();
    hb._cloud_md5 = Boot._init._jarHash;
    H2O.SELF._heartbeat = hb;
    hb.write(new AutoBuffer(H2O.SELF).putUdp(UDP.udp.heartbeat)).close();
  }
}
