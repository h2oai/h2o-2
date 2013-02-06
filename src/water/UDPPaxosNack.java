package water;

/**
 * A Paxos packet: a Nack packet
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class UDPPaxosNack extends UDP {
  @Override AutoBuffer call(AutoBuffer ab) {
    if( ab._h2o._heartbeat != null ) Paxos.doNack(ab.get8(), ab._h2o);
    return ab;
  }

  static void build_and_multicast( long proposal, H2ONode proposer ) {
    new AutoBuffer(H2O.SELF).putUdp(udp.paxos_nack).put8(proposal).close();
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( AutoBuffer ab ) {
    ab.getPort();
    return "Proposal# "+ab.get8();
  }
}
