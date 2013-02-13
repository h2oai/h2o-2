package water;

/**
 * A Paxos proposal for membership
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPPaxosProposal extends UDP {
  @Override AutoBuffer call(AutoBuffer ab) {
    if( ab._h2o._heartbeat != null ) Paxos.doProposal(ab.get8(), ab._h2o);
    return ab;
  }

  static void build_and_multicast( final long proposal_num ) {
    new AutoBuffer(H2O.SELF).putUdp(udp.paxos_proposal).put8(proposal_num).close();
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( AutoBuffer ab ) {
    ab.getPort();
    return "Proposal# "+ab.get8();
  }
}
