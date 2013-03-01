package water;

/**
 * A Paxos proposal for membership
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPPaxosProposal extends UDP {
  private static final int PRINT_PORTS_COUNT = 5;

  @Override AutoBuffer call(AutoBuffer ab) {
    if( ab._h2o._heartbeat != null ) Paxos.doProposal(ab.get8(), ab._h2o);
    return ab;
  }

  static void build_and_multicast( final long proposal_num, H2ONode[] members ) {
    AutoBuffer bb = new AutoBuffer(H2O.SELF).putUdp(udp.paxos_proposal).put8(proposal_num);
    byte[] ports = new byte[PRINT_PORTS_COUNT];
    for(int i = Math.min(members.length - 1, ports.length - 1); i >= 0; i--)
      ports[i] = (byte) (members[i]._key.htm_port() % 100);
    bb.putA1(ports, ports.length);
    bb.close();
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( AutoBuffer ab ) {
    ab.getPort();
    String s = "Proposal# " + ab.get8();
    for( int i = 0; i < PRINT_PORTS_COUNT; i++ )
      s += ", " + ab.get1();
    return s;
  }
}
