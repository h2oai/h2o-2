package water;

/**
 * A Paxos packet: an AcceptRequest packet
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class UDPPaxosAccept extends UDP {
  @Override AutoBuffer call(AutoBuffer ab) {
    Paxos.State s = ab.get(Paxos.State.class);
    Paxos.doAccept(s, ab._h2o);
    return ab;
  }

  // Build an AcceptRequest packet.  It is our PROPOSAL with the 8 bytes of
  // Accept number set, plus the Value (wireline member protocol)
  static void build_and_multicast( Paxos.State state ) {
    new AutoBuffer(H2O.SELF).putUdp(udp.paxos_accept).put(state).close();
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( AutoBuffer ab ) {
    ab.getPort();               // 3 bytes, 16-3 == 13 left
    // We sent a 'state' object; break down the next 13 bytes of it.
    int  typeMap = ab.get2();     // 2 byte , 13-2 == 11 left; TypeMap for 'Paxos.state'
    long promise = ab.get8();     // 8 bytes, 11-8 ==  3 left
    long oldProposal = ab.get3(); // 3 bytes,  3-3 == All gone!
    return "Promise# "+promise+" _oldProp# "+oldProposal;
  }
}
