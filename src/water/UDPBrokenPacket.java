package water;

/**
 * An unexpected UDP packet
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class UDPBrokenPacket extends UDP {
  @Override AutoBuffer call(AutoBuffer ab) {
    throw new Error("I really should complain more about this broken packet "+ab);
  }
}

