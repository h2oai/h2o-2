package water;

/**
 * An unexpected UDP packet
 *
 * @author <a href="mailto:cliffc@h2o.ai"></a>
 * @version 1.0
 */
public class UDPBrokenPacket extends UDP {
  @Override AutoBuffer call(AutoBuffer ab) {
    throw new RuntimeException("I really should complain more about this broken packet "+ab);
  }
}

