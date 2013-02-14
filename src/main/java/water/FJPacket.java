package water;
import jsr166y.*;

/**
 * A class to handle the work of a received UDP packet.  Typically we'll do a
 * small amount of work based on the packet contents (such as returning a Value
 * requested by another Node, or recording a heartbeat).
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class FJPacket extends CountedCompleter {
  final AutoBuffer _ab;
  FJPacket( AutoBuffer ab ) { _ab = ab; }

  public void compute() {
    int ctrl = _ab.getCtrl();
    _ab.getPort(); // skip past the port
    UDP.udp.UDPS[ctrl]._udp.call(_ab).close();
    tryComplete();
  }

  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
    ex.printStackTrace();
    return true;
  }
}
