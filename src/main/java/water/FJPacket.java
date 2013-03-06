package water;
import water.H2O.H2OCountedCompleter;

/**
 * A class to handle the work of a received UDP packet.  Typically we'll do a
 * small amount of work based on the packet contents (such as returning a Value
 * requested by another Node, or recording a heartbeat).
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class FJPacket extends H2OCountedCompleter {
  final AutoBuffer _ab;
  FJPacket( AutoBuffer ab ) { _ab = ab; }

  public void compute2() {
    int ctrl = _ab.getCtrl();
    _ab.getPort(); // skip past the port
    if(ctrl <= UDP.udp.ack.ordinal())
      UDP.udp.UDPS[ctrl]._udp.call(_ab).close();
    else
      RPC.remote_exec(_ab);
    tryComplete();
  }
  @Override
  public int priority() {
    return RPC.MAX_PRIORITY-1;
  }
}
