package water;

import water.DTask;
import water.nbhm.NonBlockingHashMap;

/**
 * Send a Key from its home node to some remote node via a "push"
 *
 * @author <a href="mailto:cliffc@h2o.ai"></a>
 * @version 1.0
 */

public class TaskSendKey extends DTask<TaskSendKey> {
  Key _key;                  // Set by client/sender JVM, cleared by server JVM
  final int _max;
  final short _type;
  final byte _be;

  protected TaskSendKey( Key key, Value val ) { _key = key; _max = val._max; _type = (short)val.type(); _be = val.backend(); }

  @Override public void dinvoke( H2ONode sender ) {
    assert !_key.home();        // No point in sending Keys to home
    // Update ONLY if there is not something there already.
    // Update only a bare Value, with no backing data.
    // Real data can be fetched on demand.
    Value val = new Value(_key,_max,null,_type,_be);
    Value old = H2O.raw_get(_key);
    while( old == null && H2O.putIfMatch(_key,val,null) != null )
      old = H2O.raw_get(_key);
    _key = null;                // No return result
    tryComplete();
  }  
  @Override public void compute2() { throw H2O.unimpl(); }
  @Override public byte priority() { return H2O.GUI_PRIORITY; }
}
