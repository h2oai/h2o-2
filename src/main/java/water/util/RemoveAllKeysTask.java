package water.util;

import water.DRemoteTask;
import water.H2O;
import water.Key;
import water.UKV;
import java.io.IOException;

public class RemoveAllKeysTask extends DRemoteTask {
  public RemoveAllKeysTask() {}

  @Override public void lcompute() {
    try {
      int numnodes = H2O.CLOUD._memary.length;
      int idx = H2O.SELF.index();

      Log.info("Removing "+H2O.keySet().size()+" keys on this node; nodeIdx("+idx+") numNodes("+numnodes+")");
      final int numKeys = H2O.keySet().size();
      Key[] keys = new Key[numKeys];
      int len = 0;
      //Loop over keys
      for( Key key : H2O.keySet() ) {
        if( H2O.get(key) == null ) continue;
        keys[len++] = key;
        if( len == keys.length ) break;
      }
      //remove keys
      UKV.removeAll(keys);
      assert H2O.keySet().size() <= 1; //1 null key left over
    }
    catch (Exception e) {
      Log.err("RemoveAllKeysTask failed with the following exception");
      Log.err(e);
    }
    finally {
      tryComplete();
    }
  }

  @Override public void reduce(DRemoteTask drt) {
  }

  @Override public byte priority() { return H2O.GUI_PRIORITY; }
}
