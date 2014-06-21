package water.util;

import water.*;

public class RemoveAllKeysTask extends DRemoteTask {
  public RemoveAllKeysTask() {}

  @Override public void lcompute() {
    int keysetSize = H2O.localKeySet().size();
    int numNodes = H2O.CLOUD._memary.length;
    int nodeIdx = H2O.SELF.index();
    Log.info("Removing "+keysetSize+" keys on this node; nodeIdx("+nodeIdx+") numNodes("+numNodes+")");

    // Now remove all keys.
    Futures fs = new Futures();
    for( Key key : H2O.localKeySet() )
      DKV.remove(key, fs);

    fs.blockForPending();

    Log.info("Keys remaining: "+H2O.store_size());

    tryComplete();
  }

  @Override public void reduce(DRemoteTask drt) {
  }

  @Override public byte priority() { return H2O.GUI_PRIORITY; }
}
