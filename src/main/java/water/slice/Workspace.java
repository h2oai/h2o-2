package water.slice;

import water.Iced;
import water.util.Utils;

import java.util.Map;

// We intend to use workspaces to shield concurrent tasks from sharing
// and hence data in each workspace should either be read only or deep copied.
public class Workspace<K extends SliceKey, S extends Slice> extends Iced {
  /* All algo instances in this workspace */
  private Map<K, S> _slices;

  public Workspace() {
    Utils.IcedHashMap<K, S> _slices = new Utils.IcedHashMap<K, S>();
  }

  public S getSlice(K k) {
    return _slices.get(k);
  }

  public Workspace makeCopy() {
    Workspace copy = (Workspace) super.clone();
    copy._slices = new Utils.IcedHashMap();
    for (K k : _slices.keySet())
      copy._slices.put(k.makeCopy(), _slices.get(k).makeCopy());

    return copy;
  }

  public Workspace<K,S> merge(Workspace<K,S> that) {
    for (  Map.Entry<K,S> e : that._slices.entrySet() ) {
      K k = e.getKey();
      S s = _slices.get(k);
      if (s == null) _slices.put(k, e.getValue());
      else s.merge(e.getValue());
    }
    return this;
  }
}
