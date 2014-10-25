package water.fvec;

import hex.Interaction;
import jsr166y.CountedCompleter;
import water.*;
import water.util.Log;
import water.util.Utils;

import java.util.*;

/**
 * Helper to create interaction features between enum columns
 */
public class createInteractions extends H2O.H2OCountedCompleter {

  public createInteractions(Interaction ci) { this(ci, null); }
  public createInteractions(Interaction ci, Key job) { super(null); _job = job; _ci = ci; }

  final private Interaction _ci;

  private Frame _out;
  final private Key _job;

  @Override
  public void compute2() {
    // base frame - same as source
    _out = new Frame(Key.make(_ci.target), _ci.source.names(), _ci.source.vecs());

//    for (int i=1; i<_ci.factors.length; ++i) {
      int idx1 = _ci.factors[0];
      int idx2 = _ci.factors[1];

      // Pass 1: compute unique domains of all interaction features
      createInteractionDomain pass1 = new createInteractionDomain(_ci, idx1, idx2).doAll(_out);
      String[] domain = pass1._domain;

      // Create a new Vec based on the domain
      Vec interaction = _out.anyVec().makeZero(domain);
      String name = _ci.source._names[idx1] + "_" + _ci.source._names[idx2];
      _out.add(name, interaction);

      // Create array of enum pairs, in the same (sorted) order as in the _domain map -> for linear lookup
      long[] keys = new long[pass1._tm.size()];
      int pos = 0;
      for (long k : pass1._tm.keySet()) {
        keys[pos++] = k;
      }

      // Pass 2: fill Vec values
      new fillInteractionEnums(idx1, idx2, keys).doAll(_out);
//    }

    _out.delete_and_lock(_job);
    tryComplete();
  }

  @Override
  public void onCompletion(CountedCompleter caller) {
    _out.update(_job);
    _out.unlock(_job);
  }




// Create interaction domain
private static class createInteractionDomain extends MRTask2<createInteractionDomain> {
  // INPUT
  final private Interaction _ci;
  final private int _i;
  final private int _j;

  // OUTPUT
  public String[] _domain;

  // Helper
  private Utils.IcedHashMap<Utils.IcedLong, Utils.IcedLong> _hm = null;
  transient private TreeMap<Long, Long> _tm = null;


  public createInteractionDomain(Interaction ci, int i, int j) {
    _ci = ci; _i = i; _j = j;
    Log.info("Creating interaction features between " + _ci.source._names[_i] + " and " + _ci.source._names[_j]);
  }

  @Override
  protected void postGlobal() {
//    Log.info("Collected hash table");
//    Log.info(java.util.Arrays.deepToString(_hm.entrySet().toArray()));

    Log.info("Combining domains with " +
            _fr.domains()[_i].length + " x " + _fr.domains()[_j].length + " = " +
            (_fr.domains()[_i].length * _fr.domains()[_j].length) + " factors");

    // sort hash map
    _tm = new TreeMap(Collections.reverseOrder());
    Iterator it = _hm.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry kv = (Map.Entry) it.next();
      long ab = ((Utils.IcedLong) kv.getKey())._val;
      long count = ((Utils.IcedLong) kv.getValue())._val;
      _tm.put(ab, count);
    }

    // create domain of the most frequent unique factors
    long factorCount = 0;
    Log.info("Found " + _hm.size() + " unique interaction factors.");
    _domain = new String[_hm.size()];
    Iterator it2 = _tm.entrySet().iterator();
    int d = 0;
    while (it2.hasNext()) {
      Map.Entry kv = (Map.Entry)it2.next();
      long ab = (Long)kv.getKey();
      long count = (Long)kv.getValue();
      if (factorCount < _ci.max_factors) {
        factorCount++;
        // extract the two original factor enums
        int a = (int)(ab >> 32);
        int b = (int)ab;
        String feature = _fr.domains()[_i][a] + "_" + _fr.domains()[_j][b];
//        Log.info("Adding interaction feature " + feature + ", occurrence count: " + count);
//        Log.info("Total number of interaction factors so far: " + factorCount);
        _domain[d++] = feature;
      } else break;
    }
    if (d < _domain.length) {
      String[] copy = new String[d];
      System.arraycopy(_domain, 0, copy, 0, d);
      _domain = copy;

      TreeMap tm = new TreeMap();
      it2 = _tm.entrySet().iterator();
      while (--d >= 0) {
        Map.Entry kv = (Map.Entry) it2.next();
        tm.put(kv.getKey(), kv.getValue());
      }
      _tm = tm;
//      Log.info("Truncated tree map to " + _tm.size() + " elements.");
    }
//    Log.info("Final domain: " + Arrays.deepToString(_domain));
  }


  @Override
  public void map(Chunk[] cs) {
    _hm = new Utils.IcedHashMap<Utils.IcedLong, Utils.IcedLong>();
    // find unique interaction domain
    for (int r = 0; r < cs[0]._len; r++) {
      long a = cs[_i].isNA0(r) ? Integer.MIN_VALUE : cs[_i].at80(r);
      long b = cs[_j].isNA0(r) ? Integer.MIN_VALUE : cs[_j].at80(r);

      // enum levels must fit into int
      assert((int)a == a);
      assert((int)b == b);

      // key: combine both ints into a long
      long ab = (a << 32) | (b & 0xFFFFFFFL);

      // add key to hash map, and count occurrences (for pruning)
      Utils.IcedLong AB = new Utils.IcedLong(ab);
      if (_hm.containsKey(AB)) {
        _hm.put(AB, new Utils.IcedLong(_hm.get(AB)._val + 1));
      } else {
        _hm.put(AB, new Utils.IcedLong(1));
      }
    }
  }

  @Override
  public void reduce(createInteractionDomain mrt) {
    // get all entries from mrt._hm, and put them into this._hm
    mrt._hm.putAll(_hm);
    mrt._hm.clear();
//    Log.info("Merged hash tables");
//    Log.info(java.util.Arrays.deepToString(_hm.entrySet().toArray()));
  }
}

  // Fill interaction enums in last Vec in Frame
  private static class fillInteractionEnums extends MRTask2<fillInteractionEnums> {
    // INPUT
    final private int _i;
    final private int _j;
    final long[] _keys; //mapping of combined long to index in _domain, for linear search

    public fillInteractionEnums(int i, int j, long[] keys) {
      _i = i; _j = j; _keys = keys;
    }

    @Override
    public void map(Chunk[] cs) {
      // find unique interaction domain
      for (int r = 0; r < cs[0]._len; r++) {
        final long a = cs[_i].isNA0(r) ? Integer.MIN_VALUE : cs[_i].at80(r);
        final long b = cs[_j].isNA0(r) ? Integer.MIN_VALUE : cs[_j].at80(r);

        // enum levels must fit into int
        assert((int)a == a);
        assert((int)b == b);

        // key: combine both ints into a long
        final long ab = (a << 32) | (b & 0xFFFFFFFL);

//        Log.info("Looking up position of " + ab + " in domain");
//        Log.info("This long should be in the keys:");
//        Log.info(Arrays.toString(_keys));

        // linear search in sorted array of factor levels (descending by occurrence), should be fastest for most small domains
        int level = -1;
        for (int i = 0; i < _keys.length; ++i) {
          if (ab == _keys[i]) {
            level = i;
            break;
          }
        }

        if (level >= 0)
          cs[cs.length-1].set0(r, level);
        else
          cs[cs.length-1].setNA0(r);

      }
//    Log.info("Collected hash table");
//    Log.info(java.util.Arrays.deepToString(_hm.entrySet().toArray()));
    }

  }
}
