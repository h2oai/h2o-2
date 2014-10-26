package water.fvec;

import hex.Interaction;
import jsr166y.CountedCompleter;
import water.*;
import water.util.Log;
import water.util.Utils;
import static water.util.Utils.IcedLong;

import java.util.*;

/**
 * Helper to create interaction features between enum columns
 */
public class CreateInteractions extends H2O.H2OCountedCompleter {

  public CreateInteractions(Interaction ci) { this(ci, null); }
  public CreateInteractions(Interaction ci, Key job) { super(null); _job = job; _ci = ci; }

  final private Interaction _ci;

  static final private int _missing = Integer.MIN_VALUE; //marker for missing factor level
  static final private String _other = "other"; // name for lost factor levels

  private Frame _out;
  final private Key _job;

  private Map<Long, Long> _sortedMap = null;

  private static Map<Long, Long> mySort(Map<IcedLong, IcedLong> unsortMap) {
    List<Map.Entry<IcedLong, IcedLong>> list = new LinkedList<Map.Entry<IcedLong, IcedLong>>(unsortMap.entrySet());
    // Sorting the list based on values
    Collections.sort(list, new Comparator<Map.Entry<IcedLong, IcedLong>>() {
      public int compare(Map.Entry<IcedLong, IcedLong> o1, Map.Entry<IcedLong, IcedLong> o2) {
        return ((Long)o2.getValue()._val).compareTo(o1.getValue()._val);
      }
    });
    // Maintaining insertion order with the help of LinkedList
    Map sortedMap = new LinkedHashMap<Long, Long>();
    for (Map.Entry<IcedLong, IcedLong> entry : list) {
      sortedMap.put(entry.getKey()._val, entry.getValue()._val);
    }
    return sortedMap;
  }

  protected String[] makeDomain(Map<IcedLong, IcedLong> unsortedMap, int _i, int _j) {
    String[] _domain;
    Frame _fr = _out;
//    Log.info("Collected hash table");
//    Log.info(java.util.Arrays.deepToString(unsortedMap.entrySet().toArray()));

//    Log.info("Interaction between " + _fr.domains()[_i].length + " and " + _fr.domains()[_j].length + " factor levels => " +
//            ((long)_fr.domains()[_i].length * (long)_fr.domains()[_j].length) + " possible factors.");

    _sortedMap = mySort(unsortedMap);

    // create domain of the most frequent unique factors
    long factorCount = 0;
//    Log.info("Found " + _sortedMap.size() + " unique interaction factors (out of " + ((long)_fr.domains()[_i].length * (long)_fr.domains()[_j].length) + ").");
    _domain = new String[_sortedMap.size()]; //TODO: use ArrayList here, then convert to array
    Iterator it2 = _sortedMap.entrySet().iterator();
    int d = 0;
    while (it2.hasNext()) {
      Map.Entry kv = (Map.Entry)it2.next();
      final long ab = (Long)kv.getKey();
      final long count = (Long)kv.getValue();
      if (factorCount < _ci.max_factors && count >= _ci.min_occurrence) {
        factorCount++;
        // extract the two original factor enums
        String feature = "";
        if (_j != _i) {
          int a = (int)(ab >> 32);
          final String fA = a != _missing ? _fr.domains()[_i][a] : "NA";
          feature = fA + "_";
        }
        int b = (int) ab;
        String fB = b != _missing ? _fr.domains()[_j][b] : "NA";
        feature += fB;

//        Log.info("Adding interaction feature " + feature + ", occurrence count: " + count);
//        Log.info("Total number of interaction factors so far: " + factorCount);
        _domain[d++] = feature;
      } else break;
    }
    if (d < _sortedMap.size()) {
//      Log.info("Truncated map to " + _sortedMap.size() + " elements.");
      String[] copy = new String[d+1];
      System.arraycopy(_domain, 0, copy, 0, d);
      copy[d] = _other;
      _domain = copy;

      Map tm = new LinkedHashMap<Long, Long>();
      it2 = _sortedMap.entrySet().iterator();
      while (--d >= 0) {
        Map.Entry kv = (Map.Entry) it2.next();
        tm.put(kv.getKey(), kv.getValue());
      }
      _sortedMap = tm;
    }
//    Log.info("Created domain: " + Arrays.deepToString(_domain));
    return _domain;
  }


  @Override
  public void compute2() {
    // base frame - same as source
    DKV.remove(Key.make(_ci.target));//shouldn't be needed, but this avoids missing chunk issues
    _out = new Frame(Key.make(_ci.target), _ci.source.names().clone(), _ci.source.vecs().clone());
    _out.delete_and_lock(_job);

    int idx1 = _ci.factors[0];
    Vec tmp = null;
    int start = _ci.factors.length == 1 ? 0 : 1;
    for (int i=start; i<_ci.factors.length; ++i) {
      if (i>1) {
        idx1 = _out.find(tmp);
        assert idx1 >= 0;
      }
      int idx2 = _ci.factors[i];
//      Log.info("Combining columns " + idx1 + " and " + idx2);

      // Pass 1: compute unique domains of all interaction features
      createInteractionDomain pass1 = new createInteractionDomain(idx1, idx2).doAll(_out);

      // Create a new Vec based on the domain
      final String name = _out._names[idx1] + "_" + _out._names[idx2];
      final Vec vec = _out.anyVec().makeZero(makeDomain(pass1._unsortedMap, idx1, idx2));
      _out.add(name, vec);
      _out.update(_job);

      // Create array of enum pairs, in the same (sorted) order as in the _domain map -> for linear lookup
      // Note: "other" is not mapped in keys, so keys.length can be 1 less than domain.length
      long[] keys = new long[_sortedMap.size()];
      int pos = 0;
      for (long k : _sortedMap.keySet()) {
        keys[pos++] = k;
      }
      assert(_out.lastVec().domain().length == keys.length || _out.lastVec().domain().length == keys.length + 1); // domain might contain _other

      // Pass 2: fill Vec values
      new fillInteractionEnums(idx1, idx2, keys).doAll(_out);
      tmp = _out.lastVec();

      // remove temporary vec
      if (i>1) {
        final int idx = _out.vecs().length-2; //second-last vec
//        Log.info("Removing column " + _out._names[idx]);
        _out.remove(idx);
        _out.update(_job);
      }
    }
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
  final private int _i;
  final private int _j;

  // OUTPUT
  private Utils.IcedHashMap<IcedLong, IcedLong> _unsortedMap = null;

  public createInteractionDomain(int i, int j) { _i = i; _j = j; }

  @Override
  public void map(Chunk[] cs) {
    _unsortedMap = new Utils.IcedHashMap<IcedLong, IcedLong>();
    // find unique interaction domain
    for (int r = 0; r < cs[0]._len; r++) {
      int a = cs[_i].isNA0(r) ? _missing : (int)cs[_i].at80(r);
      long ab;
      if (_j != _i) {
        int b = cs[_j].isNA0(r) ? _missing : (int) cs[_j].at80(r);

        // key: combine both ints into a long
        ab = ((long) a << 32) | (b & 0xFFFFFFFFL);
        assert a == (int) (ab >> 32);
        assert b == (int) ab;
      } else {
        if (a == _missing) continue;
        ab = (long)a;
      }

      // add key to hash map, and count occurrences (for pruning)
      IcedLong AB = new IcedLong(ab);
      if (_unsortedMap.containsKey(AB)) {
        _unsortedMap.put(AB, new IcedLong(_unsortedMap.get(AB)._val + 1));
      } else {
        _unsortedMap.put(AB, new IcedLong(1));
      }
    }
  }

  @Override
  public void reduce(createInteractionDomain mrt) {
    assert(mrt._unsortedMap != null);
    assert(_unsortedMap != null);
    _unsortedMap.putAll(mrt._unsortedMap);
    mrt._unsortedMap = null;
//    Log.info("Merged hash tables");
//    Log.info(java.util.Arrays.deepToString(_unsortedMap.entrySet().toArray()));
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
        final int a = cs[_i].isNA0(r) ? _missing : (int)cs[_i].at80(r);
        long ab;
        if (_j != _i) {
          final int b = cs[_j].isNA0(r) ? _missing : (int) cs[_j].at80(r);
          ab = ((long) a << 32) | (b & 0xFFFFFFFFL); // key: combine both ints into a long
        } else {
          ab = (long)a;
        }

        if (_i == _j && cs[_i].isNA0(r)) {
          cs[cs.length - 1].setNA0(r);
        } else {
          // linear search in sorted array of factor levels (descending by occurrence), should be fastest for most small domains
          int level = -1;
          for (int i = 0; i < _keys.length; ++i) {
            if (ab == _keys[i]) {
              level = i;
              break;
            }
          }
          if (level == -1) {
            level = _fr.lastVec().domain().length-1;
            assert _fr.lastVec().domain()[level] == _other;
          }
          cs[cs.length - 1].set0(r, level);
        }
      }
    }

  }
}
