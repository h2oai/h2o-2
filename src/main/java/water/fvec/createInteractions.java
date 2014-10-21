package water.fvec;

import hex.Interaction;
import jsr166y.CountedCompleter;
import water.H2O;
import water.Key;
import water.MRTask2;
import water.util.Log;
import static water.util.Utils.domainUnion;

import java.util.HashMap;
import java.util.Random;

/**
 * Helper to create interaction features between enum columns
 */
public class createInteractions extends H2O.H2OCountedCompleter {

  public createInteractions(Interaction ci) { this(ci, null); }

  public createInteractions(Interaction ci, Key job) {
    super(null);
    _job=job;
    _ci = ci;
  }
  final private Interaction _ci;

  private Frame _out;
  final private Key _job;

  @Override public void compute2() {

    // base frame - same as source
    _out = new Frame(Key.make(_ci.target), _ci.source.names(), _ci.source.vecs());

    String[] union = null;
    int A = _ci.factors[0];
    String[] dA = _out.vecs()[A]._domain;
    int len = dA.length;
    String name = _out._names[A];

    for( int i=1; i< _ci.factors.length; i++ ) {
      int B = _ci.factors[i];
      String[] dB = _out.vecs()[B]._domain;
      name += _out._names[B];

      len *= dB.length;
      if (len > water.parser.Enum.MAX_ENUM_SIZE) {
        Log.info("Skipping interaction term " + name + ": too many factors.");
        continue;
      }

        int count = 0;
        for (String sa : dA)
          for (String sb : dB)
            union[count++] = sa + "." + sb;
        Vec nv = _out.anyVec().makeZero(union);
        _out.add(name, nv);
//        new populateInteractionVec(_ci, i, j).doAll(_out);
      }
    }
    _out.delete_and_lock(_job);
    tryComplete();

    // Needs to create _domain and enum chunk values
  }

  @Override public void onCompletion(CountedCompleter caller){
    _out.update(_job);
    _out.unlock(_job);
  }

  // Create domain
  private static class createInteractionDomain extends MRTask2<createInteractionDomain> {
    // INPUT
    final private Interaction _ci;
    private final  HashMap<String, Integer> _hm = new HashMap<String, Integer>();

//    private NonBlockingHashMap<Long,Long> _nbhm;
    int _i;
    int _j;

    // OUTPUT
    public String[] _domain;

    public createInteractionDomain(Interaction ci, int i, int j){
      _i = i;
      _j = j;
      _ci = ci;
      _domain = new String[]{"yes","no"};
      Log.info("Making interaction between " + _ci.source._names[_ci.factors[i]] + " and " + _ci.source._names[_ci.factors[j]]);
    }

    @Override
    public void map (Chunk[]cs){
      _hm.put("Hi", 1);
//      _nbhm = new NonBlockingHashMap<Long, Long>();
      final int newCol = cs.length-1;
      for (int r = 0; r < cs[0]._len; r++) {
//        if (cs[_i].isNA0(r) && cs[_j].isNA0(r)) {
//          cs[newCol].setNA0(r);
//        }
//        else if (!cs[_i].isNA0(r) && cs[_j].isNA0(r)) {
//          cs[newCol].set0();
//        }
//
//        Long key = cs[0].at80(r);
//
//        if (!_nbhm.contains(key)) {
//          _nbhm.put(key, 1L);
//        } else {
//          Long val = _nbhm.get(key);
//          _nbhm.put(key, val + 1);
//        }
        cs[newCol].set0(r, Math.abs(new Random().nextLong())%2);
      }
    }

    @Override
    public void reduce(createInteractionDomain mrt) {
//      assert _nbhm != null;
//      assert mrt._nbhm != null;
      // TODO: merge the two hashmaps
    }
  }

}
