package water.fvec;

import hex.Interaction;
import jsr166y.CountedCompleter;
import water.*;
import water.util.Log;
import water.util.Utils;
import java.util.Random;

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

    // Pass 1: compute unique domains of all interaction features
//    for () {
    String[] domain = new createInteractionDomain(_ci, 0, 1).doAll(_out)._domain;
    Vec interaction = _out.anyVec().makeZero(domain);
    _out.add("this_interaction", interaction);

      // Pass 2: create new vector based on the domain
    new createInteractionDomain(_ci, 0, 1).doAll(_out); //FIXME: call for pass 2
//    }


    _out.delete_and_lock(_job);

    tryComplete();

    // Needs to create _domain and enum chunk values
  }

  @Override
  public void onCompletion(CountedCompleter caller) {
    _out.update(_job);
    _out.unlock(_job);
  }




// Create domain
private static class createInteractionDomain extends MRTask2<createInteractionDomain> {
  // INPUT
  final private Interaction _ci;
  private final Utils.IcedHashMap<Utils.IcedLong, Utils.IcedLong> _hm = new Utils.IcedHashMap<Utils.IcedLong, Utils.IcedLong>();

  int _i;
  int _j;

  // OUTPUT
  public String[] _domain;

  public createInteractionDomain(Interaction ci, int i, int j) {
    _i = i;
    _j = j;
    _ci = ci;
    String[] A = _ci.source.domains()[_ci.factors[i]];
    String[] B = _ci.source.domains()[_ci.factors[j]];

    _domain = new String[]{ A[0]+"_"+B[0], A[1]+"_"+B[0], A[0]+"_"+B[1], A[1]+"_"+B[1]};

    Log.info("Making interaction between " + _ci.source._names[_ci.factors[i]] + " and " + _ci.source._names[_ci.factors[j]]);
  }

  @Override
  public void map(Chunk[] cs) {
    final int newCol = cs.length - 1;
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
      cs[newCol].set0(r, Math.abs(new Random().nextLong()) % 4);
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
