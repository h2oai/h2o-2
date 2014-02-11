package water.util;

import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;

import java.util.Random;

import static water.util.Utils.getDeterRNG;

/**
 * Created by arno on 2/10/14.
 */
public class MRUtils {

  public static Frame sampleFrame(Frame fr, final long rows, final long seed) {
    assert(rows >= 1);
    final float fraction = rows > 0 ? (float)rows / fr.numRows() : 1.f;
    Frame r = new MRTask2() {
      @Override
      public void map(Chunk[] cs, NewChunk[] ncs) {
        final Random rng = getDeterRNG(seed + cs[0].cidx());
        for (int r = 0; r < cs[0]._len; r++)
          if (rng.nextFloat() < fraction) {
            for (int i = 0; i < ncs.length; i++) {
              ncs[i].addNum(cs[i].at0(r));
            }
          }
      }
    }.doAll(fr.numCols(), fr).outputFrame(fr.names(), fr.domains());
    if (r.numRows() == 0) {
      Log.warn("You asked for " + rows + " rows (out of " + fr.numRows() + "), but you got none (seed=" + seed + ").");
      Log.warn("Let's try again. You've gotta ask yourself a question: \"Do I feel lucky?\"");
      sampleFrame(fr, rows, seed+1);
    }
    return r;
  }
}
