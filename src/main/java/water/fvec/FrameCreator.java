package water.fvec;

import hex.CreateFrame;
import jsr166y.CountedCompleter;
import water.H2O;
import water.Key;
import water.MRTask2;
import water.util.Log;
import water.util.Utils;

import java.util.Arrays;
import java.util.Random;

/**
 * Helper to make up a Frame from scratch, with random content
 */
public class FrameCreator extends H2O.H2OCountedCompleter {

  public FrameCreator(CreateFrame createFrame) { this(createFrame, null); }

  public FrameCreator(CreateFrame createFrame, Key job) {
    super(null);
    _job=job;
    _createFrame = createFrame;

    int[] idx = Utils.seq(0, _createFrame.cols);
    int[] shuffled_idx = new int[idx.length];
    Utils.shuffleArray(idx, idx.length, shuffled_idx, _createFrame.seed, 0);

    int catcols = (int)(_createFrame.categorical_fraction * _createFrame.cols);
    int intcols = (int)(_createFrame.integer_fraction * _createFrame.cols);
    int realcols = _createFrame.cols - catcols - intcols;

    assert(catcols >= 0);
    assert(intcols >= 0);
    assert(realcols >= 0);

    _cat_cols  = Arrays.copyOfRange(shuffled_idx, 0,               catcols);
    _int_cols  = Arrays.copyOfRange(shuffled_idx, catcols,         catcols+intcols);
    _real_cols = Arrays.copyOfRange(shuffled_idx, catcols+intcols, catcols+intcols+realcols);

    // create domains for categorical variables
    if (_createFrame.randomize) {
      _domain = new String[_createFrame.cols][];
      for (int c : _cat_cols) {
        _domain[c] = new String[_createFrame.factors];
        for (int i = 0; i < _createFrame.factors; ++i) {
          _domain[c][i] = "C" + (c+1) + ".L" + (i+1);
        }
      }
    }
  }
  final private CreateFrame _createFrame;

  private int[] _cat_cols;
  private int[] _int_cols;
  private int[] _real_cols;
  private String[][] _domain;
  private Frame _out;
  final private Key _job;

  public Frame getResult(){
    join();
    return _out;
  }

  @Override
  public void compute2() {
    Vec[] vecs = Vec.makeNewCons(_createFrame.rows, _createFrame.cols, _createFrame.value, _domain);
    _out = new Frame(Key.make(_createFrame.key), null, vecs);
    assert _out.numRows() == _createFrame.rows;
    assert _out.numCols() == _createFrame.cols;
    _out.delete_and_lock(_job);

    // fill with random values
    new FrameRandomizer(_createFrame, _cat_cols, _int_cols, _real_cols).doAll(_out);

    //overwrite a fraction with N/A
    new MissingInserter(this, _createFrame.seed, _createFrame.missing_fraction).asyncExec(_out);
  }

  @Override public void onCompletion(CountedCompleter caller){
    _out.update(_job);
    _out.unlock(_job);
  }

  @Override
  public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
    return super.onExceptionalCompletion(ex, caller);
  }

  private static class FrameRandomizer extends MRTask2<FrameRandomizer> {
    final private CreateFrame _createFrame;
    final private int[] _cat_cols;
    final private int[] _int_cols;
    final private int[] _real_cols;

    public FrameRandomizer(CreateFrame createFrame, int[] cat_cols, int[] int_cols, int[] real_cols){
      _createFrame = createFrame;
      _cat_cols = cat_cols;
      _int_cols = int_cols;
      _real_cols = real_cols;
    }

    @Override
    public void map (Chunk[]cs){
      if (!_createFrame.randomize) return;
      final Random rng = new Random();
      for (int c : _cat_cols) {
        for (int r = 0; r < cs[c]._len; r++) {
          rng.setSeed(cs[c]._start + 1234 * c - 1723 * (cs[c]._start + r)); //row+col-dependent RNG for reproducibility
          cs[c].set0(r, (int)(rng.nextDouble() * _createFrame.factors));
        }
      }
      for (int c : _int_cols) {
        for (int r = 0; r < cs[c]._len; r++) {
          rng.setSeed(cs[c]._start + 1234 * c - 1723 * (cs[c]._start + r)); //row+col-dependent RNG for reproducibility
          cs[c].set0(r, (int) ((_createFrame.integer_range + 1) * (1 - 2 * rng.nextDouble())));
        }
      }
      for (int c : _real_cols) {
        for (int r = 0; r < cs[c]._len; r++) {
          rng.setSeed(cs[c]._start + 1234 * c - 1723 * (cs[c]._start + r)); //row+col-dependent RNG for reproducibility
          cs[c].set0(r, rng.nextDouble());
        }
      }
    }
  }



  private static class MissingInserter extends MRTask2<MissingInserter> {
    long _seed;
    double _frac;

    public MissingInserter(H2O.H2OCountedCompleter cmp, long seed, double frac){
      super(cmp);
      _seed = seed;
      _frac = frac;
    }

    @Override
    public void map (Chunk[]cs){
      if (_frac == 0) return;
      final Random rng = new Random();
      for (int c = 0; c < cs.length; c++) {
        for (int r = 0; r < cs[c]._len; r++) {
          rng.setSeed(cs[c]._start + 1234 * c - 1723 * (cs[c]._start + r)); //row+col-dependent RNG for reproducibility
          if (rng.nextDouble() < _frac) cs[c].setNA0(r);
        }
      }
    }
  }



}
