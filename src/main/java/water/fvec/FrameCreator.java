package water.fvec;

import hex.CreateFrame;
import jsr166y.CountedCompleter;
import water.H2O;
import water.Key;
import water.MRTask2;
import water.util.Utils;

import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

/**
 * Helper to make up a Frame from scratch, with random content
 */
public class FrameCreator extends H2O.H2OCountedCompleter {

  public FrameCreator(CreateFrame createFrame) { this(createFrame, null); }

  public FrameCreator(CreateFrame createFrame, Key job) {
    super(null);
    _job=job;
    _createFrame = createFrame;

    int[] idx = Utils.seq(1, _createFrame.cols+1);
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
      assert(_createFrame.response_factors >= 1);
      _domain = new String[_createFrame.cols+1][];
      _domain[0] = _createFrame.response_factors == 1 ? null : new String[_createFrame.response_factors];
      if (_domain[0] != null) {
        for (int i=0; i <_domain[0].length; ++i) {
          _domain[0][i] = "resp." + i;
        }
      }

      for (int c : _cat_cols) {
        _domain[c] = new String[_createFrame.factors];
        for (int i = 0; i < _createFrame.factors; ++i) {
          _domain[c][i] = UUID.randomUUID().toString().subSequence(0,5).toString();
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

  @Override public void compute2() {
    Vec[] vecs = Vec.makeNewCons(_createFrame.rows, _createFrame.cols+1, _createFrame.value, _domain);
    String[] names = new String[vecs.length];
    names[0] = "response";
    for( int i=1; i<vecs.length; i++ ) names[i] = "C"+i;

    _out = new Frame(Key.make(_createFrame.key), names, vecs);
    assert _out.numRows() == _createFrame.rows;
    assert _out.numCols() == _createFrame.cols+1;
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

    //row+col-dependent RNG for reproducibility with different number of VMs, chunks, etc.
    void setSeed(Random rng, int col, long row) {
      rng.setSeed(_createFrame.seed + _createFrame.cols * row + col);
      rng.setSeed(rng.nextLong());
    }

    @Override
    public void map (Chunk[]cs){
      if (!_createFrame.randomize) return;
      final Random rng = new Random();

      // response
      for (int r = 0; r < cs[0]._len; r++) {
        setSeed(rng, 0, cs[0]._start + r);
        if (_createFrame.response_factors >1)
          cs[0].set0(r, (int)(rng.nextDouble() * _createFrame.response_factors)); //classification
        else if (_createFrame.positive_response)
          cs[0].set0(r, _createFrame.real_range * rng.nextDouble()); //regression with positive response
        else
          cs[0].set0(r, _createFrame.real_range * (1 - 2 * rng.nextDouble())); //regression
      }

      for (int c : _cat_cols) {
        for (int r = 0; r < cs[c]._len; r++) {
          setSeed(rng, c, cs[c]._start + r);
          cs[c].set0(r, (int)(rng.nextDouble() * _createFrame.factors));
        }
      }
      for (int c : _int_cols) {
        for (int r = 0; r < cs[c]._len; r++) {
          setSeed(rng, c, cs[c]._start + r);
          cs[c].set0(r, (long) ((_createFrame.integer_range+1) * (1 - 2 * rng.nextDouble())));
        }
      }
      for (int c : _real_cols) {
        for (int r = 0; r < cs[c]._len; r++) {
          setSeed(rng, c, cs[c]._start + r);
          cs[c].set0(r, _createFrame.real_range * (1 - 2 * rng.nextDouble()));
        }
      }
    }
  }



  private static class MissingInserter extends MRTask2<MissingInserter> {
    final long _seed;
    final double _frac;

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
          rng.setSeed(_seed + 1234 * c ^ 1723 * (cs[c]._start + r)); //row+col-dependent RNG for reproducibility
          if (rng.nextDouble() < _frac) cs[c].setNA0(r);
        }
      }
    }
  }



}
