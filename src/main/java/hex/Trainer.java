package hex;

import java.util.concurrent.CyclicBarrier;

import jsr166y.*;

/**
 * Trains a neural network.
 */
public abstract class Trainer {
  final Layer[] _ls;

  Trainer(Layer[] ls) {
    _ls = ls;
  }

  abstract void fprop();

  abstract void bprop();

  public static class Direct extends Trainer {
    public Direct(Layer[] ls) {
      super(ls);
    }

    @Override void fprop() {
      for( int i = 1; i < _ls.length; i++ )
        _ls[i].fprop(0, _ls[i]._len);
    }

    @Override void bprop() {
      for( int i = _ls.length - 1; i > 0; i-- )
        _ls[i].bprop(0, _ls[i]._len);
    }
  }

  /**
   * Chunks weight matrices over multiple threads.
   */
  public static class Chunked extends Trainer {
    final Chunk[] _chunks;
    final CyclicBarrier _wait, _done;

    public Chunked(Layer[] ls) {
      super(ls);

      _chunks = new Chunk[Runtime.getRuntime().availableProcessors()];
      _wait = new CyclicBarrier(_chunks.length + 1);
      _done = new CyclicBarrier(_chunks.length + 1);

      int[][] offs = new int[_chunks.length][_ls.length];
      int[][] lens = new int[_chunks.length][_ls.length];
      for( int layer = 1; layer < _ls.length; layer++ ) {
        int last = 0;
        for( int i = 0; i < _chunks.length; i++ ) {
          final int limit = _ls[layer]._len * (i + 1) / _chunks.length;
          offs[i][layer] = last;
          lens[i][layer] = limit - last;
          last = limit;
        }
        assert last == _ls[layer]._len;
      }
      for( int i = 0; i < _chunks.length; i++ ) {
        _chunks[i] = new Chunk(offs[i], lens[i]);
        _chunks[i].start();
      }
    }

    final class Chunk extends Thread {
      final int[] _offs, _lens;
      int _level = 1;
      boolean _up = true;

      Chunk(int[] offs, int[] lens) {
        _offs = offs;
        _lens = lens;
      }

      @Override public void run() {
        try {
          for( ;; ) {
            _wait.await();
            if( _up ) {
              _ls[_level].fprop(_offs[_level], _lens[_level]);
              _level++;
              if( _level == _ls.length ) {
                _up = false;
                _level--;
              }
            } else {
              _ls[_level].bprop(_offs[_level], _lens[_level]);
              _level--;
              if( _level == 0 ) {
                _up = true;
                _level++;
              }
            }
            _done.await();
          }
        } catch( Exception e ) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override void fprop() {
      pass();
    }

    @Override void bprop() {
      pass();
    }

    private void pass() {
      try {
        for( int i = 1; i < _ls.length; i++ ) {
          _wait.await();
          _done.await();
        }
      } catch( Exception e ) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class TrainerFJ extends Trainer {
    static ForkJoinPool _pool = new ForkJoinPool();

    // F/J tasks for forward and back propagation
    private ForkJoinTask[][] _fprops, _bprops;

    public TrainerFJ(Layer[] ls) {
      super(ls);

      int cores = Runtime.getRuntime().availableProcessors();
      _fprops = new ForkJoinTask[ls.length][cores];
      _bprops = new ForkJoinTask[ls.length][cores];
      for( int n = 0; n < ls.length; n++ ) {
        final Layer layer = ls[n];
        int last = 0;
        for( int i = 0; i < cores; i++ ) {
          final int limit = layer._len * (i + 1) / cores, off = last, len = limit - last;
          last = limit;
          _fprops[n][i] = new RecursiveAction() {
            @Override protected void compute() {
              layer.fprop(off, len);
            }
          };
          _bprops[n][i] = new RecursiveAction() {
            @Override protected void compute() {
              layer.bprop(off, len);
            }
          };
        }
        assert last == layer._len;
      }
    }

    @Override void fprop() {
//      for( int i = 0; i < _fprops.length; i++ )
//        _fj.submit(_fprops[i]);
//      for( int i = 0; i < _fprops.length; i++ ) {
//        _fprops[i].join();
//        _fprops[i].reinitialize();
//      }
    }

    @Override void bprop() {
//      for( int i = 0; i < _bprops.length; i++ )
//        _fj.submit(_bprops[i]);
//      for( int i = 0; i < _bprops.length; i++ ) {
//        _bprops[i].join();
//        _bprops[i].reinitialize();
//      }
    }
  }
}