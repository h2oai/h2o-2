package hex;

import hex.Layer.Input;

import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import jsr166y.*;
import water.util.Utils;

/**
 * Trains a neural network.
 */
public abstract class Trainer {
  final AtomicInteger _count;
  int _batch = 20;
  int _batches;

  public Trainer(AtomicInteger count) {
    _count = count;
  }

  abstract void join();

  abstract void start();

  abstract Layer[] layers();

  public static class Direct extends Trainer {
    final Layer[] _ls;
    final Thread _thread;

    public Direct(Layer[] ls) {
      this(ls, new AtomicInteger());
    }

    public Direct(Layer[] ls, AtomicInteger count) {
      super(count);
      _ls = ls;

      _thread = new Thread() {
        public void run() {
          for( int batch = 0; _batches == 0 || batch < _batches; batch++ ) {
            Input input = (Input) _ls[0];
            for( int b = 0; b < _batch; b++ ) {
              fprop();

              for( int i = 1; i < _ls.length; i++ )
                Arrays.fill(_ls[i]._e, 0);
              float[] err = _ls[_ls.length - 1]._e;
              err[input.label()] = 1.0f;
              for( int i = 0; i < err.length; i++ )
                err[i] -= _ls[_ls.length - 1]._a[i];

              bprop();
              input._n = input._n == input._count - 1 ? 0 : input._n + 1;
            }

            for( int i = 1; i < _ls.length; i++ )
              _ls[i].adjust(_count.get());

            _count.addAndGet(_batch);
          }
        }
      };
    }

    @Override void join() {
      try {
        _thread.join();
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }
    }

    @Override void start() {
      _thread.start();
    }

    @Override Layer[] layers() {
      return _ls;
    }

    void fprop() {
      for( int i = 0; i < _ls.length; i++ )
        _ls[i].fprop(0, _ls[i]._a.length);
    }

    void bprop() {
      for( int i = _ls.length - 1; i > 0; i-- )
        _ls[i].bprop(0, _ls[i]._a.length);
    }
  }

  /**
   * Runs several trainers in parallel.
   */
  public static class ParallelTrainers extends Trainer {
    final Direct[] _trainers;

    public ParallelTrainers(Layer[] ls) {
      super(new AtomicInteger());
      _trainers = new Direct[Runtime.getRuntime().availableProcessors()];
      for( int t = 0; t < _trainers.length; t++ ) {
        Layer[] clones = new Layer[ls.length];
        for( int i = 0; i < ls.length; i++ )
          clones[i] = Utils.deepClone(ls[i], "_w", "_b");
        for( int i = 1; i < ls.length; i++ )
          clones[i]._in = clones[i - 1];
        _trainers[t] = new Direct(clones, _count);
        Input input = (Input) _trainers[t]._ls[0];
        input._n = t * input._count / _trainers.length;
      }
    }

    @Override void join() {
      for( int i = 0; i < _trainers.length; i++ )
        _trainers[i].join();
    }

    @Override void start() {
      for( int t = 0; t < _trainers.length; t++ ) {
        _trainers[t]._batches = _batches / _trainers.length;
        _trainers[t].start();
      }
    }

    @Override Layer[] layers() {
      return _trainers[0]._ls;
    }
  }

  /**
   * Chunks weight matrices over multiple threads.
   */
  public static class Chunked extends Direct {
    final Chunk[] _chunks;
    final CyclicBarrier _wait, _done;

    public Chunked(Layer[] ls) {
      super(ls);

      _chunks = new Chunk[Runtime.getRuntime().availableProcessors()];
      _wait = new CyclicBarrier(_chunks.length);
      _done = new CyclicBarrier(_chunks.length);

      int[][] offs = new int[_chunks.length][_ls.length];
      int[][] lens = new int[_chunks.length][_ls.length];
      for( int layer = 1; layer < _ls.length; layer++ ) {
        int last = 0;
        for( int i = 0; i < _chunks.length; i++ ) {
          final int limit = _ls[layer]._a.length * (i + 1) / _chunks.length;
          offs[i][layer] = last;
          lens[i][layer] = limit - last;
          last = limit;
        }
        assert last == _ls[layer]._a.length;
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

  /**
   * Process items of a batch in parallel.
   */
  public static class ParallelBatch extends Trainer {
    final Thread0[] _threads;
    final CyclicBarrier _wait, _done;
    final int _perThread;

    public ParallelBatch(Layer[] ls, byte[] labels) {
      super(labels);

      _threads = new Thread0[Runtime.getRuntime().availableProcessors()];
      _wait = new CyclicBarrier(_threads.length + 1);
      _done = new CyclicBarrier(_threads.length + 1);

      assert Layer.BATCH % _threads.length == 0;
      _perThread = Layer.BATCH / _threads.length;

      for( int t = 0; t < _threads.length; t++ ) {
        Layer[] clones = new Layer[ls.length];
        for( int i = 0; i < clones.length; i++ )
          clones[i] = Utils.deepClone(ls[i], "_w", "_b");
        _threads[t] = new Thread0(clones, t);
        _threads[t].start();
      }
    }

    @Override void batch() {
      for( int t = 0; t < _threads.length; t++ )
        _threads[t].batch();
    }

    final class Thread0 extends Thread {
      final Layer[] _ls;
      int _n;

      Thread0(Layer[] ls, int n) {
        _ls = ls;
        _n = n;
      }

      @Override void batch() {
        for( int b = 0; b < _perThread; b++ ) {
          _ls[0]._off = _n * _ls[0]._len;
          fprop();

          for( int i = 1; i < _ls.length; i++ )
            Arrays.fill(_ls[i]._e, 0);
          float[] err = _ls[_ls.length - 1]._e;
          err[_labels[_n]] = 1.0f;
          for( int i = 0; i < err.length; i++ )
            err[i] -= _ls[_ls.length - 1]._a[i];

          bprop();
          _n = _n == _labels.length - 1 ? 0 : _n + 1;
        }

        for( int i = 1; i < _ls.length; i++ )
          _ls[i].adjust();
      }
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

  /**
   *
   */
  public static class TrainerFJ extends Trainer {
    static ForkJoinPool _pool = new ForkJoinPool();

    // F/J tasks for forward and back propagation
    private ForkJoinTask[][] _fprops, _bprops;

    public TrainerFJ(Layer[] ls, byte[] labels) {
      super(ls, labels);

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