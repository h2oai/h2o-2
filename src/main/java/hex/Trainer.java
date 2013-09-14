package hex;

import hex.Layer.ChunksInput;
import hex.Layer.FrameInput;
import hex.Layer.Input;
import hex.NeuralNet.Weights;

import java.io.IOException;
import java.nio.FloatBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;
import water.util.Log;

import com.jogamp.opencl.*;
import com.jogamp.opencl.CLMemory.Mem;

/**
 * Trains a neural network.
 */
public abstract class Trainer {
  public Trainer() {
  }

  public abstract Layer[] layers();

  public abstract void run();

  public long steps() {
    throw new UnsupportedOperationException();
  }

  public static class Base extends Trainer {
    final Layer[] _ls;

    public Base(Layer[] ls) {
      _ls = ls;
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public void run() {
      throw new UnsupportedOperationException();
    }

    final void step() {
      Input input = (Input) _ls[0];
      fprop();

      for( int i = 1; i < _ls.length - 1; i++ )
        Arrays.fill(_ls[i]._e, 0);
      float[] err = _ls[_ls.length - 1]._e;
      int label = input.label();
      for( int i = 0; i < err.length; i++ ) {
        float t = i == label ? 1 : 0;
        err[i] = t - _ls[_ls.length - 1]._a[i];
      }

      bprop();
    }

    final void adjust(long n) {
      for( int i = 1; i < _ls.length; i++ ) {
        _ls[i].anneal(n);
        _ls[i].momentum(n);
      }
    }

    final void fprop() {
      for( int i = 0; i < _ls.length; i++ )
        _ls[i].fprop();
    }

    final void bprop() {
      for( int i = _ls.length - 1; i > 0; i-- )
        _ls[i].bprop();
    }
  }

  public static class Direct extends Base {
    int _batch = 20;
    int _batches;
    int _current;

    public Direct(Layer[] ls) {
      super(ls);
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public void run() {
      Input input = (Input) _ls[0];
      for( _current = 0; _batches == 0 || _current < _batches; _current++ ) {
        for( int s = 0; s < _batch; s++ ) {
          step();
          input.move();
        }
        adjust(_current * _batch);
      }
    }

    @Override public long steps() {
      return _batch * (long) _current;
    }
  }

  /**
   * Runs several trainers in parallel on the same weights. There might be lost updates, but works
   * well in practice. TODO replace by TrainerMR.
   */
  public static class Threaded extends Trainer {
    final Base[] _trainers;
    final Thread[] _threads;
    final int _stepsPerThread;
    static final CyclicBarrier DONE = new CyclicBarrier(1);
    volatile CyclicBarrier _suspend;
    final CyclicBarrier _resume;
    final AtomicLong _steps = new AtomicLong();

    public Threaded(Layer[] ls) {
      this(ls, 0, Runtime.getRuntime().availableProcessors());
    }

    public Threaded(Layer[] ls, int steps, int cores) {
      _trainers = new Base[cores];
      _threads = new Thread[_trainers.length];
      _stepsPerThread = steps / _threads.length;
      _resume = new CyclicBarrier(_threads.length + 1);

      for( int t = 0; t < _trainers.length; t++ ) {
        final Input input = (Input) ls[0].clone();
        input.init(null, ls[0]._a.length, false);
        input._row = input._len * t / _trainers.length;
        Layer[] clones = Layer.clone(ls, input);

        _trainers[t] = new Base(clones);
        final Base trainer = _trainers[t];

        _threads[t] = new Thread("H2O Trainer " + t) {
          @Override public void run() {
            for( int i = 0; _stepsPerThread == 0 || i < _stepsPerThread; i++ ) {
              CyclicBarrier b = _suspend;
              if( b == DONE )
                break;
              if( b != null ) {
                try {
                  b.await();
                  _resume.await();
                } catch( Exception e ) {
                  throw new RuntimeException(e);
                }
              }
              trainer.step();
              input.move();
              _steps.incrementAndGet();
            }
          }
        };
      }
      Log.info("Started " + _trainers.length + " neural network trainers");
    }

    @Override public Layer[] layers() {
      return _trainers[0].layers();
    }

    @Override public void run() {
      start();
      join();
    }

    @Override public long steps() {
      return _steps.get();
    }

    public void start() {
      for( int t = 0; t < _threads.length; t++ )
        _threads[t].start();
    }

    public void close() {
      _suspend = DONE;
    }

    void suspend() {
      try {
        _suspend = new CyclicBarrier(_threads.length + 1);
        _suspend.await();
        _suspend = null;
      } catch( Exception e ) {
        throw new RuntimeException(e);
      }
    }

    void resume() {
      try {
        _resume.await();
      } catch( Exception e ) {
        throw new RuntimeException(e);
      }
    }

    void join() {
      for( int i = 0; i < _threads.length; i++ ) {
        try {
          _threads[i].join();
        } catch( InterruptedException e ) {
          throw new RuntimeException(e);
        }
      }
    }
  }

//  public static class ThreadedNode extends Trainer {
//    final Base[] _trainers;
//    final Thread[] _threads;
//    final int _stepsPerThread;
//    final AtomicLong _steps = new AtomicLong();
//
//    public ThreadedNode(Layer[] ls, int steps) {
//      _trainers = new Base[Runtime.getRuntime().availableProcessors()];
//      _threads = new Thread[_trainers.length];
//      _stepsPerThread = steps / _threads.length;
//
//      for( int t = 0; t < _trainers.length; t++ ) {
//        final Input input = (Input) ls[0].clone();
//        input.init(null, ls[0]._a.length, false);
//        input._row = input._len * t / _trainers.length;
//        Layer[] clones = Layer.clone(ls, input);
//
//        _trainers[t] = new Base(clones);
//        final Base trainer = _trainers[t];
//
//        _threads[t] = new Thread("H2O Trainer " + t) {
//          @Override public void run() {
//            for( int i = 0; _stepsPerThread == 0 || i < _stepsPerThread; i++ ) {
//              CyclicBarrier b = _suspend;
//              if( b == DONE )
//                break;
//              if( b != null ) {
//                try {
//                  b.await();
//                  _resume.await();
//                } catch( Exception e ) {
//                  throw new RuntimeException(e);
//                }
//              }
//              trainer.step();
//              input.move();
//              _steps.incrementAndGet();
//            }
//          }
//        };
//      }
//      Log.info("Started " + _trainers.length + " neural network trainers");
//    }
//
//    @Override public Layer[] layers() {
//      return _trainers[0].layers();
//    }
//
//    @Override public void run() {
//      start();
//      join();
//    }
//
//    @Override public long steps() {
//      return _steps.get();
//    }
//
//    public void start() {
//      for( int t = 0; t < _threads.length; t++ )
//        _threads[t].start();
//    }
//
//    public void close() {
//      _suspend = DONE;
//    }
//
//    void suspend() {
//      try {
//        _suspend = new CyclicBarrier(_threads.length + 1);
//        _suspend.await();
//        _suspend = null;
//      } catch( Exception e ) {
//        throw new RuntimeException(e);
//      }
//    }
//
//    void resume() {
//      try {
//        _resume.await();
//      } catch( Exception e ) {
//        throw new RuntimeException(e);
//      }
//    }
//
//    void join() {
//      for( int i = 0; i < _threads.length; i++ ) {
//        try {
//          _threads[i].join();
//        } catch( InterruptedException e ) {
//          throw new RuntimeException(e);
//        }
//      }
//    }
//  }

  public static class MR extends Trainer {
    Layer[] _ls;
    int _epochs;
    long _count;

    public MR(Layer[] ls, int epochs) {
      _ls = ls;
      _epochs = epochs;
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public long steps() {
      return _count;
    }

    @Override public void run() {
      final Frame frame = ((FrameInput) _ls[0])._frame;
      assert _ls[0]._a.length == frame._vecs.length - 1;
      assert frame.anyVec().nChunks() >= cores();

      // Pre compute stats, otherwise can be done multiple times concurrently
      for( int i = 0; i < frame._vecs.length; i++ )
        frame._vecs[i].min();

      int batch = 1000;
      int rows = Math.max(1, batch / frame.anyVec().nChunks());
      int splits = 1 + frame.anyVec().chunk(0)._len / rows;

      for( int e = 0; _epochs == 0 || e < _epochs; e++ ) {
        for( int split = 0; split < splits; split++ ) {
          Descent task = new Descent();
          Layer[] clones = new Layer[_ls.length];
          for( int y = 1; y < _ls.length; y++ )
            clones[y] = _ls[y].clone();
          task._ls = clones;
          task._ws = new float[_ls.length][];
          task._bs = new float[_ls.length][];
          for( int y = 1; y < _ls.length; y++ ) {
            task._ws[y] = _ls[y]._w;
            task._bs[y] = _ls[y]._b;
          }
          task._splits = splits;
          task._split = split;
          task.doAll(frame);
          //      Log.info("descent " + split);

          for( int y = 1; y < _ls.length; y++ ) {
            for( int i = 0; i < _ls[y]._w.length; i++ )
              _ls[y]._w[i] += task._dw[y][i];
            for( int i = 0; i < _ls[y]._b.length; i++ )
              _ls[y]._b[i] += task._db[y][i];
          }
        }
      }
    }

    static Key key(String uid, int node) {
      return Key.make(uid + node, (byte) 1, Key.DFJ_INTERNAL_USER, H2O.CLOUD._memary[node]);
    }
  }

  static class Descent extends MRTask2<Descent> {
    Layer[] _ls;
    int _splits, _split;
    float[][] _ws, _bs;
    float[][] _dw, _db;

    @Override public void setupLocal() {
      super.setupLocal();
      for( int y = 1; y < _ls.length; y++ ) {
        _ls[y]._w = _ws[y].clone();
        _ls[y]._b = _bs[y].clone();
      }
    }

    @Override public void map(Chunk[] cs) {
      Layer[] clones = new Layer[_ls.length];
      FrameInput stats = (FrameInput) _ls[0];
      ChunksInput input = new ChunksInput(cs, stats._means, stats._sigmas);
      input.init(null, cs.length - 1, false);
      clones[0] = input;
      for( int y = 1; y < _ls.length; y++ ) {
        clones[y] = _ls[y].clone();
        clones[y].init(clones[y - 1], _bs[y].length, false);
      }
      Base base = new Base(clones);
      int off = cs[0]._len * (_split + 0) / _splits;
      int lim = cs[0]._len * (_split + 1) / _splits;

      for( input._row = off; input._row < lim; input._row++ )
        base.step();
    }

    @Override public void reduce(Descent other) {
      // If other instance (i.e. from remote node)
      if( other._dw != null ) {
        assert other._db != null && other._ws == null;
        if( _dw == null ) {
          _dw = other._dw;
          _db = other._db;
        } else {
          for( int y = 1; y < _dw.length; y++ ) {
            for( int i = 0; i < _dw[y].length; i++ )
              _dw[y][i] += other._dw[y][i];
            //_dw[y][i] = (_dw[y][i] + other._dw[y][i]) / 2;
            for( int i = 0; i < _db[y].length; i++ )
              _db[y][i] += other._db[y][i];
            //_db[y][i] = (_db[y][i] + other._db[y][i]) / 2;
          }
        }
      }
    }

    @Override protected void closeLocal() {
      super.closeLocal();
      if( _dw == null ) {
        _dw = new float[_ws.length][];
        _db = new float[_bs.length][];
        for( int y = 1; y < _ws.length; y++ ) {
          _dw[y] = new float[_ws[y].length];
          _db[y] = new float[_bs[y].length];
        }
      }
      for( int y = 1; y < _ws.length; y++ ) {
        for( int i = 0; i < _ws[y].length; i++ )
          _dw[y][i] += _ls[y]._w[i] - _ws[y][i];
        for( int i = 0; i < _bs[y].length; i++ )
          _db[y][i] += _ls[y]._b[i] - _bs[y][i];
      }
      _ws = _bs = null;
    }

    @Override public boolean logVerbose() {
      return false;
    }
  }

  /**
   *
   */
  public static class MRAsync extends Trainer {
    static final float FORWARDNESS = 1.5f;
    static final float COHESIVENESS = .5f;

    Layer[] _ls;
    int _epochs;
    volatile long _steps;
    transient Key[] _keys;

    public MRAsync(Layer[] ls, int epochs) {
      _ls = ls;
      _epochs = epochs;
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public long steps() {
      return _steps;
    }

    @Override public void run() {
    }

    public void start() {
      // Dispatch one copy of weights per node
      Weights weights = Weights.get(_ls, false);
      final String uid = UUID.randomUUID().toString();
      _keys = new Key[H2O.CLOUD._memary.length];
      for( int i = 0; i < _keys.length; i++ ) {
        _keys[i] = key(uid, i);
        UKV.put(_keys[i], weights);
      }

      final Frame frame = ((FrameInput) _ls[0])._frame;
      assert _ls[0]._a.length == frame._vecs.length - 1;
      assert frame.anyVec().nChunks() >= cores();

      // Pre compute stats, otherwise can be done multiple times concurrently
      for( int i = 0; i < frame._vecs.length; i++ )
        frame._vecs[i].min();

      // Start gradient descents
      final H2OCountedCompleter task = new H2OCountedCompleter() {
        @Override public void compute2() {
          for( int e = 0; _epochs == 0 || e < _epochs; e++ ) {
            DescentAsync task = new DescentAsync();
            task._uid = uid;
            task._ls = _ls;
            task.doAll(frame);
            Log.info("epoch " + e);
          }
          tryComplete();
        }
      };
      H2O.submitTask(task);

      // Sync nodes at regular intervals
      Thread thread = new Thread() {
        @Override public void run() {
          while( !task.isDone() ) {
            try {
              Thread.sleep(10);
            } catch( InterruptedException e ) {
              throw new RuntimeException(e);
            }
            Weights[] nodes = new Weights[_keys.length];
            for( int i = 0; i < _keys.length; i++ )
              nodes[i] = UKV.get(_keys[i]);

            // TODO try angular mean
//            NodeDescent shepherd = new NodeDescent(uid);
//            shepherd._ws = new float[last._ws.length][];
//            shepherd._bs = new float[last._bs.length][];
//              for( int y = 1; y < last._ws.length; y++ ) {
//                shepherd._ws[y] = new float[last._ws[y].length];
//                for( int i = 0; i < last._ws[y].length; i++ ) {
//                  float mean = 0;
//                  for( int n = 0; n < nodes.length; n++ )
//                    mean += nodes[n]._ws[y][i];
//                  mean /= nodes.length;
//                  shepherd._ws[y][i] = mean + FORWARDNESS * (mean - last._ws[y][i]);
//                  last._ws[y][i] = mean;
//                }
//                shepherd._bs[y] = new float[last._bs[y].length];
//                for( int i = 0; i < last._bs[y].length; i++ ) {
//                  float mean = 0;
//                  for( int n = 0; n < nodes.length; n++ )
//                    mean += nodes[n]._bs[y][i];
//                  mean /= nodes.length;
//                  shepherd._bs[y][i] = mean + FORWARDNESS * (mean - last._bs[y][i]);
//                  last._bs[y][i] = mean;
//                }
//              }
//            shepherd.dfork(_keys);
//            shepherd.join();
//            _steps = shepherd._steps;
            System.out.println("g:" + _steps);
          }
        }
      };
      thread.start();
    }

    public void close() {
      for( int i = 0; i < _keys.length; i++ )
        UKV.remove(_keys[i]);
    }

    static Key key(String uid, int node) {
      return Key.make(uid + node, (byte) 1, Key.DFJ_INTERNAL_USER, H2O.CLOUD._memary[node]);
    }

  }

  static class DescentAsync extends MRTask2<DescentAsync> {
    String _uid;
    Layer[] _ls;

    @Override public void map(Chunk[] cs) {
      Layer[] clones = new Layer[_ls.length];
      FrameInput stats = (FrameInput) _ls[0];
      ChunksInput input = new ChunksInput(cs, stats._means, stats._sigmas);
      input.init(null, cs.length - 1);
      clones[0] = input;

      Key key = MRAsync.key(_uid, H2O.SELF.index());
      assert key.home();
      Weights weights = UKV.get(key);
      for( int y = 1; y < _ls.length; y++ ) {
        clones[y] = _ls[y].clone();
        clones[y].init(clones[y - 1], weights._bs[y].length);
      }
      weights.set(clones);
      Base base = new Base(clones);
      //   Log.info("pos:" + (cs[0]._start + _off) + ", for:" + _len);
      for( input._row = 0; input._row < cs[0]._len; input._row++ ) {
        base.step();
//        weights._steps.incrementAndGet();
      }
    }

    @Override public void reduce(DescentAsync mrt) {
    }

    @Override public boolean logVerbose() {
      return false;
    }
  }

  static int cores() {
    int cores = 0;
    for( H2ONode node : H2O.CLOUD._memary )
      cores += node._heartbeat._num_cpus;
    return cores;
  }

  /**
   * Makes sure frame is spread over enough chunks to parallelize training, neural nets can require
   * lots of processing even on small datasets.
   */
  public static Frame reChunk(Frame frame) {
    final int splits = cores() * 3; // TODO
    if( frame.anyVec().nChunks() >= splits )
      return frame;
    Vec[] vecs = new Vec[frame._vecs.length];
    for( int v = 0; v < vecs.length; v++ ) {
      AppendableVec vec = new AppendableVec(UUID.randomUUID().toString());
      long rows = frame.numRows();
      Chunk cache = null;
      for( int split = 0; split < splits; split++ ) {
        long off = rows * (split + 0) / splits;
        long lim = rows * (split + 1) / splits;
        NewChunk chunk = new NewChunk(vec, split);
        for( long r = off; r < lim; r++ ) {
          if( cache == null || r < cache._start || r >= cache._start + cache._len )
            cache = frame._vecs[v].chunk(r);
          if( !cache.isNA(r) ) {
            if( frame._vecs[v]._domain != null )
              chunk.addEnum((int) cache.at8(r));
            else if( frame._vecs[v].isInt() )
              chunk.addNum(cache.at8(r), 0);
            else
              chunk.addNum(cache.at(r));
          } else {
            if( frame._vecs[v].isInt() )
              chunk.addNA();
            else {
              // Don't use addNA() for doubles, as NewChunk uses separate array
              chunk.addNum(Double.NaN);
            }
          }
        }
        chunk.close(split, null);
      }
      vecs[v] = vec.close(null);
      vecs[v]._domain = frame._vecs[v]._domain;
    }
    frame.remove();
    return new Frame(frame.names(), vecs);
  }

  /*
   *
   */

  public static class MR2 extends Trainer {
    static final ConcurrentHashMap<Key, MR2> _instances = new ConcurrentHashMap<Key, MR2>();

    Layer[] _ls;
    int _epochs;
    AtomicIntegerArray _counts;
    //AtomicLong _steps = new AtomicLong();
    transient Key _key;

    public MR2(Layer[] ls, int epochs) {
      _ls = ls;
      _epochs = epochs;
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public long steps() {
      Frame frame = ((FrameInput) _ls[0])._frame;
      long n = 0;
      for( int i = 0; i < _counts.length(); i++ )
        n += _counts.get(i) * frame._vecs[0].chunkLen(i);
      return n;
    }

    @Override public void run() {
    }

    public void start() {
      // TODO? Chunk weights over all nodes
      // _keys = new Key[H2O.CLOUD._memary.length];
      // Weights[] weights = new Weights[_keys.length];

      _key = Key.make(UUID.randomUUID().toString(), (byte) 1, Key.DFJ_INTERNAL_USER, H2O.SELF);
      _instances.put(_key, this);
      DKV.put(_key, new Value(_key, new byte[0]));

      final Frame frame = ((FrameInput) _ls[0])._frame;
      assert _ls[0]._a.length == frame._vecs.length - 1;
      assert frame.anyVec().nChunks() >= cores();
      _counts = new AtomicIntegerArray(frame._vecs[0].nChunks());

      Descent2 task = new Descent2();
      task._ws = new float[_ls.length][];
      task._bs = new float[_ls.length][];
      for( int y = 1; y < _ls.length; y++ ) {
        task._ws[y] = _ls[y]._w;
        task._bs[y] = _ls[y]._b;
      }
      task._ls = _ls;
      task._key = _key;
      task._epochs = _epochs;
      //task.dfork(frame);
      task.doAll(frame);
      Log.info("launched");
    }

    public void close() {
      _instances.remove(_key);
      UKV.remove(_key);
    }
  }

  static class Descent2 extends MRTask2<Descent2> {
    static final int BATCH = 16;

    Layer[] _ls;
    float[][] _ws, _bs;
    Key _key;
    int _epochs;
    transient NodeDescent _node;
    transient volatile boolean _done;

    @Override protected void setupLocal() {
      _node = new NodeDescent(_ls, _ws, _bs, _key);

      if( !_key.home() ) {
        // Separate thread for more regular latency
        Thread thread = new Thread() {
          @Override public void run() {
            while( !_done ) {
              if( !_node.sync() ) {
                try {
                  Thread.sleep(10);
                } catch( InterruptedException ex ) {
                }
              }
            }
          }
        };
        thread.setDaemon(true);
        thread.start();
      }
    }

    @Override protected void closeLocal() {
      // Launch actual computation in order, otherwise passes
      // between chunks diverge quickly
      Descent2Epoch epoch = new Descent2Epoch();
      epoch._node = _node;
      epoch._count = _epochs == 0 ? -1 : _epochs;
      H2O.submitTask(epoch);
      _ls = null;
      _ws = _bs = null;
      _key = null;
    }

    @Override public void map(Chunk[] cs) {
      _node._chunks.add(cs);
    }

    @Override public void reduce(Descent2 mrt) {
    }

    @Override public boolean logVerbose() {
      return false;
    }
  }

  static class Descent2Epoch extends H2OCountedCompleter {
    NodeDescent _node;
    int _count;

    @Override public void compute2() {
      if( _count < 0 || --_count > 0 ) {
        for( Chunk[] cs : _node._chunks ) {
          Descent2Chunk task = new Descent2Chunk();
          task._node = _node;
          task._cs = cs;
          H2O.submitTask(task);
        }
        reinitialize();
        H2O.submitTask(this);
      }
    }
  }

  static class Descent2Chunk extends H2OCountedCompleter {
    NodeDescent _node;
    Chunk[] _cs;

    @Override public void compute2() {
      Layer[] clones = new Layer[_node._ls.length];
      FrameInput stats = (FrameInput) _node._ls[0];
      ChunksInput input = new ChunksInput(_cs, stats._means, stats._sigmas);
      input.init(null, _cs.length - 1);
      clones[0] = input;
      for( int y = 1; y < _node._ls.length; y++ ) {
        clones[y] = _node._ls[y].clone();
        clones[y]._w = _node._ws[y];
        clones[y]._b = _node._bs[y];
        clones[y].init(clones[y - 1], _node._bs[y].length, false);
      }
      Base base = new Base(clones);
      // Log.info("row " + _cs[0]._start + " to " + (_cs[0]._start + _cs[0]._len));
      for( input._row = 0; input._row < _cs[0]._len; input._row++ )
        base.step();
      int chunk = _cs[0].cidx();
      _node.stepped(chunk);
    }
  }

  static class NodeDescent extends AtomicInteger {
    transient ConcurrentLinkedQueue<Chunk[]> _chunks = new ConcurrentLinkedQueue<Chunk[]>();

    Layer[] _ls;
    float[][] _ws, _bs;
    float[][] _wi, _bi;
    Key _key;
    //AtomicInteger _counter = new AtomicInteger();
    //int[] _steps;
    ConcurrentHashMap<Integer, Integer> _counters;
    MR2 _trainer;

    NodeDescent(Layer[] ls, float[][] ws, float[][] bs, Key key) {
      _ls = ls;
      _key = key;
      _ws = ws;
      _bs = bs;
      _wi = new float[ws.length][];
      _bi = new float[bs.length][];
      for( int y = 1; y < _ws.length; y++ ) {
        _wi[y] = ws[y].clone();
        _bi[y] = bs[y].clone();
      }
      //_steps = new int[frame.firstReadable().nChunks()];
      _trainer = MR2._instances.get(_key);
      assert (_trainer != null) == _key.home();
      if( _trainer == null )
        _counters = new ConcurrentHashMap<Integer, Integer>();
    }

    void stepped(int chunk) {
      assert (_trainer != null) == _key.home();
      if( _trainer != null )
        _trainer._counts.incrementAndGet(chunk);
      else {
        for( ;; ) {
          Integer n = _counters.get(chunk);
          if( n == null ) {
            if( _counters.putIfAbsent(chunk, 1) == null )
              break;
          } else {
            if( _counters.replace(chunk, n, n + 1) )
              break;
          }
        }
      }
    }

    boolean sync() {
      assert !_key.home();
//      int steps;
//      for( ;; ) {
//        steps = _counter.get();
//        if( _counter.compareAndSet(steps, 0) )
//          break;
//      }
//      if( _counters.size() == 0 )
//        return false;
      int[] counts = new int[10];
      int n = 0;
//      Iterator<Entry<Integer, Integer>> it = _counters.entrySet().iterator();
//        while(it.hasNext()) {
//          Entry e = it.next();
//          it.remove();
//        }
//      }
      for( Entry<Integer, Integer> entry : _counters.entrySet() ) {
        if( n == counts.length ) {
          int[] t = new int[counts.length * 2];
          System.arraycopy(counts, 0, t, 0, counts.length);
          counts = t;
        }
        counts[n++] = entry.getKey();
        counts[n++] = _counters.remove(entry.getKey());
      }
      if( n > counts.length ) {
        int[] t = new int[n];
        System.arraycopy(counts, 0, t, 0, t.length);
        counts = t;
      }
      if( n > 0 ) {
        Log.info("sync " + n);
        Shuttle s = new Shuttle();
        s._w = new float[_ws.length][];
        s._b = new float[_bs.length][];
        for( int y = 1; y < _ws.length; y++ ) {
          s._w[y] = new float[_ws[y].length];
          for( int i = 0; i < _ws[y].length; i++ ) {
            s._w[y][i] = _ws[y][i] - _wi[y][i];
            _wi[y][i] = _ws[y][i];
          }
          s._b[y] = new float[_bs[y].length];
          for( int i = 0; i < _bs[y].length; i++ ) {
            s._b[y][i] = _bs[y][i] - _bi[y][i];
            _bi[y][i] = _bs[y][i];
          }
        }
        s._counts = counts;
        s.invoke(_key);
        for( int y = 1; y < _ws.length; y++ ) {
          for( int i = 0; i < _ws[y].length; i++ ) {
            float d = _ws[y][i] - _wi[y][i];
            _wi[y][i] = s._w[y][i];
            _ws[y][i] = s._w[y][i] + d;
          }
          for( int i = 0; i < _bs[y].length; i++ ) {
            float d = _bs[y][i] - _bi[y][i];
            _bi[y][i] = s._b[y][i];
            _bs[y][i] = s._b[y][i] + d;
          }
        }
        return true;
      }
      return false;
    }

    static class Shuttle extends Atomic {
      float[][] _w, _b; // Deltas in, values out
      int[] _counts;

      @Override public Value atomic(Value value) {
        assert _key.home();
        MR2 trainer = MR2._instances.get(_key);
        for( int y = 1; y < trainer._ls.length; y++ ) {
          for( int i = 0; i < _w[y].length; i++ )
            trainer._ls[y]._w[i] += _w[y][i];
          for( int i = 0; i < _b[y].length; i++ )
            trainer._ls[y]._b[i] += _b[y][i];
        }
        for( int y = 1; y < trainer._ls.length; y++ ) {
          _w[y] = trainer._ls[y]._w;
          _b[y] = trainer._ls[y]._b;
        }
        for( int i = 0; i < _counts.length; i += 2 )
          trainer._counts.addAndGet(_counts[i], _counts[i + 1]);
        return null;
      }
    }

    /*
     * Little state machine to ensure only one running.
     */
    static final int IDLE = 0, RUNNING = 1, RUNNING_SCHEDULED = 2;

    final boolean run() {
      for( ;; ) {
        int state = get();
        switch( state ) {
          case IDLE: {
            if( compareAndSet(state, RUNNING) ) {
              return true;
            }
            break;
          }
          case RUNNING: {
            if( compareAndSet(state, RUNNING_SCHEDULED) )
              return false;
            break;
          }
          case RUNNING_SCHEDULED:
            return false;
        }
      }
    }

    final boolean done() {
      for( ;; ) {
        int state = get();
        switch( state ) {
          case RUNNING: {
            if( compareAndSet(state, IDLE) )
              return true;
            break;
          }
          case RUNNING_SCHEDULED: {
            if( compareAndSet(state, RUNNING) )
              return false;
            break;
          }
        }
      }
    }
  }

  /**
   *
   */
  public static class OpenCL extends Trainer {
    final Layer[] _ls;

    public OpenCL(Layer[] ls) {
      _ls = ls;
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public void run() {
      CLContext context = CLContext.create();
      Log.debug("Created " + context);

      try {
        CLDevice device = context.getMaxFlopsDevice();
        Log.debug("Using " + device);
        CLCommandQueue queue = device.createCommandQueue();

        CLProgram program = context.createProgram(Boot._init.getResource2("/kernels.cl")).build();
        CLKernel[] fprops = new CLKernel[_ls.length];
        CLKernel[] bprops = new CLKernel[_ls.length];
        CLKernel[] resets = new CLKernel[_ls.length];
        CLBuffer<FloatBuffer>[] w = new CLBuffer[_ls.length];
        CLBuffer<FloatBuffer>[] b = new CLBuffer[_ls.length];
        CLBuffer<FloatBuffer>[] a = new CLBuffer[_ls.length];
        CLBuffer<FloatBuffer>[] e = new CLBuffer[_ls.length];
        for( int y = 0; y < _ls.length; y++ ) {
          a[y] = context.createFloatBuffer(_ls[y]._a.length, Mem.READ_WRITE);
          if( y > 0 ) {
            w[y] = context.createFloatBuffer(_ls[y]._w.length, Mem.READ_ONLY);
            b[y] = context.createFloatBuffer(_ls[y]._b.length, Mem.READ_ONLY);
            e[y] = context.createFloatBuffer(_ls[y]._e.length, Mem.READ_ONLY);
            queue.putWriteBuffer(w[y], false);
            queue.putWriteBuffer(b[y], false);

            fprops[y] = program.createCLKernel(_ls.getClass().getSimpleName() + "_fprop");
            fprops[y].putArg(_ls[y - 1]._a.length);
            fprops[y].putArgs(a[y - 1], w[y], b[y], a[y]);

            bprops[y] = program.createCLKernel(_ls.getClass().getSimpleName() + "_bprop");
            bprops[y].putArg(_ls[y - 1]._a.length);
            bprops[y].putArgs(a[y - 1], w[y], b[y], a[y], e[y]);
            bprops[y].putArg(_ls[y]._r);
            if( e[y - 1] != null )
              bprops[y].putArg(e[y - 1]);

            resets[y] = program.createCLKernel("reset_error");
            resets[y].putArg(e[y]);
          }
        }
        int group = device.getMaxWorkGroupSize();
        Input input = (Input) _ls[0];
        for( ;; ) {
          input.fprop();
          for( int i = 0; i < input._a.length; i++ )
            a[0].getBuffer().put(i, input._a[i]);
          queue.putWriteBuffer(a[0], false);
          for( int y = 1; y < fprops.length; y++ )
            queue.put1DRangeKernel(fprops[y], 0, _ls[y]._a.length, group);

          queue.putReadBuffer(a[_ls.length - 1], true);
          for( int y = 1; y < fprops.length - 1; y++ )
            queue.put1DRangeKernel(resets[y], 0, _ls[y]._a.length, group);
          softmax(input, a[a.length - 1].getBuffer(), e[e.length - 1].getBuffer());
          queue.putWriteBuffer(a[_ls.length - 1], false);
          queue.putWriteBuffer(e[_ls.length - 1], false);

          for( int y = _ls.length - 1; y > 0; y-- )
            queue.put1DRangeKernel(bprops[y], 0, _ls[y]._a.length, group);
          input.move();
        }
      } catch( IOException ex ) {
        throw new RuntimeException(ex);
      } finally {
        context.release();
      }
    }

    static void softmax(Input input, FloatBuffer a, FloatBuffer e) {
      float max = Float.NEGATIVE_INFINITY;
      for( int o = 0; o < a.capacity(); o++ )
        if( max < a.get(o) )
          max = a.get(o);
      float scale = 0;
      for( int o = 0; o < a.capacity(); o++ ) {
        a.put(o, (float) Math.exp(a.get(o) - max));
        scale += a.get(o);
      }
      for( int o = 0; o < a.capacity(); o++ ) {
        a.put(o, a.get(o) / scale);
        e.put(o, (o == input.label() ? 1 : 0) - a.get(o));
      }
    }
  }
}
