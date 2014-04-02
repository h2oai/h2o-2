package hex;

import com.jogamp.opencl.*;
import com.jogamp.opencl.CLMemory.Mem;
import hex.Layer.*;
import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;
import water.util.Utils;

import java.io.IOException;
import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Trains a neural network.
 *
 * @author cypof
 */
public abstract class Trainer {
  Trainer() {
  }

  public abstract Layer[] layers();

  public abstract void start();

  public abstract void join();

  public long processed() {
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

    @Override public void start() {
      throw new UnsupportedOperationException();
    }

    @Override public void join() {
      throw new UnsupportedOperationException();
    }

    final void step(long seed) {
//      Log.info("step with seed " + seed);
      fprop(seed);
      for( int i = 1; i < _ls.length - 1; i++ )
        Arrays.fill(_ls[i]._e, 0);
      bprop();
    }

    final void fprop(long seed) {
      for (Layer _l : _ls) _l.fprop(seed, true);
    }

    final void bprop() {
      for( int i = _ls.length - 1; i > 0; i-- )
        _ls[i].bprop();
    }
  }

  /**
   * Trains NN on current thread.
   */
  public static class Direct extends Base {
    long _processed, _limit;
    Thread _thread;
    Key _job;

    public Direct(Layer[] ls, double epochs, Key job) {
      super(ls);
      _limit = (long) Math.ceil(epochs * ((Input) ls[0])._len);
      _job = job;
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    public void run() {
      Training training = new Training() {
        @Override long processed() {
          return _processed;
        }
      };
      for (Layer _l : _ls) _l._training = training;

      Input input = (Input) _ls[0];
      for( ; _limit == 0 || _processed < _limit; _processed++ ) {
        step(_processed);
        input.move();
        if( _job != null && (!Job.isRunning(_job) || !NeuralNet.running ) )
          break;
      }
    }

    @Override public long processed() {
      return _processed;
    }

    @Override public void start() {
      _thread = new Thread() {
        @Override public void run() {
          Direct.this.run();
        }
      };
      _thread.start();
    }

    @Override public void join() {
      try {
        _thread.join();
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Runs several trainers in parallel on the same weights, using threads. Only works on one node.
   */
  public static class Threaded extends Trainer {
    final Base[] _trainers;
    final Thread[] _threads;
    final long _stepsPerThread;
    final AtomicLong _processed = new AtomicLong();

    public Threaded(Layer[] ls, double epochs, final Key job, int threads) {
      int num_threads = threads > 0 ? threads : Runtime.getRuntime().availableProcessors();
      _trainers = new Base[num_threads];
      _threads = new Thread[num_threads];
      _stepsPerThread = (long) (epochs * ((Input) ls[0])._len / num_threads);

      Log.info("Starting " + num_threads + " threads.");
      for( int t = 0; t < num_threads; t++ ) {
        Layer[] clones = new Layer[ls.length];
        for( int y = 0; y < clones.length; y++ )
          clones[y] = ls[y].clone();
        for( int y = 0; y < clones.length; y++ ) {
          clones[y].init(clones, y, false);
          clones[y]._training = new Training() {
            @Override long processed() {
              return _processed.get();
            }
          };
        }
        final Input input = (Input) clones[0];
        input._pos = input._len * t / num_threads;
        _trainers[t] = new Base(clones);
        final Base trainer = _trainers[t];

        final int thread_num = t;
        _threads[t] = new Thread("H2O Trainer " + t) {
          @Override public void run() {
            for( long i = 0; _stepsPerThread == 0 || i < _stepsPerThread; i++ ) {
              if( job != null && (!Job.isRunning(job) || !NeuralNet.running ) )
                break;
              try {
//                long seed = thread_num * _stepsPerThread + input._pos; //BAD
                long seed = new Random().nextLong(); //GOOD
//                long seed = thread_num * _stepsPerThread + _processed.get(); //TRY
                trainer.step(seed);
                input.move();
                _processed.incrementAndGet();
              } catch (Exception e) {
                e.getStackTrace();
              }
            }
          }
        };
      }
    }

    @Override public Layer[] layers() {
      return _trainers[0].layers();
    }

    @Override public long processed() {
      return _processed.get();
    }

    @Override public void start() {
      for (Thread _thread : _threads) _thread.start();
    }

    @Override public void join() {
      for (Thread _thread : _threads) {
        try {
          _thread.join();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    public void run() {
      start();
      join();
    }

  }

  /**
   * Distributed trainer. All tasks on a node update the same weights, like Threaded. Updates
   * between nodes are synchronized at regular intervals by exchanging messages between the
   * initiating machine and others. Requires input to be Frame.
   */
  public static class MapReduce extends Trainer {
    static final ConcurrentHashMap<Key, MapReduce> _instances = new ConcurrentHashMap<Key, MapReduce>();

    Layer[] _ls;
    double _epochs;
    Key _job;
    AtomicIntegerArray _counts;
    transient Key _key;
    transient Descent _task;

    public MapReduce(Layer[] ls, double epochs, Key job) {
      _ls = ls;
      _epochs = epochs;
      _job = job;

      _key = Key.make((byte) 1, Key.DFJ_INTERNAL_USER, H2O.SELF);
      _instances.put(_key, this);
      DKV.put(_key, new Value(_key, new byte[0]));

      Vec[] vecs = ((VecsInput) ls[0]).vecs;
      assert ls[0]._a.length == VecsInput.expand(vecs);
      //assert vecs[0].nChunks() >= NeuralNet.cores() : "Not enough chunks, c.f. NeuralNet.reChunk";
      _counts = new AtomicIntegerArray(vecs[0].nChunks());
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public long processed() {
      Vec[] vecs = ((VecsInput) _ls[0]).vecs;
      long n = 0;
      for( int i = 0; i < _counts.length(); i++ )
        n += _counts.get(i) * vecs[0].chunkLen(i);
      return n;
    }

    @Override public void start() {
      // TODO? Chunk weights over all nodes
      // _keys = new Key[H2O.CLOUD._memary.length];
      // Weights[] weights = new Weights[_keys.length];

      _task = new Descent();
      _task._job = _job;
      _task._ls = _ls;
      _task._key = _key;
      _task._epochs = _epochs;
      _task._ws = new float[_ls.length][];
      _task._bs = new float[_ls.length][];
      for( int y = 1; y < _ls.length; y++ ) {
        _task._ws[y] = _ls[y]._w;
        _task._bs[y] = _ls[y]._b;
      }
      Vec[] vecs = ((VecsInput) _ls[0]).vecs;
      Layer out = _ls[_ls.length - 1];
      Vec response = out instanceof VecSoftmax ? ((VecSoftmax) out).vec : ((VecLinear) out)._vec;
      _task.dfork(new Frame(null, Utils.append(vecs, response)));
    }

    @Override public void join() {
      _task.join();
    }

    public void run() {
      start();
      join();
      while (NeuralNet.running) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    void done() {
      NeuralNet.running = false;
      _instances.remove(_key);
      UKV.remove(_key);
      if( _job != null ) {
        Job job = Job.findJob(_job);
        if( job != null ) {
          H2OCountedCompleter task = job._fjtask;
          if( task != null )
            task.tryComplete();
          job.remove();
        }
      }
    }
  }

  static class Descent extends MRTask2<Descent> {
    Key _job;
    Layer[] _ls;
    float[][] _ws;
    float[][] _bs;
    Key _key;
    double _epochs;
    transient NodeDescent _node;
    transient volatile boolean _done;

    @Override protected void setupLocal() {
      _node = new NodeDescent(_job, _ls, _ws, _bs, _key);

      // Separate thread for more regular latency
      final boolean home = _key.home();
      Thread thread = new Thread() {
        @Override public void run() {
          while( _job == null || Job.isRunning(_job) ) {
            if( !home )
              _node.sync();
            else {
              _node._total = _node._trainer.processed();
              try {
                Thread.sleep(1);
              } catch( InterruptedException ex ) {
              }
            }
          }
        }
      };
      thread.setDaemon(true);
      thread.start();
    }

    @Override protected void closeLocal() {
      // Launch actual computation in order, otherwise passes
      // between chunks diverge quickly
      DescentEpoch epoch = new DescentEpoch();
      epoch._node = _node;
      epoch._count = _epochs == 0. ? -1 : (int)Math.ceil(_epochs);
      H2O.submitTask(epoch);
      _ls = null;
      _ws = null;
      _bs = null;
      _key = null;
    }

    @Override public void map(Chunk[] cs) {
      _node._chunks.add(cs);
    }
  }

  private static abstract class NodeTask extends H2OCountedCompleter {
    NodeDescent _node;

    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
      String error = Utils.getStackAsString(ex);
      Log.info(error);
      if( _node._job != null )
        Job.findJob(_node._job).cancel(error);
      return super.onExceptionalCompletion(ex, caller);
    }
  }

  private static class DescentEpoch extends NodeTask {
    int _count;

    @Override public void compute2() {
      if( (_count < 0 || --_count >= 0) && (_node._job == null || Job.isRunning(_node._job)) ) {
        for( Chunk[] cs : _node._chunks ) {
          DescentChunk task = new DescentChunk();
          task._node = _node;
          task._cs = cs;
          H2O.submitTask(task);
        }
        reinitialize();
        H2O.submitTask(this);
      } else {
        if( _node._key.home() )
          _node._trainer.done();
      }
    }
  }

  static class DescentChunk extends NodeTask {
    Chunk[] _cs;

    @Override public void compute2() {
      if( _node._job == null || (Job.isRunning(_node._job) && NeuralNet.running)) {
        Layer[] clones = new Layer[_node._ls.length];
        ChunksInput input = new ChunksInput(Utils.remove(_cs, _cs.length - 1), (VecsInput) _node._ls[0]);
        clones[0] = input;
        for( int y = 1; y < _node._ls.length - 1; y++ )
          clones[y] = _node._ls[y].clone();
        Layer output = _node._ls[_node._ls.length - 1];
        if( output instanceof VecSoftmax )
          clones[clones.length - 1] = new ChunkSoftmax(_cs[_cs.length - 1], (VecSoftmax) output);
        else
          clones[clones.length - 1] = new ChunkLinear(_cs[_cs.length - 1], (VecLinear) output);

        // create new _a and _e, but link to weights/bias from _node (Hogwild)
        for( int y = 0; y < clones.length; y++ ) {
          clones[y].init(clones, y, false);
          clones[y]._w = _node._ws[y];
          clones[y]._b = _node._bs[y];
          clones[y]._wm = _node._wm[y];
          clones[y]._bm = _node._bm[y];
          clones[y]._training = new Training() {
            @Override long processed() {
              return _node._total;
            }
          };
        }
        Base base = new Base(clones);
        for( input._pos = 0; input._pos < _cs[0]._len; input._pos++ )
          base.step(new Random().nextLong()); //warning: no reproducible seeding
        int chunk = _cs[0].cidx();
        _node.stepped(chunk);
      }
      tryComplete();
    }
  }

  static class NodeDescent {
    ConcurrentLinkedQueue<Chunk[]> _chunks = new ConcurrentLinkedQueue<Chunk[]>();
    Key _job;
    Layer[] _ls;
    float[][] _ws; // Current weights
    float[][] _bs; // Current bias
    float[][] _wi; // Initial weights, for synchronization
    float[][] _bi; // Initial biases, for synchronization
    float[][] _wm; // Momentums
    float[][] _bm; // Momentums
    Key _key;
    ConcurrentHashMap<Integer, Integer> _counters;
    MapReduce _trainer;
    long _total;

    NodeDescent(Key job, Layer[] ls, float[][] ws, float[][] bs, Key key) {
      _job = job;
      _ls = ls;
      _key = key;
      _ws = ws;
      _bs = bs;
      _wi = new float[ws.length][];
      _bi = new float[bs.length][];
      _wm = new float[ws.length][];
      _bm = new float[bs.length][];
      for( int y = 1; y < _ws.length; y++ ) {
        _wi[y] = ws[y].clone();
        _bi[y] = bs[y].clone();
        if( ls[y].params.momentum_start != 0 || ls[y].params.momentum_stable != 0 ) {
          _wm[y] = new float[ws[y].length];
          _bm[y] = new float[bs[y].length];
        }
      }
      _trainer = MapReduce._instances.get(_key);
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
      int[] counts = new int[10];
      int n = 0;
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
        _total = s._processed;
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
      float[][] _w; // Deltas in, values out
      float[][] _b; // Deltas in, values out
      int[] _counts;
      long _processed;

      @Override public Value atomic(Value value) {
        assert _key.home();
        MapReduce trainer = MapReduce._instances.get(_key);
        if( trainer != null ) {
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
          _counts = null;
          _processed = trainer.processed();
        }
        return null;
      }
    }
  }

  /**
   * GPU based trainer. Alpha code!
   */
  public static class OpenCL extends Trainer {
    final Layer[] _ls;

    public OpenCL(Layer[] ls) {
      _ls = ls;
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public void start() {
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
//            bprops[y].putArg(_ls[y]._r);
            if( e[y - 1] != null )
              bprops[y].putArg(e[y - 1]);

            resets[y] = program.createCLKernel("reset_error");
            resets[y].putArg(e[y]);
          }
        }
        int group = device.getMaxWorkGroupSize();
        Input input = (Input) _ls[0];
        while (true) {
          input.fprop(new Random().nextLong(), true);
          for( int i = 0; i < input._a.length; i++ )
            a[0].getBuffer().put(i, input._a[i]);
          queue.putWriteBuffer(a[0], false);
          for( int y = 1; y < fprops.length; y++ )
            queue.put1DRangeKernel(fprops[y], 0, _ls[y]._a.length, group);

          queue.putReadBuffer(a[_ls.length - 1], true);
          for( int y = 1; y < fprops.length - 1; y++ )
            queue.put1DRangeKernel(resets[y], 0, _ls[y]._a.length, group);
//          softmax(input, a[a.length - 1].getBuffer(), e[e.length - 1].getBuffer());
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

    @Override public void join() {
      throw new UnsupportedOperationException();
    }

//    static void softmax(Input input, FloatBuffer a, FloatBuffer e) {
//      float max = Float.NEGATIVE_INFINITY;
//      for( int o = 0; o < a.capacity(); o++ )
//        if( max < a.get(o) )
//          max = a.get(o);
//      float scale = 0;
//      for( int o = 0; o < a.capacity(); o++ ) {
//        a.put(o, (float) Math.exp(a.get(o) - max));
//        scale += a.get(o);
//      }
//      for( int o = 0; o < a.capacity(); o++ ) {
//        a.put(o, a.get(o) / scale);
//        e.put(o, (o == input.label() ? 1 : 0) - a.get(o));
//      }
//    }
  }
}
