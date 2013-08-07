package hex;

import hex.Layer2.Input;

import java.io.IOException;
import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import water.*;
import water.fvec.Chunk;
import water.util.Log;
import water.util.Utils;

import com.jogamp.opencl.*;
import com.jogamp.opencl.CLMemory.Mem;

/**
 * Trains a neural network.
 */
public abstract class Trainer2 {
  final AtomicInteger _count;
  int _batch = 20;
  int _batches;

  public Trainer2(AtomicInteger count) {
    _count = count;
  }

  abstract Layer2[] Layer2s();

  abstract void run();

  public static class Direct extends Trainer2 {
    final Layer2[] _ls;

    public Direct(Layer2[] ls) {
      this(ls, new AtomicInteger());
    }

    public Direct(Layer2[] ls, AtomicInteger count) {
      super(count);
      _ls = ls;
    }

    @Override Layer2[] Layer2s() {
      return _ls;
    }

    @Override void run() {
      for( int batch = 0; _batches == 0 || batch < _batches; batch++ ) {
        Input input = (Input) _ls[0];
        for( int b = 0; b < _batch; b++ ) {
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
          input._n = input._n == input._count - 1 ? 0 : input._n + 1;
        }

        for( int i = 1; i < _ls.length; i++ )
          _ls[i].adjust(_count.get());

        _count.addAndGet(_batch);
      }
    }

    void fprop() {
      for( int i = 0; i < _ls.length; i++ )
        _ls[i].fprop();
    }

    void bprop() {
      for( int i = _ls.length - 1; i > 0; i-- )
        _ls[i].bprop();
    }
  }

  /**
   * Runs several trainers in parallel.
   */
  public static class ParallelTrainers extends Trainer2 {
    final Direct[] _trainers;
    final Thread[] _threads;

    public ParallelTrainers(Layer2[] ls) {
      this(ls, 1, 0);
    }

    public ParallelTrainers(Layer2[] ls, int nodes, int index) {
      super(new AtomicInteger());
      _trainers = new Direct[Runtime.getRuntime().availableProcessors()];
      _threads = new Thread[_trainers.length];
      for( int t = 0; t < _trainers.length; t++ ) {
        Layer2[] clones = new Layer2[ls.length];
        for( int i = 0; i < ls.length; i++ )
          clones[i] = Utils.deepClone(ls[i], "_w", "_b");
        for( int i = 1; i < ls.length; i++ )
          clones[i]._in = clones[i - 1];
        _trainers[t] = new Direct(clones, _count);
        _trainers[t]._batches = _batches / _trainers.length;
        Input input = (Input) _trainers[t]._ls[0];
        int chunks = nodes * _trainers.length;
        input._n = (int) (input._count * ((long) index * _trainers.length + t) / chunks);

        final Direct d = _trainers[t];
        _threads[t] = new Thread("H2O Trainer " + t) {
          @Override public void run() {
            d.run();
          }
        };
      }
    }

    @Override Layer2[] Layer2s() {
      return _trainers[0]._ls;
    }

    @Override void run() {
      start();
      join();
    }

    void start() {
      for( int t = 0; t < _threads.length; t++ )
        _threads[t].start();
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

  /**
   * Process items of a batch in parallel.
   */
  public static class ParallelBatch extends Trainer2 {
    final Thread0[] _threads;
    final CyclicBarrier _wait, _done;
    final int _perThread;

    public ParallelBatch(Layer2[] ls) {
      super(new AtomicInteger());

      _threads = new Thread0[Runtime.getRuntime().availableProcessors()];
      _wait = new CyclicBarrier(_threads.length + 1);
      _done = new CyclicBarrier(_threads.length + 1);

      assert _batch % _threads.length == 0;
      _perThread = _batch / _threads.length;

      for( int t = 0; t < _threads.length; t++ ) {
        Layer2[] clones = new Layer2[ls.length];
        for( int i = 0; i < clones.length; i++ )
          clones[i] = Utils.deepClone(ls[i], "_w", "_b");
        _threads[t] = new Thread0(clones, t);
      }
    }

    @Override Layer2[] Layer2s() {
      return _threads[0]._ls;
    }

    @Override void run() {
      for( int t = 0; t < _threads.length; t++ )
        _threads[t].start();
      for( int i = 0; i < _threads.length; i++ ) {
        try {
          _threads[i].join();
        } catch( InterruptedException e ) {
          throw new RuntimeException(e);
        }
      }
    }

    final class Thread0 extends Thread {
      final Layer2[] _ls;
      int _n;

      Thread0(Layer2[] ls, int n) {
        _ls = ls;
        _n = n;
      }
    }
  }

  //

  /**
   *
   */
  public static class Distributed extends Trainer2 {
    private static final ConcurrentHashMap<Key, Distributed> _instances = new ConcurrentHashMap<Key, Distributed>();
    private Layer2[] _ls;
    private Parameters[] _ps;

    public Distributed(Layer2[] ls) {
      super(new AtomicInteger());
      _ls = ls;
    }

    @Override Layer2[] Layer2s() {
      return _ls;
    }

    @Override void run() {
      Key key = Key.make();
      try {
        _instances.put(key, this);
        Parameters p = new Parameters();
        p._ws = new float[_ls.length][];
        p._bs = new float[_ls.length][];
        for( int i = 0; i < _ls.length; i++ ) {
          p._ws[i] = _ls[i]._w;
          p._bs[i] = _ls[i]._b;
        }
        UKV.put(key, p);
        H2ONode[] nodes = H2O.CLOUD._memary;
        RPC<Task>[] tasks = new RPC[nodes.length];
        for( int i = 0; i < nodes.length; i++ ) {
          _ps[i] = Utils.deepClone(p);
          tasks[i] = RPC.call(nodes[i], new Task(_ls, key, i));
        }
        for( int i = 0; i < nodes.length; i++ )
          tasks[i].get();
      } finally {
        _instances.remove(key);
      }
    }
  }

  static class Parameters extends Iced {
    float[][] _ws, _bs;
  }

  static class Task extends DTask<Task> {
    Layer2[] _ls;
    Key _ws;
    int _index;

    private Task(Layer2[] ls, Key ws, int index) {
      _ls = ls;
      _ws = ws;
      _index = index;
    }

    @Override public void compute2() {
      Parameters p = UKV.get(_ws);
      for( int y = 1; y < _ls.length; y++ ) {
        _ls[y]._in = _ls[y - 1];
        _ls[y]._w = p._ws[y].clone();
        _ls[y]._b = p._bs[y].clone();
        _ls[y].init(true);
      }
      ParallelTrainers t = new ParallelTrainers(_ls, H2O.CLOUD._memary.length, _index);
      t.start();
      for( ;; ) {
        Parameters delta = new Parameters();
        for( int y = 1; y < _ls.length; y++ ) {
          delta._ws[y] = new float[_ls[y]._w.length];
          delta._bs[y] = new float[_ls[y]._b.length];
          for( int i = 0; i < _ls[y]._w.length; i++ )
            delta._ws[y][i] = _ls[y]._w[i] - p._ws[y][i];
          for( int i = 0; i < _ls[y]._b.length; i++ )
            delta._bs[y][i] = _ls[y]._b[i] - p._bs[y][i];
        }
        Update u = new Update();
        u._delta = delta;
        u.invoke(_ws);
      }
    }
  }

  static class Update extends TAtomic<Parameters> {
    Parameters _delta;

    @Override public Parameters atomic(Parameters old) {
      for( int y = 1; y < old._ws.length; y++ ) {
        for( int i = 0; i < _ls[y]._w.length; i++ )
          old._ws[y][i] += _ls[y]._w[i] - p._ws[y][i];
      }
      for( int y = 1; y < old._bs.length; y++ ) {
        delta._bs[y] = new float[_ls[y]._b.length];
        for( int i = 0; i < _ls[y]._b.length; i++ )
          delta._bs[y][i] = _ls[y]._b[i] - p._bs[y][i];
      }
      return delta;
    }
  }

  /**
   *
   */
  public static class TrainerMR extends Trainer2 {
    private Layer2[] _ls;

    public TrainerMR(Layer2[] ls) {
      super(new AtomicInteger());
      _ls = ls;
    }

    @Override Layer2[] Layer2s() {
      return _ls;
    }

    @Override void run() {
      Weights w = new Weights();
      //w._w =
      Key ls = Key.make();
      UKV.put(ls, w);
      Pass pass = new Pass();
      //pass.doAll(X,Y);
    }
  }

  static class Pass extends MRTask2<Pass> {
    Key _ls;

    @Override public void map(Chunk[] bvs) {
      ;
    }

    @Override public void reduce(Pass mrt) {
      ;
    }
  }

  /**
   *
   */
  public static class OpenCL extends Trainer2 {
    final Layer2[] _ls;

    public OpenCL(Layer2[] ls) {
      super(new AtomicInteger());
      _ls = ls;
    }

    @Override Layer2[] Layer2s() {
      return _ls;
    }

    @Override void run() {
      CLContext context = CLContext.create();
      Log.debug("Created " + context);

      try {
        CLDevice device = context.getMaxFlopsDevice();
        Log.debug("Using " + device);
        CLCommandQueue queue = device.createCommandQueue();

        CLProgram program = context.createProgram(Boot._init.getResource2("/kernels.cl")).build();
        CLKernel[] fprops = new CLKernel[_ls.length];
        CLKernel[] bprops = new CLKernel[_ls.length];
        CLBuffer<FloatBuffer>[] a = new CLBuffer[_ls.length];
        CLBuffer<FloatBuffer>[] e = new CLBuffer[_ls.length];
        for( int y = 0; y < _ls.length; y++ ) {
          fprops[y] = program.createCLKernel(_ls.getClass().getSimpleName() + "_fprop");
          bprops[y] = program.createCLKernel(_ls.getClass().getSimpleName() + "_bprop");
          if( y > 0 ) {
            CLBuffer<FloatBuffer> ia = context.createFloatBuffer(_ls[y - 1]._a.length, Mem.READ_ONLY);
            CLBuffer<FloatBuffer> w = context.createFloatBuffer(_ls[y]._w.length, Mem.READ_ONLY);
            CLBuffer<FloatBuffer> b = context.createFloatBuffer(_ls[y]._b.length, Mem.READ_ONLY);
            a[y] = context.createFloatBuffer(_ls[y]._a.length, Mem.READ_WRITE);
            fprops[y].putArgs(ia, w, b, a[y]);

            CLBuffer<FloatBuffer> gw = context.createFloatBuffer(_ls[y]._gw.length, Mem.READ_WRITE);
            CLBuffer<FloatBuffer> gb = context.createFloatBuffer(_ls[y]._gb.length, Mem.READ_WRITE);
            e[y] = context.createFloatBuffer(_ls[y]._e.length, Mem.READ_ONLY);
            CLBuffer<FloatBuffer> ie = context.createFloatBuffer(_ls[y]._e.length, Mem.READ_WRITE);
            bprops[y].putArgs(w, gw, gb, a[y], e[y], ia, ie);

            queue.putWriteBuffer(w, false);
            queue.putWriteBuffer(b, false);
          }
        }
        int group = device.getMaxWorkGroupSize();
        for( int batch = 0; _batches == 0 || batch < _batches; batch++ ) {
          Input input = (Input) _ls[0];
          for( int b = 0; b < _batch; b++ ) {
//            putSample(a[a.length - 1].getBuffer(), input._n);

            for( int y = 0; y < fprops.length; y++ )
              queue.put1DRangeKernel(fprops[y], 0, _ls[y]._a.length, group);

            queue.putReadBuffer(a[_ls.length - 1], true);
            for( int y = 1; y < _ls.length - 1; y++ ) {
              FloatBuffer buffer = e[y].getBuffer();
              for( int i = 0; i < buffer.capacity(); i++ )
                buffer.put(i, 0);
            }
            FloatBuffer err = e[e.length - 1].getBuffer();
            FloatBuffer act = a[a.length - 1].getBuffer();
            for( int i = 0; i < err.capacity(); i++ )
              err.put(i, (i == input.label() ? .9f : -.1f) - act.get(i));

            queue.putWriteBuffer(a[_ls.length - 1], false);
            for( int y = _ls.length - 1; y > 0; y-- )
              queue.put1DRangeKernel(bprops[y], 0, _ls[y]._a.length, group);
            input._n = input._n == input._count - 1 ? 0 : input._n + 1;
          }

          for( int i = 1; i < _ls.length; i++ )
            _ls[i].adjust(_count.get());

          _count.addAndGet(_batch);
        }
      } catch( IOException ex ) {
        throw new RuntimeException(ex);
      } finally {
        context.release();
      }
    }
  }
}