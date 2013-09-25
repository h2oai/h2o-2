package hex;

import hex.Layer.ChunksInput;
import hex.Layer.Input;
import hex.Layer.Softmax;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;

import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;

import water.*;
import water.Job.ModelJob;
import water.api.*;
import water.fvec.*;
import water.util.RString;

/**
 * Neural network.
 *
 * @author cypof
 */
public class NeuralNet extends ModelJob {
  static final int API_WEAVER = 1;
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "Neural Network";
  public static final String KEY_PREFIX = "__NeuralNet_";
  public static final int EVAL_ROW_COUNT = 1000;

  public enum Activation {
    Tanh, Rectifier
  };

//@formatter:off
  @API(help = "Activation function", filter = activationFilter.class)
  public Activation activation;
  class activationFilter extends EnumArgument<Activation> { public activationFilter() { super(Activation.Tanh); } }

  @API(help = "Hidden layer sizes", filter = hiddenFilter.class)
  public int[] hidden;
  class hiddenFilter extends RSeq { public hiddenFilter() { super("500", false); } }
//@formatter:on

  @API(help = "Learning rate", filter = Default.class)
  public double rate = .01;

  @API(help = "L2 regularization", filter = Default.class)
  public double l2 = .0001;

  @API(help = "How many times the dataset should be iterated", filter = Default.class)
  public int epochs = 100;

  public NeuralNet() {
    super(DOC_GET, Key.make(KEY_PREFIX + Key.make()));
  }

  @Override protected void run() {
    for( int i = cols.length - 1; i >= 0; i-- )
      if( source.vecs()[cols[i]] == response )
        cols = ArrayUtils.remove(cols, i);
    Vec[] vecs = new Vec[cols.length];
    for( int i = 0; i < cols.length; i++ )
      vecs[i] = source.vecs()[cols[i]];
    vecs = reChunk(vecs);

    final Layer[] ls = new Layer[hidden.length + 2];
    ls[0] = new VecsInput(vecs);
    for( int i = 0; i < hidden.length; i++ ) {
      if( activation == Activation.Rectifier )
        ls[i + 1] = new Layer.Rectifier(hidden[i]);
      else
        ls[i + 1] = new Layer.Tanh(hidden[i]);
      ls[i + 1]._rate = (float) rate;
      ls[i + 1]._l2 = (float) l2;
    }
    ls[ls.length - 1] = new VecSoftmax(response);
    ls[ls.length - 1]._rate = (float) rate;
    ls[ls.length - 1]._l2 = (float) l2;
    for( int i = 0; i < ls.length; i++ )
      ls[i].init(ls, i);

    final Key sourceKey = Key.make(input("source"));
    String[] names = new String[cols.length];
    for( int i = 0; i < cols.length; i++ )
      names[i] = source._names[cols[i]];
    String responseName = "response";
    for( int i = 0; i < source.numCols(); i++ )
      if( source.vecs()[i] == response )
        responseName = source._names[i];
    final Frame frame = new Frame(names, vecs);
    frame.add(responseName, response);
    NeuralNetModel model = new NeuralNetModel(destination_key, sourceKey, frame, ls);
    UKV.put(destination_key, model);

    final Trainer trainer = new Trainer.MapReduce(ls, epochs, self());
    trainer.start();

    // Use a separate thread for monitoring (blocked most of the time)
    Thread thread = new Thread() {
      @Override public void run() {
        long lastTime = System.nanoTime();
        long lastItems = 0;
        while( running() ) {
          long time = System.nanoTime();
          double delta = (time - lastTime) / 1e9;
          long items = trainer.items();
          int ps = (int) ((items - lastItems) / delta);
          lastTime = time;
          lastItems = items;

          NeuralNetModel model = new NeuralNetModel(destination_key, sourceKey, frame, ls);
          int[][] confusion = new int[model.classNames().length][model.classNames().length];
          Error train = NeuralNetScore.eval(ls, EVAL_ROW_COUNT, confusion);
          model.items = items;
          model.items_per_second = ps;
          model.train_classification_error = train.Value;
          model.train_sqr_error = train.SqrDist;
          model.confusionMatrix = confusion;
          UKV.put(destination_key, model);

          try {
            Thread.sleep(2000);
          } catch( InterruptedException e ) {
            throw new RuntimeException(e);
          }
        }
      }
    };
    thread.start();
    trainer.join();
  }

  @Override public float progress() {
    NeuralNetModel model = UKV.get(destination_key);
    if( model == null )
      return 0;
    return 0.1f + Math.min(1, model.items / (float) (epochs * source.anyVec().length()));
  }

  @Override protected Response redirect() {
    String n = NeuralNetProgress.class.getSimpleName();
    return new Response(Response.Status.redirect, this, -1, -1, n, "job", job_key, "dst_key", destination_key);
  }

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='NeuralNet.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", "source");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public static class Error {
    double Value;
    double SqrDist;

    @Override public String toString() {
      return String.format("%.2f", (100 * Value)) + "% (d²:" + String.format("%.2e", SqrDist) + ")";
    }
  }

  public static class NeuralNetModel extends Model {
    @API(help = "Layers")
    public Layer[] layers;

    @API(help = "Layer weights")
    public float[][] ws, bs;

    @API(help = "How many items have been processed")
    public long items;

    @API(help = "Training speed")
    public int items_per_second;

    @API(help = "Classification error on the training set (Estimation)")
    public double train_classification_error = 1;

    @API(help = "Square distance error on the training set (Estimation)")
    public double train_sqr_error;

    @API(help = "Classification error on the validation set (Estimation)")
    public double validation_classification_error;

    @API(help = "Square distance error on the validation set (Estimation)")
    public double validation_sqr_error;

    @API(help = "Confusion matrix")
    public int[][] confusionMatrix;

    NeuralNetModel(Key selfKey, Key dataKey, Frame fr, Layer[] ls) {
      super(selfKey, dataKey, fr);

      VecsInput input = (VecsInput) ls[0];
      layers = Layer.clone(ls, new ChunksInput(null, input), 0);
      ws = new float[ls.length][];
      bs = new float[ls.length][];
      for( int y = 1; y < layers.length; y++ ) {
        ws[y] = layers[y]._w;
        bs[y] = layers[y]._b;
      }
    }

    @Override protected float[] score0(Chunk[] chunks, int rowInChunk, double[] tmp, float[] preds) {
      ChunksInput input = (ChunksInput) layers[0];
      input.init(null, input._means.length);
      for( int y = 1; y < layers.length; y++ ) {
        layers[y]._w = ws[y];
        layers[y]._b = bs[y];
        layers[y].init(layers[y - 1], bs[y].length, false, 0);
      }
      input._chunks = chunks;
      input._pos = rowInChunk;
      for( int i = 0; i < layers.length; i++ )
        layers[i].fprop();
      float[] out = layers[layers.length - 1]._a;
      assert out.length == preds.length;
      return out;
    }

    @Override protected float[] score0(double[] data, float[] preds) {
      throw new UnsupportedOperationException();
    }
  }

  public static class NeuralNetProgress extends Progress2 {
    @Override protected String name() {
      return DOC_GET;
    }

    @Override public boolean toHTML(StringBuilder sb) {
      NeuralNetModel model = UKV.get(Key.make(dst_key.value()));
      if( model != null ) {
        String train = String.format("%5.2f %%", 100 * model.train_classification_error);
        String valid = String.format("%5.2f %%", 100 * model.validation_classification_error);
        DocGen.HTML.section(sb, "Training classification error: " + train);
        DocGen.HTML.section(sb, "Training square error: " + model.train_sqr_error);
        DocGen.HTML.section(sb, "Validation classification error: N/A");// + valid);
        DocGen.HTML.section(sb, "Validation square error: N/A");// + model.train_sqr_error);
        DocGen.HTML.section(sb, "Items: " + model.items);
        DocGen.HTML.section(sb, "Items per second: " + model.items_per_second);

        //class='btn btn-danger btn-mini'
//        sb.append("<a href='Suspend.html?" + job + "'><button>Suspend</button></a>");
//        sb.append("&nbsp;&nbsp;");
//        sb.append("<a href='Resume.html?" + job + "'><button>Resume</button></a>");
//        sb.append("\n");

        if( model.confusionMatrix != null ) {
          String title = "Confusion Matrix (Training Data)";
          NeuralNetScore.confusion(sb, title, model.classNames(), model.confusionMatrix);
        }
      }
      return true;
    }

    @Override protected Response jobDone(final Job job, final String dst) {
      return new Response(Response.Status.done, this, 0, 0, null);
    }
  }

  public static class NeuralNetScore extends Request2 {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    static final String DOC_GET = "Neural network scoring";

    @API(help = "Model", required = true, filter = Default.class)
    public NeuralNetModel model;

    @API(help = "Data", required = true, filter = Default.class)
    public Frame source;

    @API(help = "Rows to consider for scoring, 0 (default) means the whole frame", filter = Default.class)
    public long max_rows;

    @API(help = "Classification error")
    public double classification_error;

    @API(help = "Square distance error")
    public double sqr_error;

    @API(help = "Confusion matrix")
    public int[][] confusionMatrix;

    @Override protected Response serve() {
      FrameInput input = new FrameInput(source);
      Layer[] layers = Layer.clone(model.layers, input, 0);
      for( int y = 1; y < layers.length; y++ ) {
        layers[y]._w = model.ws[y];
        layers[y]._b = model.bs[y];
      }
      confusionMatrix = new int[classes][classes];
      Error error = eval(layers, max_rows, confusionMatrix);
      classification_error = error.Value;
      sqr_error = error.SqrDist;
      return new Response(Response.Status.done, this, -1, -1, null);
    }

    public static Error eval(Layer[] ls, long max_rows, int[][] confusion) {
      Input input = (Input) ls[0];
      Error error = new Error();
      long len = input._len;
      if( max_rows != 0 )
        len = Math.min(len, max_rows);
      int correct = 0;
      for( input._pos = 0; input._pos < len; input._pos++ )
        if( correct(ls, error, confusion) )
          correct++;
      error.Value = (len - (double) correct) / len;
      return error;
    }

    private static boolean correct(Layer[] ls, Error error, int[][] confusion) {
      Softmax output = (Softmax) ls[ls.length - 1];
      for( int i = 0; i < ls.length; i++ )
        ls[i].fprop();
      float[] out = ls[ls.length - 1]._a;
      error.SqrDist = 0;
      for( int i = 0; i < out.length; i++ ) {
        float t = i == output.label() ? 1 : 0;
        float d = t - out[i];
        error.SqrDist += d * d;
      }
      float max = Float.MIN_VALUE;
      int idx = -1;
      for( int i = 0; i < out.length; i++ ) {
        if( out[i] > max ) {
          max = out[i];
          idx = i;
        }
      }
      if( confusion != null )
        confusion[output.label()][idx]++;
      return idx == output.label();
    }

    @Override public boolean toHTML(StringBuilder sb) {
      DocGen.HTML.section(sb, "Classification error: " + String.format("%5.2f %%", 100 * classification_error));
      DocGen.HTML.section(sb, "Square error: " + sqr_error);
      String[] classes = source.vecs()[source.numCols() - 1].domain();
      if( classes == null )
        classes = Model.responseDomain(source);
      confusion(sb, "Confusion Matrix", classes, confusionMatrix);
      return true;
    }

    static void confusion(StringBuilder sb, String title, String[] classes, int[][] confusionMatrix) {
      sb.append("<h3>" + title + "</h3>");
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      sb.append("<tr><th>Actual \\ Predicted</th>");
      for( String c : classes )
        sb.append("<th>" + c + "</th>");
      sb.append("<th>Error</th></tr>");
      long[] totals = new long[classes.length];
      long sumTotal = 0;
      long sumError = 0;
      for( int crow = 0; crow < classes.length; ++crow ) {
        long total = 0;
        long error = 0;
        sb.append("<tr><th>" + classes[crow] + "</th>");
        for( int ccol = 0; ccol < classes.length; ++ccol ) {
          int num = confusionMatrix[crow][ccol];
          total += num;
          totals[ccol] += num;
          if( ccol == crow ) {
            sb.append("<td style='background-color:LightGreen'>");
          } else {
            sb.append("<td>");
            error += num;
          }
          sb.append(num);
          sb.append("</td>");
        }
        sb.append("<td>");
        sb.append(String.format("%5.3f = %d / %d", (double) error / total, error, total));
        sb.append("</td></tr>");
        sumTotal += total;
        sumError += error;
      }
      sb.append("<tr><th>Totals</th>");
      for( int i = 0; i < totals.length; ++i )
        sb.append("<td>" + totals[i] + "</td>");
      sb.append("<td><b>");
      sb.append(String.format("%5.3f = %d / %d", (double) sumError / sumTotal, sumError, sumTotal));
      sb.append("</b></td></tr>");
      sb.append("</table>");
    }
  }

  static int cores() {
    int cores = 0;
    for( H2ONode node : H2O.CLOUD._memary )
      cores += node._heartbeat._num_cpus;
    return cores;
  }

  /**
   * Makes sure small datasets are spread over enough chunks to parallelize training. Neural nets
   * can require lots of processing even for small data.
   */
  public static Vec[] reChunk(Vec[] vecs) {
    final int splits = cores() * 2; // More in case of unbalance
    if( vecs[0].nChunks() >= splits )
      return vecs;
    for( int v = 0; v < vecs.length; v++ ) {
      AppendableVec vec = new AppendableVec(UUID.randomUUID().toString());
      long rows = vecs[0].length();
      Chunk cache = null;
      for( int split = 0; split < splits; split++ ) {
        long off = rows * (split + 0) / splits;
        long lim = rows * (split + 1) / splits;
        NewChunk chunk = new NewChunk(vec, split);
        for( long r = off; r < lim; r++ ) {
          if( cache == null || r < cache._start || r >= cache._start + cache._len )
            cache = vecs[v].chunk(r);
          if( !cache.isNA(r) ) {
            if( vecs[v]._domain != null )
              chunk.addEnum((int) cache.at8(r));
            else if( vecs[v].isInt() )
              chunk.addNum(cache.at8(r), 0);
            else
              chunk.addNum(cache.at(r));
          } else {
            if( vecs[v].isInt() )
              chunk.addNA();
            else {
              // Don't use addNA() for doubles, as NewChunk uses separate array
              chunk.addNum(Double.NaN);
            }
          }
        }
        chunk.close(split, null);
      }
      Vec t = vec.close(null);
      t._domain = vecs[v]._domain;
      vecs[v] = t;
    }
    return vecs;
  }
}
