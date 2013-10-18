package hex;

import hex.Layer.ChunkSoftmax;
import hex.Layer.ChunksInput;
import hex.Layer.Input;
import hex.Layer.Output;
import hex.Layer.Softmax;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.H2O.H2OEmptyCompleter;
import water.Job.ValidatedJob;
import water.api.*;
import water.fvec.*;
import water.util.RString;
import water.util.Utils;

/**
 * Neural network.
 *
 * @author cypof
 */
public class NeuralNet extends ValidatedJob {
  static final int API_WEAVER = 1;
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "Neural Network";
  public static final int EVAL_ROW_COUNT = 1000;

  public enum Activation {
    Tanh, Rectifier
  };

  @API(help = "Activation function", filter = Default.class)
  public Activation activation = Activation.Tanh;

  @API(help = "Hidden layer sizes", filter = Default.class)
  public int[] hidden = new int[] { 500 };

  @API(help = "Learning rate", filter = Default.class)
  public double rate = .01;

  @API(help = "L2 regularization", filter = Default.class)
  public double l2 = .0001;

  @API(help = "How many times the dataset should be iterated", filter = Default.class)
  public int epochs = 100;

  public NeuralNet() {
    description = DOC_GET;
  }

  @Override public Job fork() {
    init();
    H2OCountedCompleter start = new H2OCountedCompleter() {
      @Override public void compute2() {
        startTrain();
        tryComplete();
      }

      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
        Job.cancel(job_key, Utils.getStackAsString(ex));
        return super.onExceptionalCompletion(ex, caller);
      }
    };
    start(new H2OEmptyCompleter());
    H2O.submitTask(start);
    return this;
  }

  public void startTrain() {
    Vec[] vecs = Utils.add(_train, response);
    reChunk(vecs);
    System.arraycopy(vecs, 0, _train, 0, _train.length);
    response = vecs[vecs.length - 1];

    final Layer[] ls = new Layer[hidden.length + 2];
    ls[0] = new VecsInput(vecs, null);
    for( int i = 0; i < hidden.length; i++ ) {
      if( activation == Activation.Rectifier )
        ls[i + 1] = new Layer.Rectifier(hidden[i]);
      else
        ls[i + 1] = new Layer.Tanh(hidden[i]);
      ls[i + 1]._rate = (float) rate;
      ls[i + 1]._l2 = (float) l2;
    }
    if( classification )
      ls[ls.length - 1] = new VecSoftmax(response, null);
    else {
      // TODO Gaussian?
    }
    ls[ls.length - 1]._rate = (float) rate;
    ls[ls.length - 1]._l2 = (float) l2;
    for( int i = 0; i < ls.length; i++ )
      ls[i].init(ls, i);

    final Key sourceKey = Key.make(input("source"));
    final Frame frame = new Frame(_names, _train);
    frame.add(_responseName, response);
    final NeuralNetModel model = new NeuralNetModel(destination_key, sourceKey, frame, ls);
    UKV.put(destination_key, model);

    final Trainer trainer = new Trainer.MapReduce(ls, epochs, self());

    // Use a separate thread for monitoring (blocked most of the time)
    Thread thread = new Thread() {
      @Override public void run() {
        long start = System.nanoTime();
        Frame[] adapted = null;
        Vec[] valid = null;
        Vec validResp = null;
        if( validation != null ) {
          adapted = model.adapt(validation, false, true);
          valid = new Vec[adapted[0].vecs().length - 1];
          System.arraycopy(adapted[0].vecs(), 0, valid, 0, valid.length);
          validResp = adapted[0].vecs()[adapted[0].vecs().length - 1];
        }
        while( running() ) {
          long[][] cm = null;
          if( classification ) {
            int classes = ls[ls.length - 1]._units;
            cm = new long[classes][classes];
          }
          Error trainE = eval(_train, response, EVAL_ROW_COUNT, valid == null ? cm : null);
          Error validE = null;
          if( valid != null )
            validE = eval(valid, validResp, EVAL_ROW_COUNT, cm);

          double time = (System.nanoTime() - start) / 1e9;
          model.items = trainer.items();
          model.items_per_second = (int) (model.items / time);
          model.train_classification_error = trainE.Value;
          model.train_sqr_error = trainE.SqrDist;
          model.validation_classification_error = validE != null ? validE.Value : Double.NaN;
          model.validation_sqr_error = validE != null ? validE.SqrDist : Double.NaN;
          model.confusion_matrix = cm;
          UKV.put(model._selfKey, model);
          try {
            Thread.sleep(2000);
          } catch( InterruptedException e ) {
            throw new RuntimeException(e);
          }
        }
        if( adapted != null && adapted[1] != null )
          adapted[1].remove();
      }
    };
    trainer.start();
    thread.start();
  }

  public static Error eval(Layer[] ls, Frame frame, long n, long[][] cm) {
    Vec[] vecs = frame.vecs();
    vecs = Utils.remove(vecs, vecs.length - 1);
    Vec resp = frame.vecs()[frame.vecs().length - 1];
    return eval(ls, vecs, resp, n, cm);
  }

  public static Error eval(Layer[] ls, Vec[] vecs, Vec resp, long n, long[][] cm) {
    return eval(ls, //
        new VecsInput(vecs, (VecsInput) ls[0]), //
        new VecSoftmax(resp, (VecSoftmax) ls[ls.length - 1]), //
        n, cm);
  }

  public static Error eval(Layer[] ls, Input input, Output output, long n, long[][] cm) {
    Layer[] clones = new Layer[ls.length];
    clones[0] = input;
    for( int y = 1; y < clones.length - 1; y++ )
      clones[y] = ls[y].clone();
    clones[clones.length - 1] = output;
    for( int y = 0; y < clones.length; y++ )
      clones[y].init(clones, y, false, 0);
    Layer.copyWeights(ls, clones);
    return eval(clones, n, cm);
  }

  public static Error eval(Layer[] ls, long n, long[][] cm) {
    Error error = new Error();
    Input input = (Input) ls[0];
    long len = input._len;
    if( n != 0 )
      len = Math.min(len, n);
    int correct = 0;
    for( input._pos = 0; input._pos < len; input._pos++ )
      if( correct(ls, error, cm) )
        correct++;
    error.Value = (len - (double) correct) / len;
    return error;
  }

  private static boolean correct(Layer[] ls, Error error, long[][] confusion) {
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

  @Override protected Response redirect() {
    String n = NeuralNetProgress.class.getSimpleName();
    return new Response(Response.Status.redirect, this, -1, -1, n, "job", job_key, "dst_key", destination_key);
  }

  public Response redirect(Request req, Key key) {
    String n = new NeuralNetProgress().href();
    return new Response(Response.Status.redirect, req, -1, -1, n, "job", Key.make(), "dst_key", key);
  }

  public static String link(Key k, String content) {
    NeuralNet req = new NeuralNet();
    RString rs = new RString("<a href='" + req.href() + ".query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", "source");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override public String speedDescription() {
    return "time/epoch";
  }

  @Override public long speedValue() {
    Value value = DKV.get(dest());
    NeuralNetModel m = value != null ? (NeuralNetModel) value.get() : null;
    double epochsSoFar = m == null ? 0 : m.items / (double) source.numRows();
    long sv = (epochsSoFar <= 0) ? 0 : (long) (runTimeMs() / epochsSoFar);
    return sv;
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

    @API(help = "Classification error on the training set (Estimation)")
    public double train_classification_error = 1;

    @API(help = "Square distance error on the training set (Estimation)")
    public double train_sqr_error;

    @API(help = "Classification error on the validation set (Estimation)")
    public double validation_classification_error = 1;

    @API(help = "Square distance error on the validation set (Estimation)")
    public double validation_sqr_error;

    @API(help = "Confusion matrix")
    public long[][] confusion_matrix;

    NeuralNetModel(Key selfKey, Key dataKey, Frame fr, Layer[] ls) {
      super(selfKey, dataKey, fr);

      layers = new Layer[ls.length];
      for( int y = 0; y < ls.length; y++ )
        layers[y] = ls[y].clone();

      ws = new float[ls.length][];
      bs = new float[ls.length][];
      for( int y = 1; y < layers.length; y++ ) {
        ws[y] = layers[y]._w;
        bs[y] = layers[y]._b;
      }
    }

    @Override protected float[] score0(Chunk[] chunks, int rowInChunk, double[] tmp, float[] preds) {
      layers[0] = new ChunksInput(Utils.remove(chunks, chunks.length - 1), (VecsInput) layers[0]);
      layers[layers.length - 1] = new ChunkSoftmax(chunks[chunks.length - 1], (VecSoftmax) layers[layers.length - 1]);
      for( int y = 0; y < layers.length; y++ ) {
        layers[y]._w = ws[y];
        layers[y]._b = bs[y];
        layers[y].init(layers, y, false, 0);
      }
      ((Input) layers[0])._pos = rowInChunk;
      for( int i = 0; i < layers.length; i++ )
        layers[i].fprop();
      float[] out = layers[layers.length - 1]._a;
      assert out.length == preds.length;
      return out;
    }

    @Override protected float[] score0(double[] data, float[] preds) {
      throw new UnsupportedOperationException();
    }

    @Override public ConfusionMatrix cm() {
      long[][] cm = confusion_matrix;
      if( cm != null )
        return new ConfusionMatrix(cm);
      return null;
    }
  }

  public static class NeuralNetProgress extends Progress2 {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @Override protected String name() {
      return DOC_GET;
    }

    @Override public boolean toHTML(StringBuilder sb) {
      Job nn = job != null ? (Job) UKV.get(job) : null;
      NeuralNetModel model = UKV.get(dst_key);
      if( model != null ) {
        String cmTitle = "Confusion Matrix";
        String trainC = String.format("%5.2f %%", 100 * model.train_classification_error);
        String trainS = String.format("%.3f", model.train_sqr_error);
        String validC = String.format("%5.2f %%", 100 * model.validation_classification_error);
        String validS = String.format("%.3f", model.validation_sqr_error);
        if( Double.isNaN(model.validation_classification_error) ) {
          validC = validS = "N/A";
          cmTitle += " (Training Data)";
        }
        DocGen.HTML.section(sb, "Training classification error: " + trainC);
        DocGen.HTML.section(sb, "Training square error: " + trainS);
        DocGen.HTML.section(sb, "Validation classification error: " + validC);
        DocGen.HTML.section(sb, "Validation square error: " + validS);
        DocGen.HTML.section(sb, "Items: " + model.items);
        if( nn != null )
          DocGen.HTML.section(sb, "Items per second: " + (model.items * 1000 / nn.runTimeMs()));

        if( model.confusion_matrix != null ) {
          String[] classes = model.classNames();
          if( classes == null ) {
            classes = new String[model.confusion_matrix.length];
            for( int i = 0; i < model.confusion_matrix.length; i++ )
              classes[i] = "" + i;
          }
          NeuralNetScore.confusion(sb, cmTitle, classes, model.confusion_matrix);
        }
      }
      return true;
    }

    @Override protected Response jobDone(Job job, Key dst) {
      return new Response(Response.Status.done, this, 0, 0, null);
    }

    public static String link(Key job, Key model, String content) {
      NeuralNetProgress req = new NeuralNetProgress();
      return "<a href='" + req.href() + ".html?job=" + job + "&dst_key=" + model + "'>" + content + "</a>";
    }
  }

  public static class NeuralNetScore extends ModelJob {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    static final String DOC_GET = "Neural network scoring";

    @API(help = "Model", required = true, filter = Default.class)
    public NeuralNetModel model;

    @API(help = "Rows to consider for scoring, 0 (default) means the whole frame", filter = Default.class)
    public long max_rows;

    @API(help = "Classification error")
    public double classification_error;

    @API(help = "Square distance error")
    public double sqr_error;

    @API(help = "Confusion matrix")
    public long[][] confusion_matrix;

    public NeuralNetScore() {
      description = DOC_GET;
    }

    @Override protected void exec() {
      Frame[] frs = model.adapt(source, false, true);
      int classes = model.layers[model.layers.length - 1]._units;
      confusion_matrix = new long[classes][classes];
      Error error = model.eval(frs[0], max_rows, confusion_matrix);
      classification_error = error.Value;
      sqr_error = error.SqrDist;
      if( frs[1] != null )
        frs[1].remove();
    }

    @Override public boolean toHTML(StringBuilder sb) {
      DocGen.HTML.section(sb, "Classification error: " + String.format("%5.2f %%", 100 * classification_error));
      DocGen.HTML.section(sb, "Square error: " + sqr_error);
      confusion(sb, "Confusion Matrix", response.domain(), confusion_matrix);
      return true;
    }

    static void confusion(StringBuilder sb, String title, String[] classes, long[][] confusionMatrix) {
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
          long num = confusionMatrix[crow][ccol];
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
   * Makes sure small datasets are spread over enough chunks to parallelize training.
   */
  public static void reChunk(Vec[] vecs) {
    final int splits = cores() * 2; // More in case of unbalance
    if( vecs[0].nChunks() < splits ) {
      // A new random VectorGroup
      Key keys[] = new Vec.VectorGroup().addVecs(vecs.length);
      for( int v = 0; v < vecs.length; v++ ) {
        AppendableVec vec = new AppendableVec(keys[v]);
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
    }
  }
}
