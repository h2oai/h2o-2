package hex;

import hex.Layer.ChunkLinear;
import hex.Layer.ChunkSoftmax;
import hex.Layer.ChunksInput;
import hex.Layer.Input;
import hex.Layer.Linear;
import hex.Layer.Output;
import hex.Layer.Softmax;
import hex.Layer.VecLinear;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;

import java.util.Arrays;
import java.util.Random;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
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

  @API(help = "Hidden layer sizes, e.g. 1000, 1000. Grid search: (100, 100), (200, 200)", filter = Default.class)
  public int[] hidden = new int[] { 500 };

  @API(help = "Learning rate", filter = Default.class)
  public double rate = .005;

  @API(help = "Learning rate annealing: rate / (1 + rate_annealing * samples)", filter = Default.class)
  public double rate_annealing = 1 / 1e6;

  @API(help = "L2 regularization", filter = Default.class)
  public double l2 = .001;

  @API(help = "How many times the dataset should be iterated", filter = Default.class)
  public int epochs = 100;

  @API(help = "Seed for the random number generator", filter = Default.class)
  public long seed = new Random().nextLong();

  public NeuralNet() {
    description = DOC_GET;
  }

  @Override public Job fork() {
    init();
    H2OCountedCompleter start = new H2OCountedCompleter() {
      @Override public void compute2() {
        startTrain();
      }

      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
        Job job = Job.findJob(job_key);
        if( job != null )
          job.cancel(Utils.getStackAsString(ex));
        return super.onExceptionalCompletion(ex, caller);
      }
    };
    start(start);
    H2O.submitTask(start);
    return this;
  }

  public void startTrain() {
    Vec[] vecs = Utils.append(_train, response);
    reChunk(vecs);
    final Vec[] train = new Vec[vecs.length - 1];
    System.arraycopy(vecs, 0, train, 0, train.length);
    final Vec trainResp = classification ? vecs[vecs.length - 1].toEnum() : vecs[vecs.length - 1];

    final Layer[] ls = new Layer[hidden.length + 2];
    ls[0] = new VecsInput(train, null);
    for( int i = 0; i < hidden.length; i++ ) {
      if( activation == Activation.Rectifier )
        ls[i + 1] = new Layer.Rectifier(hidden[i]);
      else
        ls[i + 1] = new Layer.Tanh(hidden[i]);
      ls[i + 1].rate = (float) rate;
      ls[i + 1].rate_annealing = (float) rate_annealing;
      ls[i + 1].l2 = (float) l2;
    }
    if( classification )
      ls[ls.length - 1] = new VecSoftmax(trainResp, null);
    else
      ls[ls.length - 1] = new VecLinear(trainResp, null);
    ls[ls.length - 1].rate = (float) rate;
    ls[ls.length - 1].rate_annealing = (float) rate_annealing;
    ls[ls.length - 1].l2 = (float) l2;
    for( int i = 0; i < ls.length; i++ )
      ls[i].init(ls, i);

    final Key sourceKey = Key.make(input("source"));
    final Frame frame = new Frame(_names, train);
    frame.add(_responseName, trainResp);
    final Error[] trainErrors0 = new Error[] { new Error() };
    final Error[] validErrors0 = validation == null ? null : new Error[] { new Error() };

    NeuralNetModel model = new NeuralNetModel(destination_key, sourceKey, frame, ls);
    model.training_errors = trainErrors0;
    model.validation_errors = validErrors0;
    UKV.put(destination_key, model);

    final Frame[] adapted = validation == null ? null : model.adapt(validation, false,false);
    final Trainer trainer = new Trainer.MapReduce(ls, epochs, self());

    // Use a separate thread for monitoring (blocked most of the time)
    Thread thread = new Thread() {
      Error[] trainErrors = trainErrors0, validErrors = validErrors0;

      @Override public void run() {
        try {
          Vec[] valid = null;
          Vec validResp = null;
          if( validation != null ) {
            final Vec [] vs = adapted[0].vecs();
            valid = Arrays.copyOf(vs,vs.length-1);
            System.arraycopy(adapted[0].vecs(), 0, valid, 0, valid.length);
            validResp = vs[vs.length-1];
          }
          while( !cancelled() ) {
            eval(valid, validResp);
            try {
              Thread.sleep(2000);
            } catch( InterruptedException e ) {
              throw new RuntimeException(e);
            }
          }
          eval(valid, validResp);
          if( adapted != null && adapted[1] != null )
            adapted[1].remove();
        } catch( Exception ex ) {
          cancel(ex);
        }
      }

      private void eval(Vec[] valid, Vec validResp) {
        long[][] cm = null;
        if( classification ) {
          int classes = ls[ls.length - 1].units;
          cm = new long[classes][classes];
        }
        Error e = eval(train, trainResp, valid == null ? cm : null);
        trainErrors = Utils.append(trainErrors, e);
        if( valid != null ) {
          e = eval(valid, validResp, cm);
          validErrors = Utils.append(validErrors, e);
        }
        NeuralNetModel model = new NeuralNetModel(destination_key, sourceKey, frame, ls);
        model.training_errors = trainErrors;
        model.validation_errors = validErrors;
        model.confusion_matrix = cm;
        UKV.put(model._selfKey, model);
      }

      private Error eval(Vec[] vecs, Vec resp, long[][] cm) {
        Error e = NeuralNet.eval(ls, vecs, resp, EVAL_ROW_COUNT, cm);
        e.training_samples = trainer.samples();
        e.evaluation_samples = EVAL_ROW_COUNT;
        e.training_time_ms = runTimeMs();
        return e;
      }
    };
    trainer.start();
    thread.start();
  }

  @Override public float progress() {
    NeuralNetModel model = UKV.get(destination_key);
    if( model != null && source != null && epochs > 0 ) {
      Error e = model.training_errors[model.training_errors.length - 1];
      return 0.1f + Math.min(1, e.evaluation_samples / (float) (epochs * source.numRows()));
    }
    return 0;
  }

  public static Error eval(Layer[] ls, Vec[] vecs, Vec resp, long n, long[][] cm) {
    Output output = (Output) ls[ls.length - 1];
    if( output instanceof VecSoftmax )
      output = new VecSoftmax(resp, (VecSoftmax) output);
    else
      output = new VecLinear(resp, (VecLinear) output);
    return eval(ls, new VecsInput(vecs, (VecsInput) ls[0]), output, n, cm);
  }

  public static Error eval(Layer[] ls, Input input, Output output, long n, long[][] cm) {
    Layer[] clones = new Layer[ls.length];
    clones[0] = input;
    for( int y = 1; y < clones.length - 1; y++ )
      clones[y] = ls[y].clone();
    clones[clones.length - 1] = output;
    for( int y = 0; y < clones.length; y++ )
      clones[y].init(clones, y, false, 0);
    Layer.shareWeights(ls, clones);
    return eval(clones, n, cm);
  }

  public static Error eval(Layer[] ls, long n, long[][] cm) {
    Error error = new Error();
    Input input = (Input) ls[0];
    long len = input._len;
    if( n != 0 )
      len = Math.min(len, n);
    if( ls[ls.length - 1] instanceof Softmax ) {
      int correct = 0;
      for( input._pos = 0; input._pos < len; input._pos++ )
        if( correct(ls, error, cm) )
          correct++;
      error.classification = (len - (double) correct) / len;
      error.mean_square /= len;
    } else {
      for( input._pos = 0; input._pos < len; input._pos++ )
        error(ls, error);
      error.classification = Double.NaN;
      error.mean_square /= len;
    }
    input._pos = 0;
    return error;
  }

  private static boolean correct(Layer[] ls, Error error, long[][] confusion) {
    Softmax output = (Softmax) ls[ls.length - 1];
    for( int i = 0; i < ls.length; i++ )
      ls[i].fprop(false);
    float[] out = ls[ls.length - 1]._a;
    for( int o = 0; o < out.length; o++ ) {
      float t = o == output.target() ? 1 : 0;
      float d = t - out[o];
      error.mean_square += d * d;
    }
    float max = out[0];
    int idx = 0;
    for( int o = 1; o < out.length; o++ ) {
      assert !Double.isNaN(out[o]);
      if( out[o] > max ) {
        max = out[o];
        idx = o;
      }
    }
    if( confusion != null )
      confusion[output.target()][idx]++;
    return idx == output.target();
  }

  // TODO extract to layer
  private static void error(Layer[] ls, Error error) {
    Linear linear = (Linear) ls[ls.length - 1];
    for( int i = 0; i < ls.length; i++ )
      ls[i].fprop(false);
    float[] output = ls[ls.length - 1]._a;
    float[] target = linear.target();
    for( int o = 0; o < output.length; o++ ) {
      float d = target[o] - output[o];
      error.mean_square += d * d;
    }
  }

  @Override protected Response redirect() {
    String redirectName = NeuralNetProgress.class.getSimpleName();
    return Response.redirect(this, redirectName, //
        "job_key", job_key, //
        "destination_key", destination_key);
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
    long sv = 0;
    if( m != null ) {
      Error[] e = m.training_errors;
      double epochsSoFar = e[e.length - 1].training_samples / (double) source.numRows();
      sv = (epochsSoFar <= 0) ? 0 : (long) (e[e.length - 1].training_time_ms / epochsSoFar);
    }
    return sv;
  }

  public static class Error extends Iced {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "How many rows the algorithm has processed")
    public long training_samples;

    @API(help = "How long the algorithm ran in ms")
    public long training_time_ms;

    @API(help = "How many rows were used to evaluate this error")
    public long evaluation_samples;

    @API(help = "Classification error")
    public double classification = 1;

    @API(help = "MSE")
    public double mean_square;

    // TODO
    //@API(help = "Cross entropy")
    //public double cross_entropy;

    @API(help = "Layer learning rates")
    public double[] rates;

    @Override public String toString() {
      return String.format("%.2f", (100 * classification)) + "% (MSE:" + String.format("%.2e", mean_square) + ")";
    }
  }

  public static class NeuralNetModel extends Model {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Layers")
    public Layer[] layers;

    @API(help = "Layer weights")
    public float[][] weights, biases;

    @API(help = "Errors on the training set")
    public Error[] training_errors;

    @API(help = "Errors on the validation set")
    public Error[] validation_errors;

    @API(help = "Confusion matrix")
    public long[][] confusion_matrix;

    NeuralNetModel(Key selfKey, Key dataKey, Frame fr, Layer[] ls) {
      super(selfKey, dataKey, fr);

      layers = ls;
      weights = new float[ls.length][];
      biases = new float[ls.length][];
      for( int y = 1; y < layers.length; y++ ) {
        weights[y] = layers[y]._w;
        biases[y] = layers[y]._b;
      }
    }

    @Override protected float[] score0(Chunk[] chunks, int rowInChunk, double[] tmp, float[] preds) {
      Layer[] clones = new Layer[layers.length];
      clones[0] = new ChunksInput(Utils.remove(chunks, chunks.length - 1), (VecsInput) layers[0]);
      for( int y = 1; y < layers.length - 1; y++ )
        clones[y] = layers[y].clone();
      Layer output = layers[layers.length - 1];
      if( output instanceof VecSoftmax )
        clones[clones.length - 1] = new ChunkSoftmax(chunks[chunks.length - 1], (VecSoftmax) output);
      else
        clones[clones.length - 1] = new ChunkLinear(chunks[chunks.length - 1], (VecLinear) output);
      for( int y = 0; y < clones.length; y++ ) {
        clones[y]._w = weights[y];
        clones[y]._b = biases[y];
        clones[y].init(clones, y, false, 0);
      }
      ((Input) clones[0])._pos = rowInChunk;
      for( int i = 0; i < clones.length; i++ )
        clones[i].fprop(false);
      float[] out = clones[clones.length - 1]._a;
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

    public Response redirect(Request req) {
      String redirectName = new NeuralNetProgress().href();
      return Response.redirect(req, redirectName, "destination_key", _selfKey);
    }
  }

  public static class NeuralNetProgress extends Progress2 {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Model")
    public NeuralNetModel model;

    @Override protected String name() {
      return DOC_GET;
    }

    @Override public boolean toHTML(StringBuilder sb) {
      Job nn = job_key == null ? null : Job.findJob(job_key);
      NeuralNetModel model = UKV.get(destination_key);
      if( model != null ) {
        String cmTitle = "Confusion Matrix", trainC, trainS, validC = "", validS = "";
        Error train = model.training_errors[model.training_errors.length - 1];
        trainC = format(train.classification);
        trainS = "" + train.mean_square;
        if( model.validation_errors != null ) {
          Error valid = model.validation_errors[model.validation_errors.length - 1];
          validC = format(valid.classification);
          validS = "" + valid.mean_square;
        } else
          cmTitle += " (Training Data)";
        DocGen.HTML.section(sb, "Training classification error: " + trainC);
        DocGen.HTML.section(sb, "Training mean square error: " + trainS);
        DocGen.HTML.section(sb, "Validation classification error: " + validC);
        DocGen.HTML.section(sb, "Validation mean square error: " + validS);
        if( nn != null ) {
          long ps = train.training_samples * 1000 / nn.runTimeMs();
          DocGen.HTML.section(sb, "Training speed: " + ps + " samples/s");
        }
        if( model.confusion_matrix != null && model.confusion_matrix.length < 100 ) {
          String[] classes = model.classNames();
          NeuralNetScore.confusion(sb, cmTitle, classes, model.confusion_matrix);
        }
        DocGen.HTML.section(sb, "Progress");
        sb.append("<table class='table table-striped table-bordered table-condensed'>");
        sb.append("<tr>");
        sb.append("<th>Training Time</th>");
        sb.append("<th>Training Samples</th>");
        sb.append("<th>Training MSE</th>");
        sb.append("<th>Training Classification</th>");
        sb.append("<th>Validation MSE</th>");
        sb.append("<th>Validation Classification</th>");
        sb.append("</tr>");
        Error[] trains = model.training_errors;
        for( int i = 0; i < trains.length; ++i ) {
          sb.append("<tr>");
          sb.append("<td>" + PrettyPrint.msecs(trains[i].training_time_ms, true) + "</td>");
          sb.append("<td>" + trains[i].training_samples + "</td>");
          sb.append("<td>" + trains[i].mean_square + "</td>");
          sb.append("<td>" + format(trains[i].classification) + "</td>");
          if( model.validation_errors != null ) {
            sb.append("<td>" + model.validation_errors[i].mean_square + "</td>");
            sb.append("<td>" + format(model.validation_errors[i].classification) + "</td>");
          } else
            sb.append("<td></td><td></td>");
          sb.append("</tr>");
        }
        sb.append("</table>");
      }
      return true;
    }

    private static String format(double classification) {
      String s = "N/A";
      if( !Double.isNaN(classification) )
        s = String.format("%5.2f %%", 100 * classification);
      return s;
    }

    @Override protected Response jobDone(Job job, Key dst) {
      return Response.done(this);
    }

    public static String link(Key job, Key model, String content) {
      NeuralNetProgress req = new NeuralNetProgress();
      return "<a href='" + req.href() + ".html?job_key=" + job + "&destination_key=" + model + "'>" + content + "</a>";
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

    @API(help = "Mean square error")
    public double mean_square_error;

    @API(help = "Confusion matrix")
    public long[][] confusion_matrix;

    public NeuralNetScore() {
      description = DOC_GET;
    }

    @Override protected Response serve() {
      init();
      Frame[] frs = model.adapt(source, false);
      int classes = model.layers[model.layers.length - 1].units;
      confusion_matrix = new long[classes][classes];
      Layer[] clones = new Layer[model.layers.length];
      for( int y = 0; y < model.layers.length; y++ ) {
        clones[y] = model.layers[y].clone();
        clones[y]._w = model.weights[y];
        clones[y]._b = model.biases[y];
      }
      Error error = eval(clones, frs[0].vecs(), response, max_rows, confusion_matrix);
      classification_error = error.classification;
      mean_square_error = error.mean_square;
      if( frs[1] != null )
        frs[1].remove();
      return Response.done(this);
    }

    @Override public boolean toHTML(StringBuilder sb) {
      DocGen.HTML.section(sb, "Classification error: " + String.format("%5.2f %%", 100 * classification_error));
      DocGen.HTML.section(sb, "Mean square error: " + mean_square_error);
      confusion(sb, "Confusion Matrix", response.domain(), confusion_matrix);
      return true;
    }

    static void confusion(StringBuilder sb, String title, String[] classes, long[][] confusionMatrix) {
      sb.append("<h3>" + title + "</h3>");
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      sb.append("<tr><th>Actual \\ Predicted</th>");
      if( classes == null ) {
        classes = new String[confusionMatrix.length];
        for( int i = 0; i < classes.length; i++ )
          classes[i] = "" + i;
      }
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
