package hex;

import hex.Layer.FrameInput;
import hex.Layer.Input;
import hex.Trainer.Threaded;
import water.*;
import water.api.DocGen;
import water.api.Progress2;
import water.fvec.Frame;

public class NeuralNet extends Job {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "Neural network";
  public static final int EVAL_ROW_COUNT = 1000;

  @API(help = "Data Frame", required = true, filter = FrameKey.class)
  public Frame source;

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

// TODO
//  @API(help = "How much of the training set is used for validation", filter = Default.class)
//  public double validation_ratio = .2;

  //

  transient Layer[] _ls;

  //

  @API(help = "Classification error on the training set (Estimation)")
  public double train_classification_error;

  @API(help = "Square distance error on the training set (Estimation)")
  public double train_sqr_error;

  @API(help = "Classification error on the validation set (Estimation)")
  public double validation_classification_error;

  @API(help = "Square distance error on the validation set (Estimation)")
  public double validation_sqr_error;

  @API(help = "Training speed")
  public int rows_per_second;

  /**
   * Stores weights separately to avoid sending megabytes of JSON.
   */
  public static class Weights extends Iced {
    float[][] _ws, _bs;
    long _steps;

    public static Weights get(Layer[] ls, boolean clone) {
      Weights weights = new Weights();
      weights._ws = new float[ls.length][];
      weights._bs = new float[ls.length][];
      for( int y = 1; y < ls.length; y++ ) {
        weights._ws[y] = clone ? ls[y]._w.clone() : ls[y]._w;
        weights._bs[y] = clone ? ls[y]._b.clone() : ls[y]._b;
      }
      return weights;
    }

    public void set(Layer[] ls) {
      for( int y = 1; y < ls.length; y++ ) {
        ls[y]._w = _ws[y];
        ls[y]._b = _bs[y];
      }
    }
  }

  @API(help = "Layers weights")
  public Key weights;

  @Override protected void run() {
    _ls = new Layer[3];

    _ls[0] = new FrameInput(source);
    _ls[0].init(null, source.numCols() - 1);
    for( int i = 0; i < hidden.length; i++ ) {
      if( activation == Activation.Rectifier )
        _ls[i + 1] = new Layer.Rectifier();
      else
        _ls[i + 1] = new Layer.Tanh();
      _ls[i + 1]._rate = (float) rate;
      _ls[i + 1]._l2 = (float) l2;
      _ls[i + 1].init(_ls[i], hidden[i]);
    }
    _ls[_ls.length - 1] = new Layer.Softmax();
    _ls[_ls.length - 1]._rate = (float) rate;
    _ls[_ls.length - 1]._l2 = (float) l2;
    int classes = (int) (source._vecs[source._vecs.length - 1].max() + 1);
    _ls[_ls.length - 1].init(_ls[_ls.length - 2], classes);

    for( int i = 1; i < _ls.length; i++ )
      _ls[i].randomize();

    final Threaded trainer = new Threaded(_ls);
    weights = Key.make(destination_key.toString() + "_weights");
    UKV.put(destination_key, this);
    trainer.start();

    // Use a separate thread for monitoring (blocked most of the time)
    Thread thread = new Thread() {
      @Override public void run() {
        long lastTime = System.nanoTime();
        long lastItems = 0;
        while( !cancelled() ) {
          Error train = NeuralNetScore.eval(_ls, EVAL_ROW_COUNT);
          train_classification_error = train.Value;
          train_sqr_error = train.SqrDist;

          long time = System.nanoTime();
          double delta = (time - lastTime) / 1e9;
          lastTime = time;
          long items = trainer.steps();
          rows_per_second = (int) ((items - lastItems) / delta);

          UKV.put(destination_key, NeuralNet.this);
          UKV.put(weights, Weights.get(_ls, false));

          try {
            Thread.sleep(2000);
          } catch( InterruptedException e ) {
            throw new RuntimeException(e);
          }
        }
        trainer.close();;
      }
    };
    thread.start();
  }

  @Override protected Response redirect() {
    String n = NeuralNetProgress.class.getSimpleName();
    return new Response(Response.Status.redirect, this, -1, -1, n, "job", job_key, "dst_key", destination_key);
  }

  public static class Error {
    double Value;
    double SqrDist;

    @Override public String toString() {
      return String.format("%.3f", (100 * Value)) + "% (d²:" + String.format("%.2e", SqrDist) + ")";
    }
  }

  public static class NeuralNetProgress extends Progress2 {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    static final String DOC_GET = "Neural Network progress";

    transient NeuralNet _net;

    @Override protected Response serve() {
      _net = UKV.get(Key.make(dst_key.value()));
      return new Response(Response.Status.done, this, -1, -1, null);
    }

    @Override public boolean toHTML(StringBuilder sb) {
      if( _net != null ) {
//        sb.append("<h3>Confusion Matrix</h3>");
        sb.append("<dl class='dl-horizontal'>");
        sb.append("<dt>Train error</dt>");
        sb.append("<dd>").append(String.format("%5.3f %%", 100 * _net.train_classification_error)).append("</dd>");
        sb.append("<dt>Validation error</dt>");
        sb.append("<dd>").append(String.format("%5.3f %%", 100 * _net.validation_classification_error)).append("</dd>");;
        sb.append("</dl>");
//        sb.append("<table class='table table-striped table-bordered table-condensed'>");
//        sb.append("<tr><th>Actual \\ Predicted</th>");
//        Frame frame = trainer.layers()
//        String[] classes = _net.source._vecs[_net.source._vecs.length - 1].domain();
//        for( String c : classes )
//          sb.append("<th>" + c + "</th>");
//        sb.append("<th>Error</th></tr>");
//        long[] totals = new long[classes.length];
//        long sumTotal = 0;
//        long sumError = 0;
//      for( int crow = 0; crow < classes; ++crow ) {
//        JsonArray row = (JsonArray) matrix.get(crow);
//        long total = 0;
//        long error = 0;
//        sb.append("<tr><th>" + header.get(crow).getAsString() + "</th>");
//        for( int ccol = 0; ccol < classes; ++ccol ) {
//          long num = row.get(ccol).getAsLong();
//          total += num;
//          totals[ccol] += num;
//          if( ccol == crow ) {
//            sb.append("<td style='background-color:LightGreen'>");
//          } else {
//            sb.append("<td>");
//            error += num;
//          }
//          sb.append(num);
//          sb.append("</td>");
//        }
//        sb.append("<td>");
//        sb.append(String.format("%5.3f = %d / %d", (double) error / total, error, total));
//        sb.append("</td></tr>");
//        sumTotal += total;
//        sumError += error;
//      }
//        sb.append("<tr><th>Totals</th>");
//        for( int i = 0; i < totals.length; ++i )
//          sb.append("<td>" + totals[i] + "</td>");
//        sb.append("<td><b>");
//        sb.append(String.format("%5.3f = %d / %d", (double) sumError / sumTotal, sumError, sumTotal));
//        sb.append("</b></td></tr>");
//        sb.append("</table>");
      }
      return true;
    }
  }

  public static class NeuralNetScore extends FrameJob {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    static final String DOC_GET = "Neural network scoring";

    public NeuralNetScore() {
      super("Neural Net", null);
    }

    @API(help = "Model", required = true, filter = Default.class)
    public NeuralNet model;

    @API(help = "Rows to consider for scoring, 0 (default) means the whole frame", filter = Default.class)
    public long max_rows = EVAL_ROW_COUNT;

    @API(help = "Classification error")
    public double classification_error;

    @API(help = "Square distance error")
    public double sqr_error;

    @Override protected Response serve() {
      Layer[] clones = Layer.clone(model._ls, model.source);
      Weights weights = UKV.get(model.weights);
      weights.set(clones);
      Error error = eval(clones, max_rows);
      classification_error = error.Value;
      sqr_error = error.SqrDist;
      return new Response(Response.Status.done, this, -1, -1, null);
    }

    public static Error eval(Layer[] ls, long max_rows) {
      FrameInput input = (FrameInput) ls[0];
      Error error = new Error();
      long len = input._frame.numRows();
      if( max_rows != 0 )
        len = Math.min(len, max_rows);
      int correct = 0;
      for( input._row = 0; input._row < len; input._row++ )
        if( correct(ls, error) )
          correct++;
      error.Value = (len - (double) correct) / len;
      return error;
    }

    private static boolean correct(Layer[] ls, Error error) {
      Input input = (Input) ls[0];
      for( int i = 0; i < ls.length; i++ )
        ls[i].fprop();
      float[] out = ls[ls.length - 1]._a;
      error.SqrDist = 0;
      for( int i = 0; i < out.length; i++ ) {
        float t = i == input.label() ? 1 : 0;
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
      return idx == input.label();
    }
  }
}
