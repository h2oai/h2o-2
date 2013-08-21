package hex;

import hex.Layer.FrameInput;
import hex.Layer.Input;
import hex.NeuralNetTest.Error;
import hex.Trainer.ParallelTrainers;

import java.util.concurrent.ConcurrentHashMap;

import water.Job;
import water.Key;
import water.api.DocGen;
import water.api.Progress2;
import water.fvec.Frame;

public class NeuralNet extends Job {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "Neural Network";

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
  class hiddenFilter extends RSeq { public hiddenFilter() { super("1000", false); } }
//@formatter:on

  @API(help = "Learning rate", filter = Default.class)
  public double rate = .01;

  @API(help = "L2 regularization", filter = Default.class)
  public double l2 = .0001;

  @API(help = "How much of the training set is used for validation", filter = Default.class)
  public double validation_ratio = .2;

  //

  transient Layer[] _ls;

  // TODO remove static
  static final ConcurrentHashMap<Key, Trainer> _trainers = new ConcurrentHashMap<Key, Trainer>();

  long validation() {
    return (long) (source.numRows() * validation_ratio);
  }

  @Override protected void run() {
    _ls = new Layer[3];

    long test = source.numRows() - validation();
    _ls[0] = new FrameInput(source, 0, test, true);
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
    int classes = source._vecs[source._vecs.length - 1].domain().length;
    _ls[_ls.length - 1].init(_ls[_ls.length - 2], classes);

    for( int i = 1; i < _ls.length; i++ )
      _ls[i].randomize();

    ParallelTrainers trainer = new ParallelTrainers(_ls);
    trainer.start();
    _trainers.put(destination_key, trainer);
  }

  @Override public void remove() {
    super.remove();
    _trainers.remove(destination_key);
  }

  @Override protected Response redirect() {
    String n = NeuralNetProgress.class.getSimpleName();
    return new Response(Response.Status.redirect, this, -1, -1, n, "job", job_key, "dst_key", destination_key);
  }

  public static class NeuralNetProgress extends Progress2 {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    static final String DOC_GET = "Neural Network progress";

    @API(help = "Classification error on the test set (Estimation)")
    public double test_classification_error;

    @API(help = "Square distance error on the test set (Estimation)")
    public double test_sqr_error;

    @API(help = "Classification error on the validation set (Estimation)")
    public double validation_classification_error;

    @API(help = "Square distance error on the validation set (Estimation)")
    public double validation_sqr_error;

    @Override protected Response serve() {
      Trainer trainer = _trainers.get(Key.make(dst_key.value()));
      if( trainer != null ) {
        Layer[] ls = trainer.layers();
        Input test = (Input) ls[0];
        long off = _net.validation();
        long len = _net.source.numRows() - off;
        Input validation = new FrameInput(_net.source, off, len, true);

        Error testErr = NeuralNetTest.eval(_net._ls, test);
        test_classification_error = testErr.Value;
        test_sqr_error = testErr.SqrDist;

        Error validationErr = NeuralNetTest.eval(_net._ls, validation);
        validation_classification_error = validationErr.Value;
        validation_sqr_error = validationErr.SqrDist;
      }
      // TODO
      return new Response(Response.Status.done, this, -1, -1, null);
    }

    @Override public boolean toHTML(StringBuilder sb) {
      sb.append("<h3>Confusion Matrix</h3>");
      sb.append("<dl class='dl-horizontal'>");
      sb.append("<dt>Test error</dt>");
      sb.append("<dd>").append(String.format("%5.3f %%", 100 * test_classification_error)).append("</dd>");
      sb.append("<dt>Validation error</dt>");
      sb.append("<dd>").append(String.format("%5.3f %%", 100 * validation_classification_error)).append("</dd>");;
      sb.append("</dl>");
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      sb.append("<tr><th>Actual \\ Predicted</th>");
      String[] classes = _net.source._vecs[_net.source._vecs.length - 1].domain();
      for( String c : classes )
        sb.append("<th>" + c + "</th>");
      sb.append("<th>Error</th></tr>");
      long[] totals = new long[classes.length];
      long sumTotal = 0;
      long sumError = 0;
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
      sb.append("<tr><th>Totals</th>");
      for( int i = 0; i < totals.length; ++i )
        sb.append("<td>" + totals[i] + "</td>");
      sb.append("<td><b>");
      sb.append(String.format("%5.3f = %d / %d", (double) sumError / sumTotal, sumError, sumTotal));
      sb.append("</b></td></tr>");
      sb.append("</table>");
      return true;
    }
  }
}
