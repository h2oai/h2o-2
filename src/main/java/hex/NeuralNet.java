package hex;

import hex.KMeansGrid.kFilter;
import hex.Layer.FrameInput;
import hex.Layer.Tanh;
import water.Job;
import water.api.DocGen;
import water.api.Progress2;
import water.fvec.Frame;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

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
  @API(help = "Activation function", filter = kFilter.class)
  public Activation activation;
  class activationFilter extends EnumArgument<Activation> { public activationFilter() { super(Activation.Tanh); } }

  @API(help = "Hidden layer sizes", filter = kFilter.class)
  int[] hidden;
  class hiddenFilter extends RSeq { public hiddenFilter() { super("1000", false); } }
//@formatter:on

  @API(help = "Learning rate", filter = Default.class)
  public float rate = new Tanh()._rate;

  @API(help = "L2 regularization", filter = Default.class)
  public float l2 = new Tanh()._l2;

  transient Layer[] _ls;

  @Override protected void run() {
    _ls = new Layer[3];
    _ls[0] = new FrameInput(source, true);
    for( int i = 0; i < hidden.length; i++ ) {
      if( activation == Activation.Rectifier )
        _ls[i + 1] = new Layer.Rectifier();
      else
        _ls[i + 1] = new Layer.Tanh();
      _ls[i + 1].init(_ls[i], hidden[i]);
      _ls[i + 1]._rate = rate;
      _ls[i + 1]._l2 = l2;
    }
    _ls[2] = new Layer.Softmax();
    _ls[1]._rate = rate;
    _ls[2]._l2 = l2;

    for( int i = 1; i < _ls.length; i++ )
      _ls[i].randomize();

    final Trainer trainer = new Trainer.Distributed(_ls);
    trainer.run();
  }

  public class NNView extends Progress2 {
    @API(help = "Neural Network", filter = H2OKey.class)
    NeuralNet _net;

    @API(help = "Classification error on the test set (Estimation)")
    float test_error;

    @API(help = "Classification error on the validation set (Estimation)")
    float validation_error;

    @Override public boolean toHTML(StringBuilder sb) {
      sb.append("<h3>Confusion Matrix</h3>");
      sb.append("<dl class='dl-horizontal'>");
      sb.append("<dt>Test error</dt>");
      sb.append("<dd>").append(String.format("%5.3f %%", 100 * test_error)).append("</dd>");
      sb.append("<dt>Validation error</dt>");
      sb.append("<dd>").append(String.format("%5.3f %%", 100 * validation_error)).append("</dd>");;
      sb.append("</dl>");
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      sb.append("<tr><th>Actual \\ Predicted</th>");
      JsonArray header = (JsonArray) cm.get(JSON_CM_HEADER);
      for( JsonElement e : header )
        sb.append("<th>" + e.getAsString() + "</th>");
      sb.append("<th>Error</th></tr>");
      int classes = header.size();
      long[] totals = new long[classes];
      JsonArray matrix = (JsonArray) cm.get(JSON_CM_MATRIX);
      long sumTotal = 0;
      long sumError = 0;
      for( int crow = 0; crow < classes; ++crow ) {
        JsonArray row = (JsonArray) matrix.get(crow);
        long total = 0;
        long error = 0;
        sb.append("<tr><th>" + header.get(crow).getAsString() + "</th>");
        for( int ccol = 0; ccol < classes; ++ccol ) {
          long num = row.get(ccol).getAsLong();
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
      return sb.toString();
    }
  }
}
