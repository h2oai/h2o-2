package hex;

import hex.Layer.Input;
import water.Sandbox;

public class IrisSearch extends NeuralNetIrisTest {
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserMain.class, args);
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      Sandbox.localCloud(1, true, args);

      IrisSearch test = new IrisSearch();
      test.run();
    }
  }

  @Override public void run() throws Exception {
    load();

    ParamsSearch search = new ParamsSearch();
    double best = Double.MAX_VALUE;
    for( ;; ) {
      float rate = 0.01f;
      float momentum = .1f;
      int epochs = 4;
      create(rate, momentum);

      Trainer trainer = new Trainer.Direct(_ls);
      trainer._batches = epochs * (int) _train._frame.numRows();
      trainer._batch = 1;
      search.run(_ls[1], _ls[2]);
      trainer.run();
      double error = eval(_ls, _train, trainer);
      String m = "Error: " + error * 100 + " (Best: " + best + ")";
      if( error < best ) {
        best = error;
        search.save();
        System.out.println(m + ", Saved");
      } else
        System.out.println(m + ", Discarded");
    }
  }

  static double eval(Layer[] ls, Input input, Trainer trainer) {
    int correct = 0;
    for( int n = 0; n < input._count; n++ )
      if( test(ls, n) )
        correct++;
    return (input._count - (double) correct) / input._count;
  }
}