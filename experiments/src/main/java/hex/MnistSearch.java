package hex;

public class MnistSearch extends NeuralNetMnistTest {
  public static void main(String[] args) throws Exception {
    MnistSearch test = new MnistSearch();
    test.run();
  }

  @Override public void run() throws Exception {
    load();

    ParamsSearch search = new ParamsSearch();
    double best = Double.MAX_VALUE;
    for( ;; ) {
      _ls = new Layer[3];
      _ls[0] = _train;
      _ls[1] = new Layer.Tanh(_ls[0], 1000);
      _ls[1]._rate = 0.001f;
      _ls[2] = new Layer.Softmax(_ls[1], 10);
      _ls[2]._rate = 0.001f;

      search.run(_ls[1], _ls[2]);
      double current = 0;

      for( int i = 0; i < 10; i++ ) {
        Trainer trainer = new Trainer.Direct(_ls);
        trainer._batch = 1;
        //trainer._batches = (int) _train._count / trainer._batch;
        trainer._batches = 100;
        trainer.run();
        Error error = new Error();
        eval(_ls, error);
        System.out.println("Error: " + error + ", best: " + best);
        current += error.Value;
      }

      if( current < best ) {
        best = current;
        search.save();
        System.out.println("Saved");
      }
    }
  }
}