package hex;

import hex.Trainer.Threaded;
import water.Sample07_NeuralNetLowLevel;
import water.util.Utils;

public class MnistSearch extends Sample07_NeuralNetLowLevel {
  public static void main(String[] args) throws Exception {
    MnistSearch test = new MnistSearch();
    test.run();
  }

  @Override public void run() throws Exception {
    load();

    ParamsSearch search = new ParamsSearch();
    Error error = new Error();
    Error best = null;
    for( ;; ) {
      _ls = new Layer[3];
      _ls[0] = _train;
      _ls[1] = new Layer.Tanh(_ls[0], 1000);
      _ls[1]._rate = 0.001f;
      _ls[2] = new Layer.Softmax(_ls[1], 10);
      _ls[2]._rate = 0.001f;
      for( int i = 0; i < _ls.length; i++ )
        _ls[i].init(false);

      search.run(_ls[1], _ls[2]);

      for( int i = 0; i < 10; i++ ) {
        Trainer trainer = new Threaded(_ls);
        trainer._batch = 10;
        //trainer._batches = (int) _train._count / trainer._batch;
        trainer._batches = 100;
        long start = System.nanoTime();
        trainer.run();
        long ended = System.nanoTime();
        eval(_ls, error);
        int ps = (int) ((trainer._batch * trainer._batches) * (long) 1e9 / (ended - start));
        System.out.println(ps + "/s, error: " + error + ", best: " + best);
      }

      if( best == null || error.Value < best.Value ) {
        best = Utils.clone(error);
        search.save();
        System.out.println("Saved");
      }
    }
  }
}