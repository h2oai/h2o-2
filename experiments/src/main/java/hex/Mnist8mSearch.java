package hex;

import hex.Mnist8m.TestInput;
import hex.Mnist8m.Train8mInput;
import hex.Trainer.ParallelTrainers;
import water.deploy.VM;

public class Mnist8mSearch {
  public static void main(String[] args) throws Exception {
    VM.exitWithParent();

    Mnist8m.load();
    Mnist8m.loadTest();

    Mnist8mSearch mnist = new Mnist8mSearch();
    mnist.run();
  }

  public void run() throws Exception {
    ParamsSearch search = new ParamsSearch();
    double best = 4;
    for( ;; ) {
      Layer[] ls = new Layer[3];
      ls[0] = new Train8mInput();
      ls[1] = new Layer.Tanh(ls[0], 1000);
      ls[1]._rate = 0.001f;
      ls[2] = new Layer.Tanh(ls[1], 10);
      ls[2]._rate = 0.00005f;
      for( int i = 0; i < ls.length; i++ )
        ls[i].init();

      Trainer trainer = new ParallelTrainers(ls);
      trainer._batches = Mnist8m.COUNT / trainer._batch;
      search.run(ls[1], ls[2]);
      trainer.run();
      double error = eval(ls, trainer);
      if( error < best ) {
        best = error;
        search.save();
        System.out.println("Saved: " + search.toString());
      } else
        System.out.println("Discarded");
    }
  }

  double eval(Layer[] ls, Trainer trainer) {
    int count = 1000, correct = 0;
    for( int n = 0; n < count; n++ ) {
      if( Mnist8m.test(ls, n, null) ) {
        correct++;
      }
    }
    double error = (count - (double) correct) / count;
    String train = Mnist8m._format.format(error * 100f);

    ls[0] = new TestInput();
    ls[1]._in = ls[0];
    count = MnistNeuralNetTest._test._labels.length;
    correct = 0;
    for( int n = 0; n < count; n++ ) {
      if( Mnist8m.test(ls, n, null) ) {
        correct++;
      }
    }
    error = (count - (double) correct) / count;
    String test = Mnist8m._format.format(error * 100f);

    System.out.println("Train: " + train + ", test: " + test);
    return error;
  }
}