package samples;

import static water.api.RequestBuilders._format;
import hex.*;
import hex.Layer.Tanh;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import water.fvec.Vec;

/**
 * Same as previous MNIST sample but with more layers and pre-training.
 */
public class NeuralNetMnistDeep extends NeuralNetMnist {
  public static void main(String[] args) throws Exception {
    CloudLocal.launch(1, NeuralNetMnistDeep.class);
    // CloudConnect.launch("localhost:54321", NeuralNetMnist.class);
  }

  @Override public Layer[] build(Vec[] data, Vec labels, VecsInput inputStats, VecSoftmax outputStats) {
    Layer[] ls = new Layer[5];
    ls[0] = new VecsInput(data, inputStats);
    for( int i = 1; i < ls.length - 1; i++ ) {
      ls[i] = new Tanh(500);
      ls[i].rate = .05f;
    }
    ls[ls.length - 1] = new VecSoftmax(labels, outputStats);
    ls[ls.length - 1].rate = .02f;
    for( int i = 0; i < ls.length; i++ ) {
      ls[i].l2 = .0001f;
      ls[i].rateAnnealing = 1 / 2e6f;
      ls[i].init(ls, i);
    }
    return ls;
  }

  @Override Trainer startTraining(Layer[] ls) {
    Trainer trainer = new Trainer.MapReduce(ls);
    //Trainer trainer = new Trainer.Direct(ls);

    for( int i = 0; i < ls.length; i++ ) {
      System.out.println("Pre-training level " + i);
      long time = System.nanoTime();
      preTrain(ls, i, trainer);
      System.out.println((int) ((System.nanoTime() - time) / 1e6) + " ms");
    }

    trainer.start();
    return trainer;
  }

  void preTrain(Layer[] ls, int index, Trainer trainer) {
    Layer[] pre = new Layer[5];
    VecsInput input = (VecsInput) ls[0];
    pre[0] = new VecsInput(input._vecs, (VecsInput) ls[0]);
    for( int i = 1; i < index - 1; i++ ) {
      ls[i] = new Tanh(500);
      ls[i]._rate = .05f;
    }
    ls[ls.length - 1] = new VecSoftmax(labels, outputStats);
    ls[ls.length - 1]._rate = .02f;
    for( int i = 0; i < ls.length; i++ ) {
      ls[i]._l2 = .0001f;
      ls[i]._rateAnnealing = 1 / 2e6f;
      ls[i].init(ls, i);
    }

    int n = 0;
    float trainError = 0, testError = 0, trainEnergy = 0, testEnergy = 0;
    for( int i = 0; i < 10000; i++ ) {
      for( int level = 0; level < upTo; level++ )
        ls[level].fprop();

      n = up(_train, n, inputs, upTo);

      for( int b = 0; b < trainer._batch; b++ )
        _ls[upTo].contrastiveDivergence(inputs[b]);
      _ls[upTo].adjust(n);

      if( i % 100 == 0 ) {
        up(_train, 0, tester, upTo);
        for( int b = 0; b < tester.length; b++ ) {
          trainError += _ls[upTo].error(tester[b]);
          trainEnergy += _ls[upTo].freeEnergy(tester[b]);
        }

        up(_train, 0, tester, upTo);
        for( int b = 0; b < tester.length; b++ ) {
          testError += _ls[upTo].error(tester[b]);
          testEnergy += _ls[upTo].freeEnergy(tester[b]);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(i);
        sb.append(", train err: ");
        sb.append(_format.format(trainError));
        sb.append(", test err: ");
        sb.append(_format.format(testError));
        sb.append(", train egy: ");
        sb.append(_format.format(trainEnergy));
        sb.append(", test egy: ");
        sb.append(_format.format(testEnergy));
        System.out.println(sb.toString());
        trainError = testError = trainEnergy = testEnergy = 0;
      }
    }
  }
}
