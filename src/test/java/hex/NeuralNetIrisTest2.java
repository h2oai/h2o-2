package hex;

import hex.nn.NN;
import hex.nn.NNModel;
import hex.nn.NNTask;
import hex.nn.Neurons;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.JUnitRunnerDebug;
import water.Key;
import water.TestUtil;
import water.UKV;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;
import water.util.Utils;

import java.util.ArrayList;
import java.util.Random;

public class NeuralNetIrisTest2 extends TestUtil {
  static final String PATH = "smalldata/iris/iris.csv";
  Frame _train, _test;

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  void compareVal(double a, double b, double abseps, double releps) {
    // check for equality
    if (Double.compare(a, b) == 0) {
      return;
    }
    // check for small relative error
    else if (Math.abs(a-b)/Math.max(a,b) < releps) {
      return;
    }
    // check for small absolute error
    else if (Math.abs(a - b) <= abseps) {
      return;
    }
    // fail
    else Assert.failNotEquals("Not equal: ", new Double(a), new Double(b));
  }

  @Test public void compare() throws Exception {

    // Testing different things
    // Note: Microsoft reference implementation is only for Tanh + MSE, rectifier and MCE are implemented by 0xdata (trivial).
    // Note: Initial weight distributions are copied, but what is tested is the stability behavior.
    //NN.Activation[] activations = { NN.Activation.Tanh, NN.Activation.Rectifier };
    NN.Activation[] activations = { NN.Activation.Tanh };
    //NN.Loss[] losses = { NN.Loss.MeanSquare, NN.Loss.CrossEntropy };
    NN.Loss[] losses = { NN.Loss.MeanSquare };
    NN.InitialWeightDistribution[] dists = {
            NN.InitialWeightDistribution.Normal,
            //NN.InitialWeightDistribution.Uniform,
            //NN.InitialWeightDistribution.UniformAdaptive
            };
    double[] initial_weight_scales = { 0.0258 };
    double[] holdout_ratios = { 0.8 };


    int hogwild_runs = 0;
    int hogwild_errors = 0;
    for (NN.Activation activation : activations) {
      for (NN.Loss loss : losses) {
        for (NN.InitialWeightDistribution dist : dists) {
          for (double scale : initial_weight_scales) {
            for (double holdout_ratio : holdout_ratios) {

              Log.info("");
              Log.info("STARTING.");
              Log.info("Running with " + activation.name() + " activation function and " + loss.name() + " loss function.");
              Log.info("Initialization with " + dist.name() + " distribution and " + scale + " scale, holdout ratio " + holdout_ratio);

              NeuralNetMLPReference2 ref = new NeuralNetMLPReference2();

              final long seed = new Random().nextLong(); //Actually change the seed every time!
              Log.info("Using seed " + seed);
              ref.init(activation, Utils.getDeterRNG(seed), holdout_ratio);

              // Parse Iris and shuffle the same way as ref
              Key file = NFSFileVec.make(find_test_file(PATH));
              Key pars = Key.make();
              Frame frame = ParseDataset2.parse(pars, new Key[] { file });
              UKV.remove(file);

              double[][] rows = new double[(int) frame.numRows()][frame.numCols()];
              String[] names = new String[frame.numCols()];
              for( int c = 0; c < frame.numCols(); c++ ) {
                names[c] = "ColumnName" + c;
                for( int r = 0; r < frame.numRows(); r++ )
                  rows[r][c] = frame.vecs()[c].at(r);
              }

              Random rand = Utils.getDeterRNG(seed);
              for( int i = rows.length - 1; i >= 0; i-- ) {
                int shuffle = rand.nextInt(i + 1);
                double[] row = rows[shuffle];
                rows[shuffle] = rows[i];
                rows[i] = row;
              }

              int limit = (int) (frame.numRows() * holdout_ratio);
              _train = frame(names, Utils.subarray(rows, 0, limit));
              _test = frame(names, Utils.subarray(rows, limit, (int) frame.numRows() - limit));

              NN p = new NN();
              p.hidden = new int[]{7};
              p.rate = 0.01;
              p.activation = activation;
              p.max_w2 = Double.MAX_VALUE;
              p.rate = 0.01;
              p.epochs = 13*17;
              p.activation = activation;
              p.input_dropout_ratio = 0;
              p.rate_annealing = 0;
              p.l1 = 0;
              p.l2 = 0;
              p.momentum_start = 0;
              p.momentum_ramp = 0;
              p.momentum_stable = 0;
              p.initial_weight_distribution = dist;
              p.initial_weight_scale = scale;
              p.classification = true;
              p.source = _train;
              p.validation = null;
              p.response = _train.lastVec();
              p.destination_key = Key.make("iris_test.hex");

              // outsource start
              Frame fr = new Frame(p.source._names.clone(), p.source.vecs().clone());

              boolean toE = false;
              final Vec[] vecs =  fr.vecs();
              ArrayList<Integer> constantOrNAs = new ArrayList<Integer>();
              for(int i = 0; i < vecs.length-1; ++i) {// put response to the end
                if(vecs[i] == p.response){
                  if (p.classification) {
                    toE = true;
                    fr.add(fr._names[i], fr.remove(i).toEnum());
                  }
                  else
                    fr.add(fr._names[i], fr.remove(i));
                  break;
                }
              }
              if (p.classification && !p.response.isEnum() && vecs[vecs.length-1] == p.response) {
                toE = true;
                vecs[vecs.length-1] = vecs[vecs.length-1].toEnum();
              }

              for(int i = 0; i < vecs.length-1; ++i) // remove constant cols and cols with too many NAs
                if(vecs[i].min() == vecs[i].max() || vecs[i].naCnt() > vecs[i].length()*0.2)constantOrNAs.add(i);

              if(!constantOrNAs.isEmpty()){
                int [] cols = new int[constantOrNAs.size()];
                for(int i = 0; i < cols.length; ++i)cols[i] = constantOrNAs.get(i);
                fr.remove(cols);
              }
              //outsource end

              p._dinfo = new FrameTask.DataInfo(fr, 1, true);
              p.initModel(); //randomize weights, but don't start training yet

              NNModel mymodel = UKV.get(p.dest()); //get the model
              Neurons[] neurons = NNTask.makeNeurons(p._dinfo, mymodel.model_info);

              // use the same random weights for the reference implementation
              Neurons l = neurons[1];
              for( int o = 0; o < l._a.length; o++ ) {
                for( int i = 0; i < l._previous._a.length; i++ )
                  ref._nn.ihWeights[i][o] = l._w[o * l._previous._a.length + i];
                ref._nn.hBiases[o] = l._b[o];
              }
              l = neurons[2];
              for( int o = 0; o < l._a.length; o++ ) {
                for( int i = 0; i < l._previous._a.length; i++ )
                  ref._nn.hoWeights[i][o] = l._w[o * l._previous._a.length + i];
                ref._nn.oBiases[o] = l._b[o];
              }

              // Train the Reference
              ref.train((int)p.epochs, p.rate, loss);

              // Train H2O
              p.trainModel();
              mymodel = UKV.get(p.dest()); //get the model
              neurons = NNTask.makeNeurons(p._dinfo, mymodel.model_info); //link the weights to the neurons

              // tiny absolute and relative tolerances for single threaded mode
              double abseps = 1e-15;
              double releps = 1e-12; // relative error check only triggers if abs(a-b) > abseps
              double weight_mse = 0;

              // Make sure weights are equal
              l = neurons[1];
              for( int o = 0; o < l._a.length; o++ ) {
                for( int i = 0; i < l._previous._a.length; i++ ) {
                  double a = ref._nn.ihWeights[i][o];
                  double b = l._w[o * l._previous._a.length + i];
                  weight_mse += (a-b) * (a-b);
                }
              }
              weight_mse /= l._a.length * l._previous._a.length;

              // Make sure output layer (predictions) are equal
              for( int o = 0; o < neurons[2]._a.length; o++ ) {
                double a = ref._nn.outputs[o];
                double b = neurons[2]._a[o];
                compareVal(a, b, abseps, releps);
              }

              Frame fpreds;
              double myTrainAcc;
              {
                fpreds = mymodel.score(_train);
                water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
                CM.actual = _train;
                CM.vactual = _train.lastVec();
                CM.predict = fpreds;
                CM.vpredict = fpreds.vecs()[0];
                CM.serve();
                myTrainAcc = CM.toASCII(new StringBuilder());
                fpreds.remove();
              }

              double myTestAcc;
              {
                fpreds = mymodel.score(_test);
                water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
                CM.actual = _test;
                CM.vactual = _test.lastVec();
                CM.predict = fpreds;
                CM.vpredict = fpreds.vecs()[0];
                CM.serve();
                myTestAcc = CM.toASCII(new StringBuilder());
                fpreds.remove();
              }

              double trainAcc = ref._nn.Accuracy(ref._trainData);
              double testAcc = ref._nn.Accuracy(ref._testData);

              final boolean hogwild_error = (trainAcc != myTrainAcc || testAcc != myTestAcc);
              Log.info("DONE. " + (hogwild_error ? "Threaded mode resulted in errors due to Hogwild." : ""));
              Log.info("MSE of Hogwild H2O weights: " + weight_mse + ".");
              hogwild_errors += hogwild_error == true ? 1 : 0;
              Log.info("H2O  training error : " + myTrainAcc*100 + "%, test error: " + myTestAcc*100 + "%" +
                      (trainAcc != myTrainAcc|| testAcc != myTestAcc? " HOGWILD! " : ""));
              Log.info("REF  training error : " + trainAcc*100 + "%, test error: " + testAcc*100 + "%");

              if (toE) UKV.remove(vecs[vecs.length-1]._key);
              UKV.remove(pars);
              _train.remove();
              _test.remove();
              fr.remove();
              UKV.remove(p.dest());

              hogwild_runs++;
            }
          }
        }
      }
    }

    Log.info("===============================================================");
    Log.info("Number of differences due to Hogwild: " + hogwild_errors + " (out of " + hogwild_runs + " runs).");
    Log.info("===============================================================");
  }
}
