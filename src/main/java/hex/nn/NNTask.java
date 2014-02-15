package hex.nn;

import hex.FrameTask;
import water.H2O;
import water.H2O.H2OCountedCompleter;
import water.util.Log;

import java.util.Arrays;

public class NNTask extends FrameTask<NNTask> {
  final private boolean _training;
  private NNModel.NNModelInfo _input;
  NNModel.NNModelInfo _output;
  public NNModel.NNModelInfo model_info() { return _output; }

  transient Neurons[] _neurons;

  int _chunk_node_count = 1;

  public NNTask(DataInfo dinfo, NNModel.NNModelInfo input, boolean training, float fraction, boolean shuffle){this(dinfo,input,training,fraction,shuffle,null);}
  private NNTask(DataInfo dinfo, NNModel.NNModelInfo input, boolean training, float fraction, boolean shuffle, H2OCountedCompleter cmp){
    super(input.job(),dinfo,cmp);
    _training=training;
    _input=input;
    _useFraction=fraction;
    _seed=_input.get_params().seed;
    _shuffle = shuffle;
    assert(_output == null);
  }

  // transfer ownership from input to output (which will be worked on)
  @Override protected void setupLocal(){
    _output = _input;
//    _output = new NNModel.NNModelInfo(_input);
    _input = null;
    _output.set_processed_local(0l);
  }

  // create local workspace (neurons)
  // and link them to shared weights
  @Override protected void chunkInit(){
    _neurons = makeNeuronsForTraining(_dinfo, _output);
  }

  @Override public final void processRow(long seed, final double [] nums, final int numcats, final int [] cats, double [] responses){
    seed += model_info().get_processed_total(); //avoid periodicity
    ((Neurons.Input)_neurons[0]).setInput(seed, nums, numcats, cats);
    step(seed, _neurons, _output, _training, responses);
  }

  @Override public void reduce(NNTask other){
//    Log.info("reduce: my local " + _output.get_processed_local());
//    Log.info("reduce: other local " + other._output.get_processed_local());
    if (other._output.get_processed_local() > 0 //other NNTask was active (its model_info should be used for averaging)
            && other._output != _output) //other NNTask worked on a different model_info
    {
      _output.add(other._output);
      _chunk_node_count += other._chunk_node_count;
//      Log.info("reduce: adding them together, now have " + _chunk_node_count + " contributions, total: " + _output.get_processed_local());
    }
  }

  @Override protected void postGlobal(){
    if (H2O.CLOUD.size() > 1) {
      Log.info("Synchronizing across " + _chunk_node_count + " H2O nodes.");
      if (_chunk_node_count < H2O.CLOUD.size())
        Log.warn(H2O.CLOUD.size() - _chunk_node_count + " nodes (out of "
                + H2O.CLOUD.size() + ") are not contributing to model updates. Consider using a larger training dataset (or fewer H2O nodes).");
    }
    _output.div(_chunk_node_count);
    _output.add_processed_global(_output.get_processed_local());
    _output.set_processed_local(0l);
    assert(_input == null);
  }

  public static Neurons[] makeNeuronsForTraining(final DataInfo dinfo, final NNModel.NNModelInfo minfo) {
    return makeNeurons(dinfo, minfo, true);
  }
  public static Neurons[] makeNeuronsForTesting(final DataInfo dinfo, final NNModel.NNModelInfo minfo) {
    return makeNeurons(dinfo, minfo, false);
  }

  // Helper
  private static Neurons[] makeNeurons(final DataInfo dinfo, final NNModel.NNModelInfo minfo, boolean training) {
    final NN params = minfo.get_params();
    final int[] h = params.hidden;
    Neurons[] neurons = new Neurons[h.length + 2]; // input + hidden + output
    // input
    neurons[0] = new Neurons.Input(dinfo.fullN(), dinfo);
    // hidden
    for( int i = 0; i < h.length; i++ ) {
      switch( params.activation ) {
        case Tanh:
          neurons[i+1] = new Neurons.Tanh(h[i]);
          break;
        case TanhWithDropout:
          neurons[i+1] = new Neurons.TanhDropout(h[i]);
          break;
        case Rectifier:
          neurons[i+1] = new Neurons.Rectifier(h[i]);
          break;
        case RectifierWithDropout:
          neurons[i+1] = new Neurons.RectifierDropout(h[i]);
          break;
        case Maxout:
          neurons[i+1] = new Neurons.Maxout(h[i]);
          break;
      }
    }
    // output
    if(params.classification)
      neurons[neurons.length - 1] = new Neurons.Softmax(dinfo._adaptedFrame.lastVec().domain().length);
    else
      neurons[neurons.length - 1] = new Neurons.Linear(1);

    //copy parameters from NN, and set previous/input layer links
    for( int i = 0; i < neurons.length; i++ )
      neurons[i].init(neurons, i, params, minfo, training);

    return neurons;
  }

  // forward/backward propagation
  // assumption: layer 0 has _a filled with (horizontalized categoricals) double values
  public static void step(long row, Neurons[] neurons, NNModel.NNModelInfo minfo, boolean training, double[] responses) {
    for (int i=1; i<neurons.length-1; ++i) {
      neurons[i].fprop(row, training);
    }
    if (minfo.get_params().classification) {
      ((Neurons.Softmax)neurons[neurons.length-1]).fprop();
      if (training) {
        for( int i = 1; i < neurons.length - 1; i++ )
          Arrays.fill(neurons[i]._e, 0);
        assert((double)(int)responses[0] == responses[0]);
        final int target_label = (int)responses[0];
        ((Neurons.Softmax)neurons[neurons.length-1]).bprop(target_label);
      }
    }
    else {
      ((Neurons.Linear)neurons[neurons.length-1]).fprop();
      if (training) {
        for( int i = 1; i < neurons.length - 1; i++ )
          Arrays.fill(neurons[i]._e, 0);
        final double target_value = responses[0];
        ((Neurons.Linear)neurons[neurons.length-1]).bprop(target_value);
      }
    }
    if (training) {
      for (int i=neurons.length-2; i>0; --i)
        neurons[i].bprop();

      /**
       * Let neurons know the real-time number of processed rows -> for accurate learning rate decay, etc.
       */
      //Note: in multi-vm operation, all vms sync their number of processed rows after every reduce() call.
      //That means that the number of processed rows will jump regularly, and then continue to increase in steps of 1.
      //This is equivalent to saying that each VM thinks that its rows come first in every epoch, which is probably
      //the closest thing to do when trying to match the single-node behavior.
      minfo.add_processed_local(1);
//      if (minfo.get_processed_local() % 1000 == 0) {
//        Log.info("processed global: " + minfo.get_processed_global());
//        Log.info("processed local : " + minfo.get_processed_local());
//        Log.info("processed total : " + minfo.get_processed_total());
//      }
    }
  }

}
