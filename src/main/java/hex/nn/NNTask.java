package hex.nn;

import hex.FrameTask;
import water.H2O.H2OCountedCompleter;

import java.util.Arrays;

public class NNTask extends FrameTask<NNTask> {
  final private NN _params;
  final private boolean _training;
  final private NNModel.NNModelInfo _input;

  private NNModel.NNModelInfo _output;
  public NNModel.NNModelInfo model_info() { return _output; }

  transient Neurons[] _neurons;

  // book-keeping
  int _chunk_node_count; //how many nodes are contributing with chunks?

  public NNTask(NN job, DataInfo dinfo, NNModel.NNModelInfo input, boolean training, float fraction){this(job,dinfo,input,training,fraction, null);}
  public NNTask(NN job, DataInfo dinfo, NNModel.NNModelInfo input, boolean training, float fraction, H2OCountedCompleter cmp){
    super(job,dinfo,cmp);
    _params=job;
    _training=training;
    _input=input;
    _useFraction=fraction;
  }

  // transfer ownership from input to output (which will be worked on)
  @Override protected void setupLocal(){
    _output = new NNModel.NNModelInfo(_input);
    _output.set_processed_local(0l);
  }

  // create local workspace (neurons)
  // and link them to shared weights
  @Override protected void chunkInit(int nrows){
    _neurons = makeNeurons(_dinfo, _output);
    _chunk_node_count = nrows > 0 ? 1 : 0;
//    Log.info("chunkInit: " + nrows + " rows (used: ~" + (nrows * _useFraction) + ")");
  }

  @Override public final void processRow(final double [] nums, final int numcats, final int [] cats, double [] responses){
    ((Neurons.Input)_neurons[0]).setInput(nums, numcats, cats);
    step(_neurons, _output, _training, responses);
  }

  @Override public void reduce(NNTask other){
    if (other._chunk_node_count > 0 //other NNTask was active (its model_info should be used for averaging)
            && other._output != _output) //other NNTask worked on a different model_info
    {
//      Log.info("before reduce: " + _output.get_processed_local() + " processed.");
      _output.add(other._output);
//      Log.info("after reduce: " + _output.get_processed_local() + " processed.");
      _chunk_node_count += other._chunk_node_count;
    }
  }

  @Override protected void postGlobal(){
//    Log.info("postGlobal: dividing by " + _chunk_node_count + ".");
    _output.div(_chunk_node_count);
//    Log.info("postGlobal: before global " + _output.get_processed_global() + ".");
    _output.add_processed_global(_output.get_processed_local());
    _output.set_processed_local(0l);
//    Log.info("postGlobal: after global " + _output.get_processed_global() + ".");
  }

  // Helper
  public static Neurons[] makeNeurons(DataInfo dinfo, NNModel.NNModelInfo minfo) {
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
      neurons[neurons.length - 1] = new Neurons.Softmax(dinfo._adaptedFrame.lastVec().domain().length, params.loss);
    else
      neurons[neurons.length - 1] = new Neurons.Linear(1);

    //copy parameters from NN, and set previous/input layer links
    for( int i = 0; i < neurons.length; i++ )
      neurons[i].init(neurons, i, params, minfo);

    return neurons;
  }

  // forward/backward propagation
  // assumption: layer 0 has _a filled with (horizontalized categoricals) double values
  static void step(Neurons[] neurons, NNModel.NNModelInfo minfo, boolean training, double[] responses) {
    for (int i=1; i<neurons.length-1; ++i)
      neurons[i].fprop(training);
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
      //This is equivalent to saying that each vms thinks that its rows come first in every epoch, which is probably
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
