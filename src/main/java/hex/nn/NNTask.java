package hex.nn;

import hex.FrameTask;
import water.H2O;
import water.H2O.H2OCountedCompleter;
import water.Job;

public class NNTask extends FrameTask<NNTask> {

  final protected NN _params;
  boolean _training;

  public NNModel.NNModelInfo _input, _output;

  transient Neurons[] _neurons;

  public NNTask(Job job, DataInfo dinfo, NN params, NNModel.NNModelInfo input, boolean training){this(job,dinfo,params,input,training,null);}
  public NNTask(Job job, DataInfo dinfo, NN params, NNModel.NNModelInfo input, boolean training, H2OCountedCompleter cmp){
    super(job,dinfo,cmp);
    _params=params;
    _training=training;
    _input=input;
  }




  // initialize node-local shared data (weights and biases)
  // transfer ownership from input to output (which will be worked on)
  @Override protected void setupLocal(){
    System.out.println("setupLocal");
    if (_input == null) _input = new NNModel.NNModelInfo(_params, _dinfo.fullN(), _params.response.toEnum().cardinality());
    _output = _input.deep_copy();
    _input = null;
  }

  // create local workspace (neurons)
  // and link them to shared weights
  @Override protected void chunkInit(){
    System.out.println("chunkInit");
    _neurons = makeNeurons(_dinfo, _output);
  }

  @Override public final void processRow(final double [] nums, final int ignored, final int [] cats, double [] responses){
    step(_neurons, _dinfo, _output, _training, nums, cats, responses);
  }

  @Override protected void chunkDone(){ }

  @Override public void reduce(NNTask other){
    _output.add(other._output);
  }

  @Override protected void postGlobal(){
    _output.div(H2O.CLOUD.size());
    _input = _output.deep_copy();
  }

  // Helper
  static Neurons[] makeNeurons(DataInfo dinfo, NNModel.NNModelInfo minfo) {
    final NN params = minfo.parameters;
    final int[] h = params.hidden;
    Neurons[] neurons = new Neurons[h.length + 2]; // input + hidden + output
    // input
    neurons[0] = new Neurons.Input(dinfo.fullN());
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
    if( params.classification ) {
      neurons[neurons.length - 1] = new Neurons.Softmax(params.response.toEnum().cardinality(), params.loss);
    }
    else
      neurons[neurons.length - 1] = new Neurons.Linear(1);

    //copy parameters from NN, and set previous/input layer links
    for( int i = 0; i < neurons.length; i++ )
      neurons[i].init(neurons, i, params, minfo);

    return neurons;
  }

  // forward/backward propagation
  static void step(Neurons[] neurons, DataInfo dinfo, NNModel.NNModelInfo minfo,
                   boolean training, final double[] nums, final int [] cats, double[] responses) {
    ((Neurons.Input)neurons[0]).fprop(nums, cats, dinfo); //copy nums and cats into _a ("activation" of input neurons)
    for (int i=1; i<neurons.length-1; ++i) {
      neurons[i].fprop(training);
    }
    if (minfo.parameters.classification) {
      ((Neurons.Softmax)neurons[neurons.length-1]).fprop();
      if (training) {
        assert((double)(int)responses[0] == responses[0]);
        final int target = (int)responses[0];
        ((Neurons.Softmax)neurons[neurons.length-1]).bprop(target);
      }
    }
    else {
      ((Neurons.Linear)neurons[neurons.length-1]).fprop();
      if (training) {
        final double target = responses[0];
        ((Neurons.Linear)neurons[neurons.length-1]).bprop(target);
      }
    }
    if (training) {
      for (int i=neurons.length-2; i>0; --i) {
        neurons[i].bprop();
      }
      minfo.processed++;
      if (minfo.processed % 1000 == 0) System.out.println("Processed: " + minfo.processed);
    }
  }

}
