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
    if (_input == null) _input = new NNModel.NNModelInfo(_params, _dinfo.fullN(), _dinfo._adaptedFrame.lastVec().domain().length);

    _output = _input.deep_copy();
    _input = null;
  }

  // create local workspace (neurons)
  // and link them to shared weights
  @Override protected void chunkInit(){
    _neurons = makeNeurons(_dinfo, _output);
  }

  @Override public final void processRow(final double [] nums, final int numcats, final int [] cats, double [] responses){
    ((Neurons.Input)_neurons[0]).setInput(nums, numcats, cats);
    step(_neurons, _output, _training, responses);
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
  public static Neurons[] makeNeurons(DataInfo dinfo, NNModel.NNModelInfo minfo) {
    final NN params = minfo.parameters;
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
    if( params.classification ) {
      neurons[neurons.length - 1] = new Neurons.Softmax(dinfo._adaptedFrame.lastVec().domain().length, params.loss);
    }
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
      if (minfo.processed % 10000 == 0) System.out.println("Processed: " + minfo.processed);
    }
  }

}
