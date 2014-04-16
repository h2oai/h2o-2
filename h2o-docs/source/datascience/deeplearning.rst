.. _DLmath:


Deep Learning
------------------------------

H2O Deep Learning is based on a multi-layer feed-forward artificial neural
network that is trained with stochastic gradient descent using
back-propagation. The network can contain a large number of hidden layers
consisting of neurons with tanh, rectifier and maxout activation functions.
Advanced features such as adaptive learning rate, rate annealing, momentum
training, dropout, L1/L2 regularization, checkpointing and grid search enable
high predictive accuracy. Each compute node trains a copy of the global model
parameters on its local data with multi-threading (asynchronously), and
contributes periodically to the global model via model averaging across the
network.

  
Defining a Deep Learning Model
""""""""""""""""""""""""""""""""

H2O Deep Learning models have many input parameters, many of which are only accessible via
the expert mode, and their default values should be fine for most use cases.
Please read the following instructions before building extensive Deep Learning
models. Many of the parameters allow specification of multiple values for grid
search (e.g., comma-separated list "10,25,40" or from:to:step range "10:40:5").
The application of grid search and successive continuation of winning models
via checkpoint restart is highly recommended as model performance can vary
greatly.

**destination key**

    Name of the model to be trained. Will be auto-generated if omitted.

**source**

   A hex key associated with the parsed traing data.
 
**response**

    The dependent or target variable of interest.  Can be numerical or
    a factor (categorical).
	
**ignored columns** 
     
    This field will auto populate a list of the columns from the data
    set in use. The user-selected set of columns are the features
    that will be omitted from the model. Additionally - users can
    specify whether the model should omit constant columns by
    selecting expert settings and checking the tic box indicating
    **Ignore const cols**.

**classification** 
     
    Check box indicating whether the dependent variable is to be
    treated as a factor or a continuous variable. 

**validation** 

    A unique data set with the same shape and features as the
    training data to be used in model validation (i.e., production of
    error rates on data not used in model building). 

**checkpoint**
      
    A model key associated with a previously trained Deep Learning
    model. This option allows users to build a new model as a
    continuation of a previously generated model (e.g., by a grid search).

**expert mode** 

    Unlock expert mode parameters than can affect model building speed,
    predictive accuracy and scoring. Leaving expert mode parameters at default
    values is fine for many problems, but best results on complex datasets are often
    only attainable via expert mode options.
    
**activation**

    The activation function (non-linearity) to be used the neurons in the
    hidden layers.

    *Tanh*: Hyperbolic tangent function (same as scaled and shifted sigmoid).
    
    *Rectifier*: Chooses the maximum of (0, x) where x is the input value.

    *Maxout*: Choose the maximum coordinate of the input vector.

    *With Dropout*: Zero out a random user-given fraction of the
    incoming weights to each hidden layer during training, for each
    training row. This effectively trains exponentially many models at
    once, and can improve generalization. 

**hidden**

    The number and size of each hidden layer in the model. 
    For example, if a user specifies "100,200,100" a model with 3 hidden
    layers will be produced, and the middle hidden layer will have 200
    neurons.To specify a grid search, add parentheses around each
    model's specification: "(100,100), (50,50,50), (20,20,20,20)".  

**epochs** 

    The number of passes over the training dataset to be carried out. 

**train samples per iteration**

    The number of training data rows to be processed per iteration. Note that
    independent of this parameter, each row is used immediately to update the model
    with (online) stochastic gradient descent. This parameter controls the
    synchronization period between nodes in a distributed environment and the
    frequency at which scoring and model cancellation can happen. For example, if
    it is set to 10,000 on H2O running on 4 nodes, then each node will
    process 2,500 rows per iteration, sampling randomly from their local data.
    Then, model averaging between the nodes takes place, and scoring can happen
    (dependent on scoring interval and duty factor). Special values are 0 for
    one epoch per iteration and -1 for processing the maximum amount of data
    per iteration. If **replicate training data** is enabled, N epochs
    will be trained per iteration on N nodes, otherwise one epoch.

**seed**

    The random seed controls sampling and initialization. Reproducible
    results are only expected with single-threaded operation (i.e.,
    when running on one node, turning off load balancing and providing
    a small dataset that fits in one chunk).  In general, the
    multi-threaded asynchronous updates to the model parameters will
    result in (intentional) race conditions and non-reproducible
    results. Note that deterministic sampling and initialization might
    still lead to some weak sense of determinism in the model.

**learning rate**

    The implemented adaptive learning rate algorithm (ADADELTA) automatically
    combines the benefits of learning rate annealing and momentum
    training to avoid slow convergence. Specification of only two
    parameters (rho and epsilon) simplifies hyper parameter search.

    In some cases, manually controlled (non-adaptive) learning rate and
    momentum specifications can lead to better results, but require the
    specification (and hyper parameter search) of up to 7 parameters.
    If the model is built on a topology with many local minima or
    long plateaus, it is possible for a constant learning rate to produce
    sub-optimal results. Learning rate annealing allows digging deeper into
    local minima, while rate decay allows specification of different
    learning rates per layer.  When the gradient is being estimated in
    a long valley in the optimization landscape, a large learning rate
    can cause the gradient to oscillate and move in the wrong
    direction. When the gradient is computed on a relatively flat
    surface with small learning rates, the model can converge far
    slower than necessary.

**momentum**

    When adaptive learning rate is disabled, the magnitude of the weight
    updates are determined by the user specified learning rate
    (potentially annealed), and are a function of the difference
    between the predicted value and the target value. That difference,
    generally called delta, is only available at the output layer. To
    correct the output at each hidden layer, back propagation is
    used. Momentum modifies back propagation by allowing prior
    iterations to influence the current update. Using the momentum
    parameter can aid in avoiding local minima and the associated
    instability. Too much momentum can lead to instabilities, that's
    why the momentum is best ramped up slowly.
       
    *Momentum start:* Initial momentum at the start of model building.
       
    *Momentum ramp:* The number of data samples for which the momentum
    rises from its starting value to its final value (momentum stable).

    *Momentum stable:* The final momentum value after the ramp is over.

**Nesterov accelerated Gaadient** 

    The Nesterov accelerated gradient descent method is a modification to
    traditional gradient descent for convex functions. The method relies on
    gradient information at various points to build a polynomial approximation that
    minimizes the residuals in fewer iterations of the descent. 

**input dropout ratio**

    A fraction of the features for each training row to be omitted from training in order
    to improve generalization (dimension sampling).

**hidden dropout ratios**

    A fraction of the inputs for each hidden layer to be omitted from training in order
    to improve generalization. Defaults to 0.5 for each hidden layer if omitted.

**L1 regularization** 

    A regularization method that constrains the absolute value of the weights and
    has the net effect of dropping some weights (setting them to zero) from a model
    to reduce complexity and avoid overfitting. 

**L2 regularization** 

    A regularization method that constrdains the sum of the squared
    weights. This method introduces bias into parameter estimates, but
    frequently produces substantial gains in modeling as estimate variance is
    reduced. 

**max w2**

    A maximum on the sum of the squared incoming weights into
    any one neuron. This tuning parameter is especially useful for unbound
    activation functions such as Maxout or Rectifier.

**initial weight distribution**

    The distribution from which initial weights are to be drawn. The default
    option is an optimized initialization that considers the size of the network.
    The "uniform" option uses a uniform distribution with a mean of 0 and a given
    interval. The "normal" option draws weights from the standard normal
    distribution with a mean of 0 and given standard deviation.

**initial weight scale**

    The scale of the distribution function for Uniform or Normal distributions.
    For Uniform, the values are drawn uniformly from -initial_weight_scale...initial_weight_scale.
    For Normal, the values are drawn from a Normal distribution with a standard deviation of initial_weight_scale.

**loss function** 

    The loss (error) function to be optimized by the model. 

    *Cross Entropy* Used when the model output consists of independent
    hypotheses, and the outputs can be interpreted as the probability that each
    hypothesis is true. Cross entropy is the recommended loss function when the
    target values are class labels, and especially for imbalanced data.
    It strongly penalizes error in the prediction of the actual class label.

    *Mean Square* Used when the model output are continuous real values, but can
    be used for classification as well (where it emphasizes the error on all
    output classes, not just for the actual class).

**score interval**

    The minimum time (in seconds) to elapse between model scoring. The actual
    interval is determined by the number of training samples per iteration and the scoring duty cycle.

**score training samples**

    The number of training dataset points to be used for scoring. Will be
    randomly sampled. Use 0 for selecting the entire training dataset.

**score validation samples** 

    The number of validation dataset points to be used for scoring. Can be
    randomly sampled or stratified (if "balance classes" is set and "score
    validation sampling" is set to stratify). Use 0 for selecting the entire
    training dataset.

**score duty cycle**
    Maximum fraction of wall clock time spent on model scoring on training and validation samples,
    and on diagnostics such as computation of feature importances (i.e., not on training).

**classification stop**

    The stopping criteria in terms of classification error (1-accuracy) on the
    training data scoring dataset. When the error is at or below this threshold,
    training stops. 

**regression stop**

    The stopping criteria in terms of regression error (MSE) on the training
    data scoring dataset. When the error is at or below this threshold, training
    stops.

**quiet mode**

    Enable quiet mode for less output to standard output.

**max confusion matrix** 

    For classification models, the maximum size (in terms of classes) of the
    confusion matrix for it to be printed. This option is meant to avoid printing
    extremely large confusion matrices.

**max hit ratio K** 

    The maximum number (top K) of predictions to use for hit ratio computation (for multi-class only, 0 to disable)

**balance classes** 

    For imbalanced data, balance training data class counts via
    over/under-sampling. This can result in improved predictive accuracy.

**max after balance size** 

    When classes are balanced, limit the resulting dataset size to the
    specified multiple of the original dataset size.

**score validation sampling**

    Method used to sample the validation dataset for scoring, see Score Validation Samples above.

**diagnostics**

    Gather diagnostics for hidden layers, such as mean and RMS values of learning
    rate, momentum, weights and biases.

**variable importance**

    Whether to compute variable importances for input features.
    The implemented method (by Gedeon) considers the weights connecting the
    input features to the first two hidden layers.

**fast mode**
    
    Enable fast mode (minor approximation in back-propagation), should not affect results significantly.

**ignore const cols**

    Ignore constant training columns (no information can be gained anyway).

**force load balance** 

    Increase training speed on small datasets by splitting it into many chunks
    to allow utilization of all cores.

**replicate training data**

    Replicate the entire training dataset onto every node for faster training on small datasets.

**single node mode**

    Run on a single node for fine-tuning of model parameters. Can be useful for
    checkpoint resumes after training on multiple nodes for fast initial
    convergence.

**shuffle training data** 

    Enable shuffling of training data (on each node). This option is
    recommended if training data is replicated on N nodes, and the number of training samples per iteration
    is close to N times the dataset size, where all nodes train will (almost) all
    the data. It is automatically enabled if the number of training samples per iteration is set to -1 (or to N
    times the dataset size or larger).

Interpreting the Model
""""""""""""""""""""""""

The model view page displays information about the Deep Learning model being trained.

**Diagnostics table**
    If diagnostics is enabled, information for each layer is displayed.

    *Units* The number of units (or artificial neurons) in the layer

    *Type* The type of layer (used activation function). Each model
    will have one input and one output layer. Hidden layers are
    identified by the activation function specified. 

    *Dropout* For input layer, the percentage of dropped features for
    each training row. For hidden layers, the percentage of incoming
    weights dropped from training at that layer. Note that dropout is
    randomized for each training row.

    *L1, L2* The L1 and L2 regularization penalty applied to the
    layer. 

    *Rate, Weight and Bias* The per-layer learning rate, weight and bias statistics are displayed.
 
**Scoring** 

    If a validation set was given, the scoring results are displayed for
    the validation set (or a sample thereof). Otherwise, scoring is performed on
    the training dataset (or a sample thereof).

**Confusion matrix**

    For classification models, a table showing the number of actual
    observations in a particular class relative to the number of predicted
    observations in a class.

**Hit ratio table**

    A table displaying the percentage of instances where the actual
    class label assigned to an observation is in the top K classes predicted by the
    model. For instance, in a four class classifier on values A, B, C, D, a
    particular observation is predicted to be class A with a probability of .6 of
    being A, .2 probability of being B, a .1 probability of being C, and a .1
    probability of being D. If the true class is B, the observation will be counted
    in the hit rate for K=2, but not in the hit rate of K=1. 

**Variable Importance** 

    A table listing the importance of variables listed from greatest
    importance, to least importance. Note that variable importances are notoriously
    difficult to compute for Neural Net models. Gedeon's method is implemented here.



References
""""""""""""""""""""""""""""""""

    Deep Learning http://en.wikipedia.org/wiki/Deep_learning

    Artificial Neural Network http://en.wikipedia.org/wiki/Artificial_neural_network

    ADADELTA http://arxiv.org/abs/1212.5701

    Momentum http://www.cs.toronto.edu/~fritz/absps/momentum.pdf

    Dropout http://arxiv.org/pdf/1207.0580.pdf and http://arxiv.org/abs/1307.1493

    Feature Importance http://www.ncbi.nlm.nih.gov/pubmed/9327276
