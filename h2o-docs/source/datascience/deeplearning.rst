.. _DLmath:


Deep Learning
------------------------------

Deep Learning relies on interconnected nodes and weighted
information paths, which are adapted to minimize prediction error via
back propogation,  to produce non-linear models of complex
relationships. 
  
  
Defining a Deep Learning Model
""""""""""""""""""""""""""""""""

**Response:**
  The dependent or target variable of interest.   
	
**Ignored Columns:** 
     
     This field will auto populate a list of the columns from the data
     set in use. The user selected set of columns are the features
     that will be omitted from the model. Additionally - users can
     specify whether the model should omit constant columns by
     selecting expert settings and checking the tic box indicating
     **Ignore const cols**.

**Classification** 
     
     Checkbox indicating whether the dependent variable is to be
     treated as a factor or a continious variable. 

**Validation** 

     A unique data set with the same shape and features as the
     training data to be used in model validation (i.e. production of
     error rates on data not used in model building). 

**Checkpoint**
      
     A model key associated with a previously run deep learning
     model. This option allows users to build a new model as a
     continuation of a prevously generated model.  


**Expert mode** 

     When selected **Expert mode** allows users to specify expert
     settings, explained in more detail below. 
     *max w2*
 
    
**Activation**

      The activation function to be used at each of the nodes in the
      hidden layers. 

      *Tanh* :Hyperbolic tangent function
      
      *Rectifier*

      *Maxout*

      *With Dropout* A percentage of the data will be omitted from
      training as data are presented to each hidden layer in order to
      improve generalization. 

**Hidden:**

     The number of hidden layers in the model. Multiple models can be
     specified and generated simultaneously. For example if a user
     specifies (300,300,100) a model with 3 layers off 100 hidden nodes
     each will be produced. To specify several different models with
     different dimensions enter information in the format (300, 300, 100),
     (200, 200), (200, 20).  
    

**Epochs** 

      The number of iterations to be carried out. In model training
      data is fed into an input layer and passed down weighted
      information paths, through each of the hidden layers, and a
      prediction is returned at the output layer. Deviations between
      the predicted values and the actual values are then calculated,
      and used to adjust the path weights to reduce the error between
      the predicted and true value. One full backward
      pass over the weighted paths is one epoch. 

**Mini Batch**

      Batch learning is a method in which the aggregated gradient
      contributions for all observations in the training set are
      obtained before weights are updated. Alternatively, users can specify
      mini-batch to update weights more frequently. If users specify
      mini-batch = 2000, the training data will be split into chunks
      of 2000 observations, and the model weights will be updated
      after each chunk is passed through the network.  

**Seed**

      Because of the random nature of the algorithm, models with the
      same specification can sometimes produce slightly different
      results. To control this behavior, users can specify a seed, 
      which will produce the same values for random components on 
      independent tries. 

**Adaptive Rate:**

       In the even that a model is specified over a topology with
       local minima or long plateaus, it is possible for a constant
       learning rate to produce sub-optimal results. When the gradient
       is being estimated in a well, a large learning rate can cause
       the gradient to oscillate and move in the wrong direction. When
       the gradient is being taken on a relatively flat surface, the
       model can converge far slower than necessary for small learning
       rates. Adaptive learning rates self adjust to avoid local
       minima or slow convergence.  

**Momentum:**

       The magnitude of the weight updates are determined by the user specified
       learning rate, and are a function of the difference between the
       predicted value and the target value. That difference,
       generally called delta, is only available at the output
       layer. To correct the output at each hidden layer, back
       propogation is used. Momentum modifies back propogation
       by allowing prior iterations to influence the current
       update. Using the momentum parameter can aid in avoiding local
       minima and the associated instability. 
       
       *Momentum start* The weight assigned to the results of the
       first sample passed through the model. 
       
       *Momentum ramp* The number of data samples for which results
       will be weighted. 

       *Momentum stable* The minimum weight to be attributed to the
       last weighted output. 

**Nestrov Accelerated** 

        The Nestrov Accelerated Gradient Descent method is a
	modification to traditional gradient descent for convex
	functions. The method relies on gradient information at
	various points to build a polynomial approximation that
	minimizes the residuals in fewer iterations of the 
        descent. 

**Input dropout ratio**

        A percentage of the data to be omited from training in order
	to improve generalization. 

**L1 regularization** 

        A regularization method that contrains the size of individual
	coefficients and has the net effect of dropping some
	coefficients from a model to reduce complexity and avoid
	overfitting. 

**L2 regularization** 

        A regularization method that constrains the sum of the squared
	coefficients. This method introduces bias into parameter
	estimates, but frequently produces substantial gains in
	modeling as estimate variance is reduced. 


**Max W2**

        A maximum on the sum of the squared weights of information
	paths input into any one unit. This tuning parameter functions
	in a manner similar to L2 Regularization on the hidden layers
	of the network. 

**Initial weight distribution**

         The distribution from which intial path weights are to be
	 drawn. When the norma option is selected weights are drawn
	 from the standard normal with a mean of 0 and a standard
	 deviation of 1. 

**Loss function** 

         The loss function to be optimized by the model. 

         *Cross Entropy* Used when the model output consists of
	 independent hypothesis, and the outputs can be interpreted as
	 the probabilty that each hypotesis is true. Cross entropy is
	 the reccomended loss function when the target values are
	 classifications, and especially when data are unbalanced. 

	 *Mean Square* Used when the model output are continuious real
	 values. 

**Score Interval**

         The number of seconds to elapse between model scoring. 

**Score Training Samples**

         The number of training set observations to be used in scoring. 

**Score Validation Samples** 

         The number of validation set observations to be used in
	 scoring. 

**Classification Stop**

         The stopping criteria in terms of classification error. When
	 error is at or below this threshold, the algorithm stops. 

**Regression Stop**

         The stopping criteria in terms of error. When error is at or
	 below this threshold, the algorithm stops. 

**Max Confusion Matrix** 

         The maximum number of classes to be shown in the returned
	 confusion matrix for classification models. 

**Max Hit Ratio K** 

           The maximum frequency of actual class label to be among the top-K
	   predicted class labels).

**Balance Classes** 

          When data are unbalanced selecting this option will
	  oversample the minority class to train on. 

**Variable Importance** 

          Report variable importance in the model output. 

**Force Load Balance** 

          Increase training speed on small data sets to utilize all
	  cores. 

**Shuffle Training Data** 

          When data include classes with unbalanced distributions, or
	  when data are ordered, it is possible to run the algorithm
	  on chunks of data that do not accurately reflect the shape
	  of the data as a whole, which can produce poor
	  models. Shuffling training data ensures that all prediction
	  classes are present in all chunks of data. 





 
  

	

