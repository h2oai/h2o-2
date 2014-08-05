#demo of h2o's deeplearning algorithm from R
library(h2o)

# Start 1-node H2O cluster on localhost
#?h2o.init
h2o_server = h2o.init()

# Import data into H2O cluster and run Summary
train = h2o.importFile(h2o_server, 
                            path = 'https://raw.githubusercontent.com/0xdata/h2o/master/smalldata/logreg/prostate.csv',  ## can be a local path to file
                            header = T,
                            sep = ',', 
                            key = 'prostate.hex')
summary(train)

# Train a DeepLearning model on the H2O cluster using 3 hidden layers with 10 neurons each, Tanh activation function, 10000 epochs, predict CAPSULE from other predictors (ignore column 1: ID)
#?h2o.deeplearning
model = h2o.deeplearning(x = 3:8, y = 2, 
                              data = train, 
                              activation = "Tanh", 
                              hidden = c(10, 10, 10), 
                              epochs = 10000)
model

# Make prediction with trained model (on training data for simplicity), prediction is stored in H2O cluster
prediction = h2o.predict(model, newdata = train)

# Download prediction from H2O cluster into R environment
pred = as.data.frame(prediction)
head(pred)
tail(pred)

# Check performance of binary classification model and return the probability threshold ("Best Cutoff") for optimal F1 score
#?h2o.performance
per = h2o.performance(prediction[,3], train[,2], measure = "F1")
per

######## END
