#Section 1.1: Installation
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Set the CRAN mirror
local({r <- getOption("repos"); r["CRAN"] <- "http://cran.us.r-project.org"; options(repos = r)})

# The following two commands remove any previously installed H2O packages for R.
if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

# Next, we download, install and initialize the H2O package for R. Do not copy and paste the installation link below. 
# Instead, go to http://0xdata.com/downloadtable/ and click on "Bleeding Edge Developer Build". The following link
# should follow the template "http://s3.amazonaws.com/h2o-release/h2o/master/****/index.html" with the 4 *'s filled
# in with digits. Replace the 4 *'s in the following command with these 4 digits before executing.

version = "****" 

### Opportunity to manually set the version (must be at least 1444)
#version = "1444"

# Fall-back: Automatically get the latest version
if (version == "****") {
    library(RCurl)
    if(url.exists("http://s3.amazonaws.com/h2o-release/h2o/master/latest")) {
        h = basicTextGatherer()
        curlPerform(url = "http://s3.amazonaws.com/h2o-release/h2o/master/latest", writefunction = h$update)
        version = h$value()
        version = gsub("\n","",version)
    }
}

install.packages("h2o", repos=(c(paste(paste("http://s3.amazonaws.com/h2o-release/h2o/master/",version,sep=""),"/R",sep=""), getOption("repos"))))

# load the library
library(h2o)

# start H2O locally
h2o_server = h2o.init()

#Check out the built-in h2o.deeplearning demo
demo(h2o.deeplearning)

#If not done by the user, we can unpack the data.zip file here
#unzip("data.zip")

#Section 2.4: Loading Data
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

weather.hex = h2o.uploadFile(h2o_server, path = "weather.csv", header = TRUE, sep = ",", key = "weather.hex")

#Alternatively (required for HDFS or S3 or URLs), use importFile:
#weather.hex = h2o.importFile(h2o_server, path = "<full-path-to>/weather.csv", header = TRUE, sep = ",", key = "weather.hex")

# Display a summary
summary(weather.hex)


#Section 3: MNIST use case for digit classification
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

train_images.hex = h2o.uploadFile(h2o_server, path = "mnist_train.csv", header = FALSE, sep = ",", key = "train_images.hex")
test_images.hex = h2o.uploadFile(h2o_server, path = "mnist_test.csv", header = FALSE, sep = ",", key = "test_images.hex")

#Train the model for digit classification
mnist_model = h2o.deeplearning(x = 1:784, y = 785, data = train_images.hex, activation = "RectifierWithDropout",
hidden = c(200,200,200), input_dropout_ratio = 0.2, l1 = 1e-5, validation = test_images.hex, epochs = 10, variable_importances = TRUE)

#View the specified parameters of your deep learning model
mnist_model@model$params

#Examine the performance of the trained model
mnist_model

#Access variable importances (note: here, variables are pixel indices)
vi = mnist_model@model$varimp
head(vi)

#Perform classification on the test set
prediction = h2o.predict(mnist_model, newdata=test_images.hex)

#Copy predictions from H2O to R (for illustration only, not needed here)
pred = as.data.frame(prediction)

#Get the confusion matrix on the validation set 
mnist_model@model$confusion

#Create a set of network topologies
hidden_layers = list(c(200,200), c(100,300,100),c(500,500,500))

#Run the grid search
mnist_model_grid = h2o.deeplearning(x=1:784, y=785, data=train_images.hex, activation="RectifierWithDropout", 
hidden=hidden_layers, validation=test_images.hex, epochs=1, l1=c(1e-5,1e-7), input_dropout_ratio=0.2)

#Print out all prediction errors and run times of the models
mnist_model_grid
mnist_model_grid@model

#Print out a *short* summary of each of the models (indexed by parameter)
mnist_model_grid@sumtable

#Print out *full* summary of each of the models
all_params = lapply(mnist_model_grid@model, function(x) { x@model$params })
all_params

#Access a particular parameter across all models
l1_params = lapply(mnist_model_grid@model, function(x) { x@model$params$l1 })
l1_params

#Continue the model with the lowest test set error, train for 9 more epochs for illustration
mnist_checkpoint_model = h2o.deeplearning(x=1:784, y=785, data=train_images.hex, checkpoint=mnist_model_grid@model[[1]], validation = test_images.hex, epochs=9)

#Specify a model and the filename where it is to be saved
h2o.saveModel(object = mnist_model_grid@model[[1]], name = "/tmp/mymodel", force = TRUE)

#Alternatively, save the model under its key in some directory (here we use /tmp)
#h2o.saveModel(object = mnist_model_grid@model[[1]], dir = "/tmp", force = TRUE))

#Later, load the saved model by indicating the host and saved model filename
best_mnist_grid.load = h2o.loadModel(h2o_server, "/tmp/mymodel")

#Continue training the loaded model
best_mnist_grid.continue = h2o.deeplearning(x=1:784, y=785, data=train_images.hex, checkpoint=best_mnist_grid.load, validation = test_images.hex, epochs=1)

#This model should result in a test set error of 0.9% or better - runs for several hours
#super_model = h2o.deeplearning(x=1:784, y=785, data=train_images.hex, activation="RectifierWithDropout",
#hidden=c(1024,1024,2048), validation=test_images.hex, epochs=2000, l1=1e-5, input_dropout_ratio=0.2, 
#rho=0.99, epsilon=1e-8, max_w2=15, classification_stop=-1, train_samples_per_iteration=-1)


#Section 4: Deep autoencoders used for anomaly detection
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

train_ecg.hex = h2o.uploadFile(h2o_server, path="ecg_train.csv", header=F, sep=",", key="train_ecg.hex") 
test_ecg.hex = h2o.uploadFile(h2o_server, path="ecg_test.csv", header=F, sep=",", key="test_ecg.hex") 

#Train deep autoencoder learning model on "normal" training data, y ignored 
anomaly_model = h2o.deeplearning(x=1:210, y=1, train_ecg.hex, activation = "Tanh", 
classification=F, autoencoder=T, hidden = c(50,20,50), l1=1E-4, epochs=100)                 

#Compute reconstruction error with the Anomaly detection app (MSE between output layer and input layer)
recon_error.hex = h2o.anomaly(test_ecg.hex, anomaly_model)

#Pull reconstruction error data into R and plot to find outliers (last 3 heartbeats)
recon_error = as.data.frame(recon_error.hex)
recon_error
plot.ts(recon_error)

#Note: Testing = Reconstructing the test dataset
test_recon.hex = h2o.predict(anomaly_model, test_ecg.hex) 
head(test_recon.hex)
