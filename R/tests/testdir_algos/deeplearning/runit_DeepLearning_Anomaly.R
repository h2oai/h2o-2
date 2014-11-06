setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

check.deeplearning_anomaly <- function(conn) {
  Log.info("Deep Learning Anomaly Detection MNIST)")
  
  TRAIN = "smalldata/mnist/train.csv.gz"
  TEST = "smalldata/mnist/test.csv.gz"
  
  # set to FALSE for stand-alone demo
  if (TRUE) {
    train_hex = h2o.uploadFile(conn, locate(TRAIN))
    test_hex = h2o.uploadFile(conn, locate(TEST))
  } else {
    library(h2o)
    conn=h2o.init()
    homedir=paste0(path.expand("~"),"/h2o/") #modify if needed
    train_hex = h2o.importFile(conn, path = paste0(homedir,TRAIN), header = F, sep = ',', key = 'train.hex')
    test_hex = h2o.importFile(conn, path = paste0(homedir,TEST), header = F, sep = ',', key = 'test.hex')
  }
  
  predictors = c(1:784)
  resp = 785
  
  # unsupervised -> drop the response column (digit: 0-9)
  train_hex <- train_hex[,-resp]
  test_hex <- test_hex[,-resp]
  
  # helper function for display of handwritten digits
  # adapted from http://www.r-bloggers.com/the-essence-of-a-handwritten-digit/
  plotDigit <- function(mydata, rec_error) {
    len <- nrow(mydata)
    N <- ceiling(sqrt(len))
    par(mfrow=c(N,N),pty='s',mar=c(1,1,1,1),xaxt='n',yaxt='n')
    for (i in 1:nrow(mydata)) {
      colors<-c('white','black')
      cus_col<-colorRampPalette(colors=colors)
      z<-array(mydata[i,],dim=c(28,28))
      z<-z[,28:1]
      image(1:28,1:28,z,main=paste0("rec_error: ", round(rec_error[i],4)),col=cus_col(256))
    }
  }
  
  
  ## START ANOMALY DETECTION DEMO
  
  # 1) LEARN WHAT'S NORMAL
  # train unsupervised Deep Learning autoencoder model on train_hex
  ae_model <- h2o.deeplearning(x=predictors,
                               y=42, #response is ignored (pick any non-constant predictor column index)
                               train_hex,
                               activation="Tanh",
                               autoencoder=T,
                               hidden=c(50),
                               l1=1e-5,
                               ignore_const_cols=F,
                               epochs=1)
  
  
  # 2) DETECT OUTLIERS
  # anomaly app computes the per-row reconstruction error for the test data set
  # (passing it through the autoencoder model and computing mean square error (MSE) for each row)
  rec_error <- as.data.frame(h2o.anomaly(test_hex, ae_model))
  
  
  # 3) VISUALIZE OUTLIERS
  # Show the data points with low/medium/high reconstruction error
  
  # Convert the test data into its autoencoded representation (pass through narrow neural net)
  test_recon <- h2o.predict(ae_model, test_hex)
  
  #LOWEST RECONSTRUCTION ERROR - LEAST OUTLIER-like
  #row indices and reconstruction errors for easiest to reconstruct data points
  easy_row_idx <- order(rec_error[,1],decreasing=F)[1:25]
  easy_rec_error <- rec_error[easy_row_idx,]
  test_rec_easy <- as.matrix(as.data.frame(test_recon[easy_row_idx,]))
  test_orig_easy <- as.matrix(as.data.frame(test_hex[easy_row_idx,]))
  plotDigit(test_rec_easy,  easy_rec_error) #Reconstructed data points - LEAST OUTLIER-like
  plotDigit(test_orig_easy, easy_rec_error) #Original data points - LEAST OUTLIER-like
  
  #MEDIAN RECONSTRUCTION ERROR
  #row indices and reconstruction errors for averge to reconstruct data points
  median_row_idx <- tail(head(order(rec_error[,1],decreasing=T),5013),25)[1:25]
  median_rec_error <- rec_error[median_row_idx,]
  test_rec_median <- as.matrix(as.data.frame(test_recon[median_row_idx,]))
  test_orig_median <- as.matrix(as.data.frame(test_hex[median_row_idx,]))
  plotDigit(test_rec_median,  median_rec_error) #Reconstructed data points) - MEDIAN
  plotDigit(test_orig_median, median_rec_error) #Original data points - MEDIAN
  
  #LARGEST RECONSTRUCTION ERROR - OUTLIERS
  #row indices and reconstruction errors for hardest to reconstruct data points
  hardest_row_idx <- order(rec_error[,1],decreasing=T)[1:25]
  hardest_rec_error <- rec_error[hardest_row_idx,]
  test_rec_worst <- as.matrix(as.data.frame(test_recon[hardest_row_idx,]))
  test_orig_worst <- as.matrix(as.data.frame(test_hex[hardest_row_idx,]))
  plotDigit(test_rec_worst,  hardest_rec_error) #Reconstructed data points - OUTLIERS
  plotDigit(test_orig_worst, hardest_rec_error) #Original data points - OUTLIERS
  
  testEnd()
}

doTest("Deep Learning Anomaly Detection MNIST", check.deeplearning_anomaly)

