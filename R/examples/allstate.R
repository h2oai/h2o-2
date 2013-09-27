# demo on allstate kaggle data
# the dataset is presplit into test.zip and train.zip; they are imported as train.hex and test.hex

# configuration you may have to set:
ip = '127.0.0.1'
port = 54321
datapath = './datasets/allstate/'

sessionInfo()

# standard preamble to connect to h2o
library( h2oWrapper )
h2oWrapper.init(ip=ip, port=port, silentUpgrade=T, startH2O=F)
library(h2o)
h2o = new('H2OClient', ip=ip, port=port)
h2o.checkClient( h2o )


# import data from a zip on disk
train.hex <- h2o.importFile( h2o, path=paste( datapath, 'train_set.zip', sep='/'), key='allstate.train.hex')
summary( train.hex )
c( nrow( train.hex), ncol( train.hex ) )

# need to convert 4 columns from int -> factor
for (col in c( 'Vehicle', 'Calendar_Year', 'Model_Year', 'OrdCat' ) ){
  train.hex <- h2o.factor( data=train.hex, col )
}
summary( train.hex )

test.hex <- h2o.importFile( h2o, path=paste( datapath, 'test_set.zip', sep='/'), key='allstate.test.hex')
summary( test.hex )
c( nrow( test.hex), ncol( test.hex ) )
for (col in c( 'Vehicle', 'Calendar_Year', 'Model_Year', 'OrdCat' ) ){
  test.hex <- h2o.factor( data=test.hex, col )
}
summary( test.hex )

# predict a tweedie model
X <- setdiff(colnames(train.hex), c('Claim_Amount', 'Household_ID', 'Row_ID'))
model0 <- h2o.glm( y = 'Claim_Amount', x=X, data=train.hex, family='tweedie', tweedie.p=1.33, nfolds=2, alpha=0.5, lambda=1e-4 )

print( model0 )

# examine interesting things
coefs <- model0@model$coefficients
head( coefs[order(abs(coefs), decreasing=T)], n=20)

preds <- h2o.predict( model0, test.hex )
c( nrow(preds), ncol(preds))
