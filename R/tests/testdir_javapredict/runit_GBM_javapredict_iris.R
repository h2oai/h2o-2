# setwd("/Users/tomk/0xdata/ws/h2o/R/tests/testdir_javapredict")

TEST_ROOT_DIR <- ".."
source(sprintf("%s/%s", TEST_ROOT_DIR, "findNSourceUtils.R"))

heading("BEGIN TEST")
conn <- new("H2OClient", ip=myIP, port=myPort)
n.trees <- 100 
interaction.depth <- 5
n.minobsinnode <- 10
shrinkage <- 0.1
x = c("sepal_len","sepal_wid","petal_len","petal_wid");
y = "species"

heading("Uploading train data to H2O")
iris_train.hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris_train.csv"))

heading("Creating GBM model in H2O")
iris.gbm.h2o <- h2o.gbm(x = x, y = y, data = iris_train.hex, n.trees = n.trees, interaction.depth = interaction.depth, n.minobsinnode = n.minobsinnode, shrinkage = shrinkage)
print(iris.gbm.h2o)

heading("Downloading Java prediction model code from H2O")
model_key <- iris.gbm.h2o@key
tmpdir_name <- sprintf("tmp_model_%s", as.character(Sys.getpid()))
cmd <- sprintf("rm -fr %s", tmpdir_name)
safeSystem(cmd)
cmd <- sprintf("mkdir %s", tmpdir_name)
safeSystem(cmd)
cmd <- sprintf("curl -o %s/%s.java http://%s:%d/2/GBMModelView.java?_modelKey=%s", tmpdir_name, model_key, myIP, myPort, model_key)
safeSystem(cmd)

heading("Uploading test data to H2O")
iris_test.hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris_test.csv"))

heading("Predicting in H2O")
iris.gbm.pred <- h2o.predict(iris.gbm.h2o, iris_test.hex)
summary(iris.gbm.pred)
head(iris.gbm.pred)
p1 <- as.data.frame(iris.gbm.pred)

heading("Setting up for Java POJO")
iris_test_with_response <- read.csv(locate("smalldata/iris/iris_test.csv"), header=T)
iris_test_without_response <- subset(iris_test_with_response, select=-c(species))
write.csv(iris_test_without_response, file = sprintf("%s/in.csv", tmpdir_name), row.names=F)
cmd <- sprintf("cp main.java %s", tmpdir_name)
safeSystem(cmd)
cmd <- sprintf("javac -cp %s/h2o-model.jar -J-Xmx2g -J-XX:MaxPermSize=128m %s/main.java %s/%s.java", H2O_JAR_DIR, tmpdir_name, tmpdir_name, model_key)
safeSystem(cmd)

heading("Predicting with Java POJO")
cmd <- sprintf("java -cp %s/h2o-model.jar:%s -Xmx2g -XX:MaxPermSize=256m main --header --model %s --input %s/in.csv --output %s/out.csv", H2O_JAR_DIR, tmpdir_name, model_key, tmpdir_name, tmpdir_name)
safeSystem(cmd)

heading("Comparing predictions between H2O and Java POJO")

heading("Cleaning up tmp files")
cmd <- sprintf("rm -fr %s", tmpdir_name)
safeSystem(cmd)

PASS_BANNER()
