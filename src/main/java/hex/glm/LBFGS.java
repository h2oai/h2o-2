package hex.glm;

import hex.FrameTask.DataInfo;
import hex.glm.GLMTask.GLMIterationTask;
import water.DKV;
import water.H2O;
import water.Key;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.util.ModelUtils;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * Created by tomasnykodym on 9/2/14.
 */
public class LBFGS {

  // here we have the distributed task to go over the dataset and compute the gradient

  public static void run(Key jobKey, DataInfo dinfo,GLMParams params){
    double [] beta = null; // TODO: make initial solution here...
    // LBFGS impl goes here
    int iter = 0;
    int max_iter = 10000;
    double alpha = 0; // TODO
    double lambda = 10000; // TODO
    GLMIterationTask currentIter = null;
    while(iter < max_iter /* add stopping criterium */){
      // do distributed (expensive) part
      currentIter = new GLMIterationTask(jobKey,dinfo, params, false, true, true, beta, 0,1, ModelUtils.DEFAULT_THRESHOLDS,null).doAll(dinfo._adaptedFrame);
      // now single node single threaded...
      double [] gradient = currentIter.gradient(alpha,lambda);
      GLMValidation val = currentIter._val; // extract line search info from here (e.g. deviance..)
      //TODO: do lbfgs stuff here
    }
    // we should have solution in beta, we should make model here and store it, for now just print it? And the final deviance?
    System.out.println("beta = " + Arrays.toString(beta));
    System.out.println("val = " + currentIter._val.toString());
  }

  // to be parsed when cloud is up
  private static String [] datasets = new String[] {
    /* TODO: put your dataset here as: path, key*/"yourpath","mydata",
//    "/Users/tomasnykodym/h2o/smalldata/logreg/prostate.csv","prostate",
//    "/Users/tomasnykodym/h2o/smalldata/mnist/train.csv.gz","mtrain",
////      "/Users/tomasnykodym/mydata/arcene/arcene_train.data","train_raw",
////      "/Users/tomasnykodym/mydata/arcene/arcene_train_labels.labels","labels",
//    "/Users/tomasnykodym/mydata/coll/train.csv","dtrain",
//    "/Users/tomasnykodym/mydata/coll/test.csv","dtest"
  };

  private static String [] cmds = new String[] {
    "train2 <- cbind(train_raw,labels);","train",
    "A <- mtrain;","A",
  };

  public static void main(String [] args) throws InterruptedException, ExecutionException, Throwable {
    System.out.println("Running Parser Cmp Test with args=" + Arrays.toString(args));
    System.out.println(Arrays.equals((int[])null,(int[])null));
    final int nnodes = 3;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes, 20000);

    for(int i = 0; i < datasets.length; i += 2){
      File f = new File(datasets[i]);
      if(!f.exists()) throw new RuntimeException("did not find file " + f.getAbsolutePath());
      Key frRaw = NFSFileVec.make(f);
      Key frHex = Key.make(datasets[i+1]);
      ParseDataset2.parse(frHex, new Key[]{frRaw});
    }
    Key k = Key.make("mydata");
    Frame f = DKV.get(k).get();

    //TODO: finish these
    // f = DataInfo.prepareFrame(...)
    // DataInfo dinfo = new DataInfo(f,...);
    // run(null,dinfo,params);
    System.out.println("DONE");
  }

}
