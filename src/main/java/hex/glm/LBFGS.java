package hex.glm;

import hex.FrameTask.DataInfo;
import hex.glm.GLMTask.GLMIterationTask;
import water.DKV;
import water.H2O;
import water.Key;
import water.api.Request.API;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;
import water.util.ModelUtils;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Created by tomasnykodym on 9/2/14.
 */
public class LBFGS {

  @API(help="The number of steps (pair of s and y) we remember in L-BFGS")
  public static int m = 20;

  @API(help="s stores the steps: s[i] = x[i+1] - x[i] where x are the variable to be optimized, in our case, beta.")
//  public List<Vec> s = new LinkedList<Vec>();
  public static List<double[]> s = new LinkedList<double[]>();

  @API(help="y stores the difference of the gradient: y[i] = g[i+1] - g[i]")
//  public List<Vec> y = new LinkedList<Vec>();
  public static List<double[]> y = new LinkedList<double[]>();

  @API(help="Max iterations regardless of the stopping criteria")
  public static int max_iter = 10000;

  // here we have the distributed task to go over the dataset and compute the gradient

  public static void run(Key jobKey, DataInfo dinfo, GLMParams params){
    long M = dinfo._adaptedFrame.numRows();
    // generate initial guess
    int N = dinfo.fullN()+1;
    double [] beta = new double[N];
    Random randomGenerator = new Random();
    for (int i = 0; i < N; i++) {
      beta[i] = randomGenerator.nextGaussian();
    }

    // LBFGS impl goes here
    int iter = 0;
    double alpha = 0; // TODO
    double lambda = 10;
    GLMIterationTask currentIter = null;
    int k = 0;
    double[] x_old = beta;
    double[] x_cur = beta;
    // calculate the gradient
    currentIter = new GLMIterationTask(jobKey,dinfo, params, false, true, true, x_old, 0,1, ModelUtils.DEFAULT_THRESHOLDS,null).doAll(dinfo._adaptedFrame);
    double [] g_old = currentIter.gradient(alpha,lambda);
    double [] g_cur = g_old;
    Log.info("Before Iteration:  "+k+"  val=  "+currentIter._val.toString());


    while(k < max_iter /* add stopping criterium */){

      double [] pk = getSearchDirection(k, g_old);
      double step = lineSearch();
      x_cur = MatrixUtils.add(x_old, MatrixUtils.scalarProduct(step, pk));
      // update s
      if (k>m-1) { s.remove(0); } s.add(MatrixUtils.minus(x_cur, x_old));
      // update gradient and y
      currentIter = new GLMIterationTask(jobKey,dinfo, params, false, true, true, x_cur, 0,1, ModelUtils.DEFAULT_THRESHOLDS,null).doAll(dinfo._adaptedFrame);
      GLMValidation val = currentIter._val; // extract line search info from here (e.g. deviance..)
      double obj = val.residualDeviance();
      g_cur = currentIter.gradient(alpha,lambda);
      if (k>m-1) { y.remove(0); } y.add(MatrixUtils.minus(g_cur, g_old));
      Log.info("After Iteration:  "+k+"  val=  "+currentIter._val.toString());
      x_old = x_cur;
      g_old = g_cur;
      k++;
    }
    // we should have solution in beta, we should make model here and store it, for now just print it? And the final deviance?
    System.out.println("beta = " + Arrays.toString(beta));
    System.out.println("val = " + currentIter._val.toString());
  }

  public static double[] getSearchDirection(int k, double[] q) {
    double[] alpha = new double[m]; // alpha[i] is actually alpha[-i+k]
    for (int i = m-1 ; i >= 0; i--) {
      if (k-m+i >= 0 && i<k) {
        alpha[i] = rho(i) * MatrixUtils.innerProduct(s.get(i), q);
        q = MatrixUtils.add(q, MatrixUtils.scalarProduct(-1 * alpha[i], y.get(i)));
      }
    }
    double[] r = MatrixUtils.Hk0q(k, q, s, y);
    for (int i = 0; i < m; i++) {
      if (k-m+i >= 0 && i<k) {
        double b = rho(i) * MatrixUtils.innerProduct(y.get(i),r);
        r = MatrixUtils.add(r, MatrixUtils.scalarProduct(b-alpha[i], s.get(i)));
      }
    }
    return MatrixUtils.scalarProduct(-1, r);
  }

  public static double lineSearch() {
    double stepSize = 1;

    return stepSize;
  }

  public static double rho(int i) {
    int m = LBFGS.m;
    assert 0<=i && m>i: "Index out of range getting rho";
    return 1.0 / MatrixUtils.innerProduct(y.get(i), s.get(i));
  }



  // to be parsed when cloud is up
  private static String [] datasets = new String[] {
    /* TODO: put your dataset here as: path, key*/"" +
          "" +
          "smalldata/airlines/AirlinesTrain.csv.zip","airline",
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
    Key k = Key.make("airline");
    Frame f = DKV.get(k).get();

    int [] ignored_cols = null;
    boolean toEnum = false;
    boolean dropConstantCols = false;
    Vec response = f.vec("IsDepDelayed_REC");
    int nResponses = 1;
    boolean useAllFactors = true;
    GLMParams params = new GLMParams(GLMParams.Family.gaussian);

    f = DataInfo.prepareFrame(f, response, ignored_cols, toEnum, dropConstantCols);
    DataInfo dinfo = new DataInfo(f, nResponses, useAllFactors, DataInfo.TransformType.NORMALIZE);
    run(null, dinfo, params);
    System.out.println("DONE");
  }

}
