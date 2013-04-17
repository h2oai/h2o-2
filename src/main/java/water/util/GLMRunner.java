
package water.util;

import hex.*;
import hex.DGLM.Family;
import hex.DGLM.GLMJob;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMParams;
import hex.DLSM.ADMMSolver;
import water.*;


public class GLMRunner {

  static class GLMArgs extends Arguments.Opt {
    String file;                // data
    String family = "gaussian";           //
    double _alpha = 0.5;
    double lambda = 1e-5;
    int xval = 0;
    String y;
    String xs = "all";
  }

  /**
   * Simple GLM wrapper to enable launching GLM from command line.
   *
   * Example input:
   *   java -jar target/h2o.jar  -name=test -runMethod water.util.GLMRunner   -file=smalldata/logreg/prostate.csv -y=CAPSULE -family=binomial
   *
   * @param args
   * @throws InterruptedException
   */
  public static void main(String[] args) throws InterruptedException {
    try {
      GLMArgs ARGS = new GLMArgs();
      new Arguments(args).extract(ARGS);
      System.out.println("==================<GLMRunner START>===================");
      ValueArray ary = Utils.loadAndParseKey(ARGS.file);
      int ycol;
      try{
        ycol = Integer.parseInt(ARGS.y);
      } catch (NumberFormatException e){
        ycol = ary.getColumnIds(new String [] {ARGS.y})[0];
      }
      int ncols = ary.numCols();
      if(ycol < 0 || ycol >= ary.numCols()){
        System.err.println("invalid y column: " + ycol);
        System.exit(-1);
      }
      int [] xcols;
      if(ARGS.xs.equalsIgnoreCase("all")){
         xcols = new int[ncols-1];
        for(int i = 0; i < ycol; ++i)
          xcols[i] = i;
        for(int i = ycol; i < ncols-1; ++i)
          xcols[i] = i+1;
      } else {
        System.out.println("xs = " + ARGS.xs);
        String [] names = ARGS.xs.split(",");
        xcols = new int[names.length];
        try{
          for(int i = 0; i < names.length; ++i)
            xcols[i] = Integer.valueOf(names[i]);
        } catch(NumberFormatException e){
          xcols = ary.getColumnIds(ARGS.xs.split(","));
        }
      }
      for(int x:xcols) if(x < 0){
        System.err.println("Invalid predictor specification " + ARGS.xs);
        System.exit(-1);
      }
      GLMJob j = DGLM.startGLMJob(DGLM.getData(ary, xcols,ycol, null, true), new ADMMSolver(ARGS.lambda,ARGS._alpha), new GLMParams(Family.valueOf(ARGS.family)), null, ARGS.xval, true);
      System.out.print("[GLM] computing model...");
      int progress = 0;
      while(!j.isDone()){
        int p = (int)(100*j.progress());
        int dots = p - progress;
        progress = p;
        for(int i = 0; i < dots;++i)System.out.print('.');
        Thread.sleep(250);
      }
      System.out.println("DONE.");
      GLMModel m = j.get();
      String [] colnames = ary.colNames();
      System.out.println("Intercept"  + " = " + m._beta[ncols-1]);
      for(int i = 0; i < xcols.length; ++i){
        System.out.println(colnames[i]  + " = " + m._beta[i]);
      }
    }catch(Throwable t){
      t.printStackTrace();
    } finally {  // we're done. shutdown the cloud
      System.out.println("==================<GLMRunner DONE>===================");
      UDPRebooted.suicide(UDPRebooted.T.shutdown, H2O.SELF);
    }
  }
}
