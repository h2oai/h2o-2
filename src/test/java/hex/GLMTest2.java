package hex;

import hex.glm.*;
import hex.glm.GLMParams.Family;
import hex.glm.GLMParams.Link;

import java.io.File;

import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.fvec.*;


public class GLMTest2  extends TestUtil {
  private static void runModel(File f, String outputKey, String response, String [] ignores){
    Key k = NFSFileVec.make(f);
    Key fk = Key.make(outputKey + "_data");
    Frame fr = ParseDataset2.parse(fk, new Key[]{k});
//    if(outputKey.equals("airlines")){
//      String [] names2keep = new String[]{"DepTime","Distance","Origin","Dest","IsArrDelayed"};
//      Vec [] vecs = new Vec[names2keep.length];
//      for(int i = 0; i < names2keep.length; ++i)
//        vecs[i] = fr.remove(names2keep[i]);
//      DKV.put(Key.make("airlines_data_reduced"), new Frame(names2keep,vecs));
//    }
    for(String c:ignores)fr.remove(c);
    fr.add(response, fr.remove(response));
    new GLM2("glm", Key.make(outputKey + "_new"),fr, Family.binomial, Link.logit,0.5,1e-3).run();
  }
  public static void main(String [] args) throws Exception{
    System.out.println("Running ParserTest2");
    final int nnodes = 1;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
    System.out.println("Running...");
//    File f = new File("/Users/tomasnykodym/h2o/smalldata/logreg/prostate.csv");
    File f = new File("/Users/tomasnykodym/h2o/smalldata/airlines/allyears2k_headers.csv");
    String response = "IsArrDelayed";
    String [] ignores = new String []{"ArrTime","ActualElapsedTime","ArrDelay","DepDelay","TailNum","AirTime","TaxiIn","TaxiOut","CancellationCode","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay","IsDepDelayed"};
    runModel(f, "airlines", response, ignores);
    f = new File("/Users/tomasnykodym/h2o/smalldata/logreg/prostate.csv");
    ignores = new String[]{};
    response = "CAPSULE";
    runModel(f, "prostate", response, ignores);
    f = new File("/Users/tomasnykodym/Downloads/140k_train_anonymised.csv");
    ignores = new String[]{"Choicepoint_Cx","Choicepoint_pass","Has_bnk_AC"};
    response = "Converted";
    runModel(f, "rushcard", response, ignores);
    System.out.println("DONE!");
  }
}
