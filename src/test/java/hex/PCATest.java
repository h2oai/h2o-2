package hex;

import com.google.gson.*;

import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;

import hex.DPCA.*;
import hex.NewRowVecTask.DataFrame;

public class PCATest extends TestUtil {
  public static void main(String [] args) throws Exception{
    System.out.println("Running PCATest");
    final int nnodes = 1;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
    System.out.println("Cloud formed");

    Key ksrc = loadAndParseFile("prostate.hex", "smalldata/logreg/prostate.csv");
    ValueArray va = DKV.get(ksrc).get();
    int[] cols = new int[va._cols.length];
    for( int i = 0; i < cols.length; i++ ) cols[i] = i;
    DataFrame df = DGLM.getData(va, cols, null, true);

    // int num_pc = 3;
    double tol = 0.25;
    Key kdst = Key.make("prostate.pca");

    // PCAJob job = DPCA.startPCAJob(kdst, df, new PCAParams(num_pc));
    PCAParams params = new PCAParams(tol);
    JsonObject resPCA = DPCA.buildModel(null, kdst, df, params).toJson();
    System.out.println(resPCA.toString());

    UKV.remove(ksrc);
    System.out.println("DONE!");
  }
}