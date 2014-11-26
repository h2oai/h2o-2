package hex;

import hex.FrameTask.DataInfo;
import hex.glm.GLMModel;
import hex.gram.Gram.GramTask;
import hex.la.DMatrix;
import org.junit.Test;
import water.*;
import water.fvec.*;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.RebalanceDataSet;
import water.util.Utils;

import java.io.File;
import java.util.Arrays;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Created by tomasnykodym on 11/14/14.
 */
public class MatrixTest extends TestUtil {
  private static Frame getFrameForFile(Key outputKey, String path){
    File f = TestUtil.find_test_file(path);
    Key k = NFSFileVec.make(f);
    Frame fr = ParseDataset2.parse(outputKey, new Key[]{k});
    return fr;
  }
  @Test
  public void testTranspose(){
    Futures fs = new Futures();
    Key parsed = Key.make("prostate_parsed");
    Key modelKey = Key.make("prostate_model");
    GLMModel model = null;
    File f = TestUtil.find_test_file("smalldata/glm_test/prostate_cat_replaced.csv");
    Frame fr = getFrameForFile(parsed, "smalldata/glm_test/prostate_cat_replaced.csv");
    fr.remove("RACE").remove(fs);
    Key k = Key.make("rebalanced");
    H2O.submitTask(new RebalanceDataSet(fr, k, 64)).join();
    fr.delete();
    fr = DKV.get(k).get();
    Frame tr = DMatrix.transpose(fr);
    tr.reloadVecs();
    for(int i = 0; i < fr.numRows(); ++i)
      for(int j = 0; j < fr.numCols(); ++j)
        assertEquals(fr.vec(j).at(i),tr.vec(i).at(j),1e-4);
    fr.delete();
    for(Vec v:tr.vecs())
      v.remove(fs);
    fs.blockForPending();
//    checkLeakedKeys();
  }

  @Test // simple small & dense, compare t(X) %*% X against gram computed by glm task.
  public void testMultiplication(){
    Key parsed = Key.make("prostate_parsed");
    Futures fs = new Futures();
    Frame fr = getFrameForFile(parsed, "smalldata/glm_test/prostate_cat_replaced.csv");
    fr.remove("RACE").remove(fs);
    Key k = Key.make("rebalanced");
    H2O.submitTask(new RebalanceDataSet(fr, k, 64)).join();
    fr.delete();
    fr = DKV.get(k).get();
    Frame tr = DMatrix.transpose(fr);
    tr.reloadVecs();
    Frame z = DMatrix.mmul(tr,fr);
    DataInfo dinfo = new DataInfo(fr, 0, false, false, DataInfo.TransformType.NONE);
    GramTask gt = new GramTask(null, dinfo, false,false).doAll(dinfo._adaptedFrame);
    gt._gram.mul(gt._nobs);
    double [][] gram = gt._gram.getDenseXX();
    for(int i = 0; i < gram.length; ++i)
      for(int j = 0; j < gram[i].length; ++j)
        assertEquals("position " + i + ", " + j, gram[i][j], z.vec(j).at(i),1e-4);
    fr.delete();
    for(Vec v:tr.vecs())
      v.remove(fs);
    for(Vec v:z.vecs())
      v.remove(fs);
//    for(Vec v:z2.vecs())
//      v.remove(fs);
    fs.blockForPending();
    checkLeakedKeys();
  }

//  @Test // bigger & sparse, compare X2 <- H2 %*% M2 against R
//  public void testMultiplicationSparse() {
//    Futures fs = new Futures();
//    Key xParsed = Key.make("xParsed"), hParsed = Key.make("hParsed"), mParsed = Key.make("mParsed");
//    Frame C = getFrameForFile(xParsed, "smalldata/sparse_matrices/C.svm");
//    C.remove(0).remove(fs);
//    Frame A = getFrameForFile(hParsed, "smalldata/sparse_matrices/A.svm");
//    A.remove(0).remove(fs);
//    Frame B = getFrameForFile(mParsed, "smalldata/sparse_matrices/B.svm");
//    B.remove(0).remove(fs);
//    Frame C2 = DMatrix.mmul(A,B);
//    for(int i = 0; i < C.numRows(); ++i)
//      for(int j = 0; j < C.numCols(); ++j) // we match only up to 1e-3?
//        assertEquals("@ " + i + ", " + j + " " + C.vec(j).at(i) + " != " + C2.vec(j).at(i), C.vec(j).at(i),C2.vec(j).at(i),1e-3);
//    C.delete();
//    A.delete();
//    B.delete();
//    for(Vec v:C2.vecs())
//      v.remove(fs);
//    fs.blockForPending();
//    checkLeakedKeys();
//  }


  @Test
  public void testTransposeSparse(){
    Key parsed = Key.make("arcene_parsed");
    GLMModel model = null;
    String[] data = new String[] {
      "1 2:.2 5:.5 9:.9\n-1 1:.1 4:.4 8:.8\n",
      "1 2:.2 5:.5 9:.9\n1 3:.3 6:.6\n",
      "-1 7:.7 8:.8 9:.9\n1 20:2.\n",
      "+1 1:.1 5:.5 6:.6 10:1\n1 19:1.9\n",
      "1 2:.2 5:.5 9:.9\n-1 1:.1 4:.4 8:.8\n",
      "1 2:.2 5:.5 9:.9\n1 3:.3 6:.6\n",
      "-1 7:.7 8:.8 9:.9\n1 20:2.\n",
      "+1 1:.1 5:.5 6:.6 10:1\n1 19:1.9\n",
      "1 2:.2 5:.5 9:.9\n-1 1:.1 4:.4 8:.8\n",
      "1 2:.2 5:.5 9:.9\n1 3:.3 6:.6\n",
      "-1 7:.7 8:.8 9:.9\n1 20:2.\n",
      "+1 1:.1 5:.5 6:.6 10:1\n1 19:1.9\n"
    };
    Key k = FVecTest.makeByteVec(Key.make("svmtest_bits").toString(),data);
    Frame fr = ParseDataset2.parse(parsed, new Key[]{k});
    Frame tr = DMatrix.transpose(fr);
    tr.reloadVecs();
    for(int i = 0; i < fr.numRows(); ++i)
      for(int j = 0; j < fr.numCols(); ++j)
        assertEquals("at " + i + ", " + j + ":",fr.vec(j).at(i),tr.vec(i).at(j),1e-4);
    fr.delete();
    Futures fs = new Futures();
    for(Vec v:tr.vecs())
      v.remove(fs);
    fs.blockForPending();
//    checkLeakedKeys();
  }
}
