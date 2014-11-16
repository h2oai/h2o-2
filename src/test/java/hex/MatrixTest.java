package hex;

import hex.glm.GLMModel;
import hex.la.DMatrix;
import org.junit.Test;
import water.*;
import water.fvec.*;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.RebalanceDataSet;

import java.io.File;

import static junit.framework.Assert.assertEquals;

/**
 * Created by tomasnykodym on 11/14/14.
 */
public class MatrixTest extends TestUtil {
  private static Frame getFrameForFile(Key outputKey, String path,String [] ignores, String response){
    File f = TestUtil.find_test_file(path);
    Key k = NFSFileVec.make(f);
    Frame fr = ParseDataset2.parse(outputKey, new Key[]{k});
    if(ignores != null)
      for(String s:ignores) UKV.remove(fr.remove(s)._key);
    // put the response to the end
    fr.add(response, fr.remove(response));
    return fr;
  }
  @Test
  public void testTranspose(){
    Key parsed = Key.make("prostate_parsed");
    Key modelKey = Key.make("prostate_model");
    GLMModel model = null;
    File f = TestUtil.find_test_file("smalldata/glm_test/prostate_cat_replaced.csv");
    Frame fr = getFrameForFile(parsed, "smalldata/glm_test/prostate_cat_replaced.csv", new String[]{"ID"}, "CAPSULE");
    fr.remove("RACE");
    Key k = Key.make("rebalanced");
    H2O.submitTask(new RebalanceDataSet(fr, k, 64)).join();
    fr.delete();
    fr = DKV.get(k).get();
    Frame tr = DMatrix.transpose(fr);
    tr.reloadVecs();
    Key kk = Key.make("transposed");
    DKV.put(kk,new Frame(kk,tr.names(),tr.vecs()));
    for(int i = 0; i < fr.numRows(); ++i)
      for(int j = 0; j < fr.numCols(); ++j)
        assertEquals(fr.vec(j).at(i),tr.vec(i).at(j),1e-4);
    fr.delete();
    Futures fs = new Futures();
    for(Vec v:tr.vecs())
      v.remove(fs);
  }

  @Test
  public void testTranspose2(){
    Key parsed = Key.make("prostate_parsed");
    GLMModel model = null;
    File f = TestUtil.find_test_file("smalldata/arcene/arcene_train.data");
    Key k = NFSFileVec.make(f);
    Frame fr = ParseDataset2.parse(parsed, new Key[]{k});
    k = Key.make("rebalanced");
    H2O.submitTask(new RebalanceDataSet(fr, k, 64)).join();
    fr.delete();
    fr = DKV.get(k).get();
    Frame tr = DMatrix.transpose(fr);
    tr.reloadVecs();
//    Key kk = Key.make("transposed");
//    DKV.put(kk,new Frame(kk,tr.names(),tr.vecs()));
    for(int i = 0; i < fr.numRows(); ++i)
      for(int j = 0; j < fr.numCols(); ++j)
        assertEquals("at " + i + ", " + j + ":",fr.vec(j).at(i),tr.vec(i).at(j),1e-4);
    fr.delete();
    Futures fs = new Futures();
    for(Vec v:tr.vecs())
      v.remove(fs);
    fs.blockForPending();
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

    k = FVecTest.makeByteVec(Key.make("svmtest_bits").toString(),data);
    fr = ParseDataset2.parse(parsed, new Key[]{k});
    tr = DMatrix.transpose(fr);
    tr.reloadVecs();
    for(int i = 0; i < fr.numRows(); ++i)
      for(int j = 0; j < fr.numCols(); ++j)
        assertEquals("at " + i + ", " + j + ":",fr.vec(j).at(i),tr.vec(i).at(j),1e-4);
    fr.delete();
    fs = new Futures();
    for(Vec v:tr.vecs())
      v.remove(fs);
    fs.blockForPending();
  }
}
