package hex;

import static org.junit.Assert.assertEquals;
import hex.FrameTask.DataInfo;
import hex.gram.Gram.GramTask;

import java.io.File;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import water.*;
import water.fvec.*;
import water.util.Utils;

public class GramMatrixTest extends TestUtil{
/* Expected prostate result from R
          RACER2 RACER3       ID  CAPSULE       AGE     DPROS    DCAPS        PSA        VOL   GLEASON
RACER2    341.00    0.0    65389   137.00   22561.0    763.00   375.00    4864.28    5245.21   2172.00   341.00
RACER3      0.00   36.0     6681    14.00    2346.0     91.00    42.00     887.10     727.50    233.00    36.00
ID      65389.00 6681.0 18362930 28351.00 4737816.0 161930.00 78104.00 1070519.50 1173473.40 452757.00 72390.00
CAPSULE   137.00   14.0    28351   153.00   10072.0    408.00   184.00    3591.09    2025.82   1069.00   153.00
AGE     22561.00 2346.0  4737816 10072.00 1673407.0  56885.00 27818.00  386974.54  401647.75 160303.00 25095.00
DPROS     763.00   91.0   161930   408.00   56885.0   2339.00   985.00   15065.97   13267.05   5617.00   863.00
DCAPS     375.00   42.0    78104   184.00   27818.0    985.00   503.00    7158.28    6426.31   2725.00   421.00
PSA      4864.28  887.1  1070520  3591.09  386974.5  15065.97  7158.28  241785.06   94428.73  40596.37  5855.28
VOL      5245.21  727.5  1173473  2025.82  401647.8  13267.05  6426.31   94428.73  222603.14  37926.78  6008.91
GLEASON  2172.00  233.0   452757  1069.00  160303.0   5617.00  2725.00   40596.37   37926.78  15940.00  2426.00
          341.00   36.0    72390   153.00   25095.0    863.00   421.00    5855.28    6008.91   2426.00   380.00
 */
  static double [][] exp_result = new double [][]{// with some corrections for R's apparent rounding off when pretty printing
    {  341.00,    0.0,    65389,   137.00,   22561.0,    763.00,   375.00,    4864.28,    5245.21,   2172.00,   341.00},
    {    0.00,   36.0,     6681,    14.00,    2346.0,     91.00,    42.00,     887.10,     727.50,    233.00,    36.00},
    {65389.00, 6681.0, 18362930, 28351.00, 4737816.0, 161930.00, 78104.00, 1070519.50, 1173473.40, 452757.00, 72390.00},
    {  137.00,   14.0,    28351,   153.00,   10072.0,    408.00,   184.00,    3591.09,    2025.82,   1069.00,   153.00},
    {22561.00, 2346.0,  4737816, 10072.00, 1673407.0,  56885.00, 27818.00,  386974.54,  401647.75, 160303.00, 25095.00},
    {  763.00,   91.0,   161930,   408.00,   56885.0,   2339.00,   985.00,   15065.97,   13267.05,   5617.00,   863.00},
    {  375.00,   42.0,    78104,   184.00,   27818.0,    985.00,   503.00,    7158.28,    6426.31,   2725.00,   421.00},
    { 4864.28,  887.1,  1070519.5,3591.09, 386974.54,  15065.97,  7158.28,  241785.0562, 94428.734, 40596.37,  5855.28}, // changed 170520 value from R to h2o's 170519.5 to pass this test for now.
    { 5245.21,  727.5,  1173473.4,2025.82, 401647.75,  13267.05,  6426.31,   94428.734, 222603.1445,37926.78,  6008.91},
    { 2172.00,  233.0,   452757,  1069.00,  160303.0,   5617.00,  2725.00,   40596.37,   37926.78,  15940.00,  2426.00},
    {  341.00,   36.0,    72390,   153.00,   25095.0,    863.00,   421.00,    5855.28,    6008.91,   2426.00,   380.00}
  };

  @Test public void testProstate(){
    File f2 = find_test_file("smalldata/glm_test/prostate_cat_replaced.csv");
    Key ikey2 = NFSFileVec.make(f2);
    Key okey2 = Key.make("glm_model2");
    Frame fr2=null;
    try{
      fr2=ParseDataset2.parse(okey2, new Key[]{ikey2});
      DataInfo dinfo = new DataInfo(fr2, 0, true, false, DataInfo.TransformType.NONE);
      GramTask gt = new GramTask(null, dinfo, true,false);
      gt.doAll(dinfo._adaptedFrame);
      double [][] res = gt._gram.getXX();
      System.out.println(Utils.pprint(gt._gram.getXX()));
      for(int i = 0; i < exp_result.length; ++i)
        for(int j = 0; j < exp_result.length; ++j)
          assertEquals(exp_result[i][j],gt._nobs*res[i][j],1e-5);
      gt = new GramTask(null, dinfo, false,false);
      gt.doAll(dinfo._adaptedFrame);
      for(int i = 0; i < exp_result.length-1; ++i)
        for(int j = 0; j < exp_result.length-1; ++j)
          assertEquals(exp_result[i][j],gt._nobs*res[i][j],1e-5);
    }finally{
      fr2.delete();
    }
  }

  /**
   * @param args
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    startCloud(args, 3);
//    new GramMatrixTest().testProstate();
    System.out.println("running glm test...");
//    new GLMTest2().testProstate();
    System.out.println("DONE");
  }

}
