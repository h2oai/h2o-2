package hex.gbm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.File;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.fvec.*;

public class GBMTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  private abstract class PrepData { abstract Vec prep(Frame fr); }

  @Test public void testBasicGBM() {
    basicGBM("./smalldata/test/test_tree.csv","tree.hex",
             new PrepData() { Vec prep(Frame fr) { return fr.remove(1); } 
             });
    basicGBM("./smalldata/logreg/prostate.csv","prostate.hex",
             new PrepData() {
               Vec prep(Frame fr) { 
                 assertEquals(380,fr.numRows());
                 // Remove patient ID vector
                 UKV.remove(fr.remove("ID")._key); 
                 // Prostate: predict on CAPSULE
                 return fr.remove("CAPSULE");
               }
             });
    basicGBM("../datasets/UCI/UCI-large/covtype/covtype.data","covtype.hex",
             new PrepData() {
               Vec prep(Frame fr) { 
                 assertEquals(581012,fr.numRows());
                 // Covtype: predict on last column
                 return fr.remove(54);
               }
             });
  }

  // ==========================================================================
  public void basicGBM(String fname, String hexname, PrepData prep) {
    File file = TestUtil.find_test_file(fname);
    if( file == null ) return;  // Silently abort test if the file is missing
    Key fkey = NFSFileVec.make(file);
    Key dest = Key.make(hexname);
    Frame fr = ParseDataset2.parse(dest,new Key[]{fkey});
    UKV.remove(fkey);
    Vec vresponse = null;
    GBM gbm = null;
    try {
      vresponse = prep.prep(fr);
      gbm = GBM.start(GBM.makeKey(),fr,vresponse,5);
      gbm.get();                  // Block for result
    } finally {
      UKV.remove(dest);         // Remove whole frame
      UKV.remove(vresponse._key);
      if( gbm != null ) gbm.remove();
    }
  }

  @Test public void testBasicDRF() {
    basicDRF("./smalldata/test/test_tree.csv","tree.hex",
             new PrepData() { Vec prep(Frame fr) { return fr.remove(1); } 
             });
    basicDRF("./smalldata/logreg/prostate.csv","prostate.hex",
             new PrepData() {
               Vec prep(Frame fr) { 
                 assertEquals(380,fr.numRows());
                 // Remove patient ID vector
                 UKV.remove(fr.remove("ID")._key); 
                 // Prostate: predict on CAPSULE
                 return fr.remove("CAPSULE");
               }
             });
    basicDRF("../datasets/UCI/UCI-large/covtype/covtype.data","covtype.hex",
             new PrepData() {
               Vec prep(Frame fr) { 
                 assertEquals(581012,fr.numRows());
                 // Covtype: predict on last column
                 return fr.remove(54);
               }
             });
  }

  public void basicDRF(String fname, String hexname, PrepData prep) {
    File file = TestUtil.find_test_file(fname);
    if( file == null ) return;  // Silently abort test if the file is missing
    Key fkey = NFSFileVec.make(file);
    Key dest = Key.make(hexname);
    Frame fr = ParseDataset2.parse(dest,new Key[]{fkey});
    UKV.remove(fkey);
    Vec vresponse = null;
    DRF drf = null;
    try {
      vresponse = prep.prep(fr);
      int mtrys = Math.max((int)Math.sqrt(fr.numCols()),1);
      long seed = (1L<<32)|2;

      drf = DRF.start(DRF.makeKey(),fr,vresponse,/*maxdepth*/50,/*ntrees*/5,mtrys,/*sampleRate*/0.67,seed);
      drf.get();                  // Block for result
    } finally {
      UKV.remove(dest);         // Remove whole frame
      UKV.remove(vresponse._key);
      if( drf != null ) drf.remove();
    }
  }

  /*@Test*/ public void testCovtypeDRF() {
    File file = TestUtil.find_test_file("../datasets/UCI/UCI-large/covtype/covtype.data");
    if( file == null ) return;  // Silently abort test if the large covtype is missing
    Key fkey = NFSFileVec.make(file);
    Key dest = Key.make("cov1.hex");
    Frame fr = ParseDataset2.parse(dest,new Key[]{fkey});
    UKV.remove(fkey);
    System.out.println("Parsed into "+fr);
    for( int i=0; i<fr._vecs.length; i++ )
      System.out.println("Vec "+i+" = "+fr._vecs[i]);

    try {
      assertEquals(581012,fr._vecs[0].length());

      // Confirm that on a multi-JVM test setup, covtype gets spread around
      Vec c0 = fr.firstReadable();
      int N = c0.nChunks();
      H2ONode h2o =  c0.chunkKey(0).home_node(); // A chunkkey home
      boolean found=false;        // Found another chunkkey home?
      for( int j=1; j<N; j++ )    // All the chunks
        if( h2o != c0.chunkKey(j).home_node() ) found = true;
      assertTrue("Expecting to find distribution",found || H2O.CLOUD.size()==1);

      // Covtype: predict on last column
      Vec vy = fr.remove(54);
      int mtrys = Math.max((int)Math.sqrt(fr.numCols()),1);
      long seed = (1L<<32)|2;

      DRF drf = DRF.start(DRF.makeKey(),fr,vy,/*maxdepth*/50,/*ntrees*/5,mtrys,/*sampleRate*/0.67,seed);
      drf.get();                  // Block for result
      UKV.remove(drf.destination_key);
    } finally {
      UKV.remove(dest);
    }
  }
}
