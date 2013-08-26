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
    DRF drf = null;
    try {
      drf = new DRF();
      drf.source = ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);
      drf.vresponse = prep.prep(drf.source);
      drf.ntrees = 5;
      drf.max_depth = 50;
      drf.mtries = -1;
      drf.seed = (1L<<32)|2;
      drf.run();

    } finally {
      UKV.remove(dest);         // Remove whole frame
      if( drf != null ) {
        drf.source.remove();
        UKV.remove(drf.vresponse._key);
        drf.remove();
      }
    }
  }
}
