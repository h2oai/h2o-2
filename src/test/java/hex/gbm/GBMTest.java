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
    //basicDRF("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { Vec prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("economy (mpg)"); } 
    //         });
    //basicGBM("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { Vec prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("economy (mpg)"); } 
    //         });
    basicGBM("./smalldata/cars.csv","cars.hex",
             new PrepData() { Vec prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("cylinders"); } 
             });
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
    basicGBM("./smalldata/airlines/allyears2k_headers.zip","air.hex",
             new PrepData() { Vec prep(Frame fr) { return fr.remove("IsDepDelayed"); }
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
    GBM gbm = null;
    try {
      gbm = new GBM();
      gbm.source = ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);
      gbm.vresponse = prep.prep(gbm.source);
      gbm.ntrees = 5;
      gbm.max_depth = 8;
      gbm.learn_rate = 0.1f;
      gbm.min_rows=1;
      gbm.serve();

    } finally {
      UKV.remove(dest);         // Remove original hex frame key
      if( gbm != null ) {
        gbm.source.remove();    // Remove hex frame internal guts
        UKV.remove(gbm.vresponse._key);
        gbm.remove();           // Remove GBM Job
      }
    }
  }

  @Test public void testBasicDRF() {
    //basicDRF("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { Vec prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("economy (mpg)"); } 
    //         });
    basicDRF("./smalldata/cars.csv","cars.hex",
             new PrepData() { Vec prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("cylinders"); } 
             });
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
    basicDRF("./smalldata/airlines/allyears2k_headers.zip","air.hex",
             new PrepData() { Vec prep(Frame fr) { return fr.remove("IsDepDelayed"); }
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
      drf.serve();

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
