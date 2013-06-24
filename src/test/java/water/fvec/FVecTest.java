package water.fvec;

import static org.junit.Assert.assertEquals;
import java.io.File;
import java.util.Arrays;
import org.junit.*;
import water.*;

public class FVecTest extends TestUtil {
  static final double EPSILON = 1e-6;
  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // ==========================================================================
  @Test public void testBasicCRUD() {
    // Make and insert a FileVec to the global store
    File file = TestUtil.find_test_file("./smalldata/cars.csv");
    Key key = NFSFileVec.make(file);
    NFSFileVec nfs=DKV.get(key).get();

    int[] x = new ByteHisto().invoke(nfs)._x;
    int sum=0;
    for( int i : x )
      sum += i;
    assertEquals(file.length(),sum);

    UKV.remove(key);
  }

  public static class ByteHisto extends MRTask2<ByteHisto> {
    public int[] _x;
    // Count occurrences of bytes
    @Override public void map( Chunk bv ) {
      _x = new int[256];        // One-time set histogram array
      for( int i=0; i<bv._len; i++ )
        _x[(int)bv.at0(i)]++;
    }
    // ADD together all results
    @Override public void reduce( ByteHisto bh ) {
      for( int i=0; i<_x.length; i++ )
        _x[i] += bh._x[i];
    }
  }

  // ==========================================================================
  // Test making a appendable vector from a plain vector
  @Test public void testNewVec() {
    // Make and insert a FileVec to the global store
    File file = TestUtil.find_test_file("./smalldata/cars.csv");
    //File file = TestUtil.find_test_file("../Dropbox/Sris and Cliff/H20_Rush_New_Dataset_100k.csv");
    Key key = NFSFileVec.make(file);
    NFSFileVec nfs=DKV.get(key).get();
    Key key2 = Key.make("newKey",(byte)0,Key.VEC);
    AppendableVec nv = new AppendableVec(key2);
    Vec res = new TestNewVec().invoke(nv,nfs).vecs(0);
    assertEquals(nfs.at8(0)+1,res.at8(0));
    assertEquals(nfs.at8(1)+1,res.at8(1));
    assertEquals(nfs.at8(2)+1,res.at8(2));

    UKV.remove(key );
    UKV.remove(key2);
  }

  public static class TestNewVec extends MRTask2<TestNewVec> {
    @Override public void map( NewChunk out, Chunk in ) {
      for( int i=0; i<in._len; i++ )
        out.append2( in.at8(i)+(in.at8(i) >= ' ' ? 1 : 0),0);
    }
  }

  // ==========================================================================
  @SuppressWarnings("unused")
  @Test public void testParse() {
    //File file = TestUtil.find_test_file("./smalldata/airlines/allyears2k_headers.zip");
    File file = TestUtil.find_test_file("./smalldata/logreg/prostate_long.csv.gz");
    Key fkey = NFSFileVec.make(file);

    Key okey = Key.make("cars.hex");
    Frame fr = ParseDataset2.parse(okey,new Key[]{fkey});
    //System.out.println(fr);
    UKV.remove(fkey);
    UKV.remove(okey);
  }

  // ==========================================================================
  @Test public void testParse2() {
    //File file = TestUtil.find_test_file("./smalldata/logreg/prostate_long.csv.gz");
    //File file = TestUtil.find_test_file("../datasets/UCI/UCI-large/covtype/covtype.data");
    //File file = TestUtil.find_test_file("../smalldata/logreg/umass_chdage.csv");
    File file = TestUtil.find_test_file("../smalldata/logreg/syn_2659x1049.csv");
    Key fkey = NFSFileVec.make(file);

    Key okey = Key.make("syn.hex");
    Frame fr = ParseDataset2.parse(okey,new Key[]{fkey});
    UKV.remove(fkey);
    double[] sums = new Sum().invoke(fr)._sums;
    UKV.remove(okey);
    assertEquals(3949,sums[0],EPSILON);
    assertEquals(3986,sums[1],EPSILON);
    assertEquals(3993,sums[2],EPSILON);
  }

  // Sum each column independently
  private static class Sum extends MRTask2<Sum> {
    double _sums[];
    @Override public void map( Chunk[] bvs ) {
      _sums = new double[bvs.length];
      int len = bvs[0]._len;
      for( int i=0; i<len; i++ )
        for( int j=0; j<bvs.length; j++ )
          _sums[j] += bvs[j].at0(i);
    }
    @Override public void reduce( Sum mrt ) {
      for( int j=0; j<_sums.length; j++ )
        _sums[j] += mrt._sums[j];
    }
  }
}

