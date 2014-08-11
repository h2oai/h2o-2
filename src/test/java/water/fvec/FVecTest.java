package water.fvec;

import static org.junit.Assert.*;
import java.io.File;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;

public class FVecTest extends TestUtil {
  static final double EPSILON = 1e-6;
  @BeforeClass public static void stall() { stall_till_cloudsize(2); }

  public static  Key makeByteVec(String kname, String... data) {
    return makeByteVec(Key.make(kname), data);
  }
  public static  Key makeByteVec(Key k, String... data) {
    byte [][] chunks = new byte[data.length][];
    long [] espc = new long[data.length+1];
    for(int i = 0; i < chunks.length; ++i){
      chunks[i] = data[i].getBytes();
      espc[i+1] = espc[i] + data[i].length();
    }
    Futures fs = new Futures();
    ByteVec bv = new ByteVec(Vec.newKey(),espc);
    for(int i = 0; i < chunks.length; ++i){
      Key chunkKey = bv.chunkKey(i);
      DKV.put(chunkKey, new Value(chunkKey,chunks[i].length,chunks[i],TypeMap.C1NCHUNK,Value.ICE),fs);
    }
    DKV.put(bv._key,bv,fs);
    Frame fr = new Frame(k,new String[]{"makeByteVec"},new Vec[]{bv});
    DKV.put(k, fr, fs);
    fs.blockForPending();
    return k;
  }

  // ==========================================================================
  @Test public void testBasicCRUD() {
    // Make and insert a FileVec to the global store
    File file = TestUtil.find_test_file("./smalldata/cars.csv");
    Key key = NFSFileVec.make(file);
    NFSFileVec nfs=DKV.get(key).get();

    int[] x = new ByteHisto().doAll(nfs)._x;
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
    @Override public void reduce( ByteHisto bh ) { water.util.Utils.add(_x,bh._x); }
  }

  // ==========================================================================
  @Test public void testSet() {
    File file = TestUtil.find_test_file("./smalldata/airlines/allyears2k_headers.zip");
    Key fkey = NFSFileVec.make(file);
    Key dest = Key.make("air.hex");
    Frame fr = ParseDataset2.parse(dest, new Key[]{fkey});
    try {
      // Scribble into a freshly parsed frame
      new SetDoubleInt().doAll(fr);
    } finally {
      fr.delete();
    }
  }

  static class SetDoubleInt extends MRTask2 {
    @Override public void map( Chunk chks[] ) {
      Chunk c=null;
      for( Chunk x : chks )
        if( x.getClass()==water.fvec.C2Chunk.class )
          { c=x; break; }
      assertNotNull("Expect to find a C2Chunk",c);
      assertTrue(c.writable());

      double d=c._vec.min();
      for( int i=0; i<c._len; i++ ) {
        double e = c.at0(i);
        c.set0(i,d);
        d=e;
      }
    }
  }

  // ==========================================================================
  // Test making a appendable vector from a plain vector
  @Test public void testNewVec() {
    // Make and insert a File8Vec to the global store
    File file = TestUtil.find_test_file("./smalldata/cars.csv");
    Key key = NFSFileVec.make(file);
    NFSFileVec nfs=DKV.get(key).get();
    Vec res = new TestNewVec().doAll(1,nfs).outputFrame(new String[]{"v"},new String[][]{null}).anyVec();
    assertEquals(nfs.at8(0)+1,res.at8(0));
    assertEquals(nfs.at8(1)+1,res.at8(1));
    assertEquals(nfs.at8(2)+1,res.at8(2));
    UKV.remove(key );
    UKV.remove(res._key);
  }

  public static class TestNewVec extends MRTask2<TestNewVec> {
    @Override public void map( Chunk in, NewChunk out ) {
      for( int i=0; i<in._len; i++ )
        out.append2( in.at8(i)+(in.at8(i) >= ' ' ? 1 : 0),0);
    }
  }

  // ==========================================================================
  @Test public void testParse2() {
    File file = TestUtil.find_test_file("../smalldata/logreg/syn_2659x1049.csv");
    Key fkey = NFSFileVec.make(file);

    Key okey = Key.make("syn.hex");
    Frame fr = ParseDataset2.parse(okey,new Key[]{fkey});
    Vec vz = null;
    try {
      assertEquals(fr.numCols(),1050); // Count of columns
      assertEquals(fr.numRows(),2659); // Count of rows

      double[] sums = new Sum().doAll(fr)._sums;
      assertEquals(3949,sums[0],EPSILON);
      assertEquals(3986,sums[1],EPSILON);
      assertEquals(3993,sums[2],EPSILON);

      // Create a temp column of zeros
      Vec v0 = fr.vecs()[0];
      Vec v1 = fr.vecs()[1];
      vz = v0.makeZero();
      // Add column 0 & 1 into the temp column
      new PairSum().doAll(vz,v0,v1);
      // Add the temp to frame
      // Now total the temp col
      fr.delete();              // Remove all other columns
      fr = new Frame(new String[]{"tmp"},new Vec[]{vz}); // Add just this one
      sums = new Sum().doAll(fr)._sums;
      assertEquals(3949+3986,sums[0],EPSILON);

    } finally {
      if( vz != null ) UKV.remove(vz._key);
      fr.delete();
    }
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
      assert _sums != null;
      assert mrt._sums != null;
      water.util.Utils.add(_sums,mrt._sums);  }
  }

  // Simple vector sum C=A+B
  private static class PairSum extends MRTask2<Sum> {
    @Override public void map( Chunk out, Chunk in1, Chunk in2 ) {
      for( int i=0; i<out._len; i++ )
        out.set0(i,in1.at80(i)+in2.at80(i));
    }
  }

  // ==========================================================================
  @Test public void testLargeCats() {
    File file = TestUtil.find_test_file("./smalldata/categoricals/40k_categoricals.csv.gz");
    Key fkey = NFSFileVec.make(file);
    Key okey = Key.make("cat.hex");
    Frame fr = ParseDataset2.parse(okey,new Key[]{fkey});
    UKV.remove(fkey);
    Vec vz = null;
    try {
      assertEquals(fr.numRows(),40000); // Count of rows
      assertEquals(fr.vecs()[0].domain().length,40000);

    } finally {
      if( vz != null ) UKV.remove(vz._key);
      fr.delete();
    }
  }
}
