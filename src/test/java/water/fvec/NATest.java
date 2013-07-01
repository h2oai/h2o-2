package water.fvec;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.util.Utils;

public class NATest extends TestUtil {

  int h = Integer.MAX_VALUE;

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  @Test public void testNAs() {
    File file = find_test_file("./smalldata/test/na_test.zip");
    Key key = NFSFileVec.make(file);
    Key okey = Key.make("na_test.hex");
    Frame fr = ParseDataset2.parse(okey,new Key[]{key});
    int nlines = (int)fr._vecs[0].length();
    // This variable could be declared static, except that that causes an issue
    // with the weaver trying to load these classes before a Cloud is formed.
    Class [] expectedTypes = new Class[]{C1Chunk.class,C1SChunk.class,C2Chunk.class,C2SChunk.class,C4Chunk.class,C4FChunk.class,C8Chunk.class,C8DChunk.class};

    assertTrue(fr._vecs.length == expectedTypes.length);
    for(int i = 0; i < expectedTypes.length; ++i)
      assertTrue(expectedTypes[i].isInstance(fr._vecs[i].elem2BV(0)));
    for(int i = 0; i < nlines-1; ++i){
    //  System.out.print(i+": ");
      for(Vec v:fr._vecs){
        //System.out.print(v.at(i) + " ");
        assertTrue("error at line "+i+", vec " + v.elem2BV(0).getClass().getSimpleName(),!v.isNA(v.at(i)) && !v.isNA(v.at8(i)));
      }
      //System.out.println();
    }
    for(Vec v:fr._vecs){
      assertTrue(v.isNA(v.at(nlines-1)) && v.isNA(v.at8(nlines-1)));
      v.replaceNAs(1.0, 2);
      assertTrue(!v.isNA(v.at(nlines-1)) && !v.isNA(v.at8(nlines-1)));
      assertTrue(v.at(nlines-1) == 1.0 && v.at8(nlines-1) == 2);
      v.setNAs(3.0, 4);
      assertTrue(v.isNA(v.at(nlines-1)) && v.isNA(v.at8(nlines-1)));
      assertTrue(v.at(nlines-1) == 3.0 && v.at8(nlines-1) == 4);
    }
    UKV.remove(key);
    UKV.remove(okey);
  }

  public void test2(){
    System.out.println("Running test2!");
    File file = new File("/Users/tomasnykodym/Downloads/140k_train_anonymised.csv");
    assert file.exists();
    Key key = NFSFileVec.make(file);
    Key okey = Key.make("na_test.hex");
    Frame fr = ParseDataset2.parse(okey,new Key[]{key});
    System.out.println(Arrays.toString(fr._names));
    for(int i = 0; i < 100; ++i){
      for(Vec v:fr._vecs){
        if(v._domain != null && i < v.length()) {
          int eval = (int)v.at8(i);
          if(eval < v._domain.length)
            System.out.print(v._domain[(int)v.at8(i)]);
          else
            System.out.print(eval);
        }
        System.out.print(" ");
      }
      System.out.println();
    }
    for(Vec v:fr._vecs)
      if(v._domain != null)System.out.println(Arrays.toString(v._domain));
    System.out.println("DONE!");
  }
  public static void main(String [] args) throws Exception{
    final int nnodes = 3;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.main(args);
    H2O.waitForCloudSize(nnodes);
//    System.out.println("test2");
    new NATest().test2();
//    System.out.println("test3");
//    new NATest().testNAs();
//    System.out.println("test4");
//    new NewVectorTest().testCompression();
  }
}