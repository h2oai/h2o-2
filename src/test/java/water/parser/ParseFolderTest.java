package water.parser;

import static org.junit.Assert.assertTrue;
import org.junit.Test;
import java.io.File;
import water.*;
import water.fvec.*;

public class ParseFolderTest extends TestUtil {

  /*@Test*/ public void testProstate() {
    Key k1 = null,k2 = null;
    try {
      k2 = loadAndParseFolder("multipart.hex","smalldata/parse_folder_test" );
      k1 = loadAndParseFile("full.hex","smalldata/glm_test/prostate_cat_replaced.csv");
      Value v1 = DKV.get(k1);
      Value v2 = DKV.get(k2);
      assertTrue("parsed values do not match!",v1.isBitIdentical(v2));
    } finally {
      Lockable.delete(k1);
      Lockable.delete(k2);
    }
  }

  @Test public void testParserRecoversFromException() {
    Throwable ex = null;
    Key fkey0=null,fkey1=null,fkey2=null,okey=null;
    try {
      okey = Key.make("junk.hex");
      fkey0 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_0.csv"));
      fkey1 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_1.csv"));
      fkey2 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_2.csv"));
      // Now "break" one of the files.
      Vec vec = DKV.get(fkey1).get();
      Chunk chk = vec.chunkForChunkIdx(0); // Load the chunk (which otherwise loads only lazily)
      chk._mem = null;                     // Illegal setup: Chunk _mem should never be null

      Frame fr = ParseDataset2.parse(okey, new Key[]{fkey0,fkey1,fkey2});
      System.out.println("Number of chunks: "+fr.anyVec().nChunks());
      fr.delete();
    } catch( Throwable e2 ) {
      ex = e2;
      System.out.println("parse throws "+ex+" as expected");
    } finally {
      assertTrue( "Parse did not throw out an NPE", ex != null ) ;
    }
    assertTrue( "All input & output keys not removed", DKV.get(fkey0)==null );
    assertTrue( "All input & output keys not removed", DKV.get(fkey1)==null );
    assertTrue( "All input & output keys not removed", DKV.get(fkey2)==null );
    assertTrue( "All input & output keys not removed", DKV.get(okey )==null );



  }

}
