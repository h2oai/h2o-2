package water.parser;

import static org.junit.Assert.assertTrue;
import org.junit.Test;
import java.io.File;
import water.*;
import water.fvec.*;

public class ParseFolderTest extends TestUtil {

  @Test public void testProstate() {
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
    try {
      Key fkey0 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_0.csv"));
      Key fkey1 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_1.csv"));
//      Key fkey2 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_2.csv"));
      // Now "break" one of the files.
      Vec vec = DKV.get(fkey1).get();
      Chunk chk = vec.chunkForChunkIdx(0); // Load the chunk (which otherwise loads only lazily)
      chk._len = 9999;
      chk._mem = null;

      Key okey = Key.make("junk.hex");
      Frame fr = ParseDataset2.parse(okey, new Key[]{fkey0,fkey1});
      System.out.println("Number of chunks: "+fr.anyVec().nChunks());
      fr.delete();

    } finally {
    }
  }

}
