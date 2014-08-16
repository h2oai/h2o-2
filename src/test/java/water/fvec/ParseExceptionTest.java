package water.fvec;

import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.File;
import water.*;
import water.parser.CustomParser;
import water.parser.GuessSetup;

public class ParseExceptionTest extends TestUtil {
  @BeforeClass public static void stall() { stall_till_cloudsize(3); }

  @Test public void testParserRecoversFromException() {
    Throwable ex = null;
    Key fkey0=null,fkey1=null,fkey2=null,okey=null;
    CustomParser.ParserSetup setup = null;
    try {
      okey = Key.make("junk.hex");
      fkey0 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_0.csv"));
      fkey1 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_1.csv"));
      fkey2 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_2.csv"));
      setup = new GuessSetup.GuessSetupTsk(new CustomParser.ParserSetup(), true).invoke(fkey0, fkey1, fkey2)._gSetup._setup;
      // Now "break" one of the files.  Globally.
      new Break(fkey1).invokeOnAllNodes();

      ParseDataset2.parse(okey, new Key[]{fkey0,fkey1,fkey2},setup,true);

    } catch( Throwable e2 ) {
      ex = e2;                  // Record expected exception
    }
    assertTrue( "Parse should throw an NPE",ex!=null);
    assertTrue( "All input & output keys not removed", DKV.get(fkey0)==null );
    assertTrue( "All input & output keys not removed", DKV.get(fkey1)==null );
    assertTrue( "All input & output keys not removed", DKV.get(fkey2)==null );
    assertTrue( "All input & output keys not removed", DKV.get(okey )==null );

    // Try again, in the same test, same inputs & outputs but not broken.
    // Should recover completely.
    okey = Key.make("junk.hex");
    fkey0 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_0.csv"));
    fkey1 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_1.csv"));
    fkey2 = NFSFileVec.make(new File("smalldata/parse_folder_test/prostate_2.csv"));
    Frame fr = ParseDataset2.parse(okey, new Key[]{fkey0,fkey1,fkey2});
    fr.delete();

    assertTrue( "All input & output keys not removed", DKV.get(fkey0)==null );
    assertTrue( "All input & output keys not removed", DKV.get(fkey1)==null );
    assertTrue( "All input & output keys not removed", DKV.get(fkey2)==null );
    assertTrue( "All input & output keys not removed", DKV.get(okey )==null );
  }

  private static class Break extends DRemoteTask<Break> {
    final Key _key;
    Break(Key key ) { _key = key; }
    @Override public void lcompute() {
      Vec vec = DKV.get(_key).get();
      Chunk chk = vec.chunkForChunkIdx(0); // Load the chunk (which otherwise loads only lazily)
      chk._mem = null;                     // Illegal setup: Chunk _mem should never be null
      tryComplete();
    }
    @Override public void reduce(Break drt ) {}
  }

}
