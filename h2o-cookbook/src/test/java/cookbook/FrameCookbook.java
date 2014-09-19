package cookbook;

import org.junit.Test;
import water.H2O;
import water.Key;
import water.UKV;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;

import java.io.File;

/**
 * Sample recipes for using H2O's Fluid Vector Frames.
 */
public class FrameCookbook extends AbstractCookbook {
    /**
     * Read a frame from a file and print out some basic information.
     */
    @Test
    public void frame_001() {
        //-----------------------------------------------------------
        // Recipe setup.
        //-----------------------------------------------------------

        // Path to a file on the cluster filesystem.
        // Note that if you have a multi-node H2O cluster, this file must be visible on every H2O node.
        String fileName = "../smalldata/airlines/allyears2k_headers.zip";

        // Result key that we will use to store the above file in the H2O DKV (Distributed Key/Value store).
        Key resultFrameKey = Key.make("allyears2k_headers.hex");

        //-----------------------------------------------------------
        // Recipe body.
        //-----------------------------------------------------------
        File file = new File(fileName);
        Key tmpKey = NFSFileVec.make(file);
        Key[] arrayOfKeysToParse = new Key[] { tmpKey };

        Frame fr;
        try {
            fr = ParseDataset2.parse(resultFrameKey, arrayOfKeysToParse);
        }
        finally {
            UKV.remove(tmpKey);
        }

        // fr is now a valid frame.  Print some stuff about it.
        Log.info("======================================================================");
        Log.info("Number of columns: " + fr.numCols());

        String[] columnNames = fr.names();
        Log.info("Column names:");
        for (String s : columnNames) {
            Log.info("    " + s);
        }

        //-----------------------------------------------------------
        // Recipe clean up.
        // The unit test framework will fail a test if it leaks keys.
        //-----------------------------------------------------------

        // Add a sleep if you want to poke around using your Web Browser.
        //     From the menu, choose Data->View All
        //
        // logThisH2OInstanceWebBrowserAddress();
        // sleepForever();

        // UKV (User-visible Key/Value store) is an abstraction over DKV.
        //
        // When removing through the UKV then sub-objects referenced by the main Frame object
        // we created will also get removed.
        //
        // If we did a DKV.remove() here instead of UKV.remove(), then the test would fail with
        // leaked keys.
        fr.delete();
        UKV.remove(resultFrameKey);
    }
}
