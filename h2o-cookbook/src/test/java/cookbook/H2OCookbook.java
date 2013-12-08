package cookbook;

import org.junit.Test;
import water.H2O;
import water.util.Log;

/**
 * Samples that exercise the H2O object.
 */
public class H2OCookbook extends AbstractCookbook {
    /**
     * Print basic information about the H2O singleton.
     */
    @Test
    public void h2o_001() {
        //-----------------------------------------------------------
        // Recipe setup.
        //-----------------------------------------------------------

        //-----------------------------------------------------------
        // Recipe body.
        //-----------------------------------------------------------
        Log.info("======================================================================");
        Log.info("This H2O instance's IP address: " + H2O.SELF_ADDRESS);
        Log.info("This H2O instance's HTTP port: " + H2O.API_PORT);

        // A helper function to log the above.
        logThisH2OInstanceWebBrowserAddress();

        // Note:
        Log.info("The name of your cloud is (-name command line option): " + H2O.NAME);
        Log.info("My cloud has this many instances in it (numbered 0 to n-1): " + H2O.CLOUD.size());
        Log.info("My node's instance number is: " + H2O.SELF.index());

        //-----------------------------------------------------------
        // Recipe clean up.
        //-----------------------------------------------------------
    }
}