package cookbook;

import water.H2O;
import water.TestUtil;
import water.util.Log;

/**
 * Cookbook helper class.
 *
 * Note that TestUtil provides a before/after test behavior to check for leaked H2O DKV keys.
 */
public class AbstractCookbook extends TestUtil {
    final long FOREVER_MILLIS = 1000000000;

    public void sleepMillis (long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (Exception e) {}
    }

    public void sleepForever() {
        sleepMillis (FOREVER_MILLIS);
    }

    public void logThisH2OInstanceWebBrowserAddress() {
        Log.info("http:/" + H2O.SELF_ADDRESS.toString() + ":" + H2O.API_PORT);
    }
}
