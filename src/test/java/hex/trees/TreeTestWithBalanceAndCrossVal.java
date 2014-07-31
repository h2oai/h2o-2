package hex.trees;

import org.junit.Test;

import water.TestUtil;

/** Abstract test to be shared between all tree builders
 * to test advanced workflows involving data loadbalancing
 * and cross validation.
 *
 * <p>The use case capture binary,multinomial classifiers which need to be factorized
 * and multinomial which does not need <code>as.factor</code> call for response column.</p>
 */
public abstract class TreeTestWithBalanceAndCrossVal extends TestUtil {

  /** The test is testing execution of balance and cross validation workflow on weather data.
  *
  *  <p>Response column is binary (0/1) so it needs to be factorized.</p>.
  */
 @Test
 public void testWeatherDataset() {
   testBalanceWithCrossValidation("smalldata/weather.csv", 23, ari(0,1,22), 2, 10);
 }

 /** Another test which is testing execution of balance and cross validation workflow on cars data
  *  <p>Response column is integer so it needs to be factorized</p>.
  **/
 @Test
 public void testCarBalanceAndCrossValidation() {
   testBalanceWithCrossValidation("smalldata/cars.csv", 2, ari(0), 2, 3);
 }
 /** Another test which is testing execution of balance and cross validation workflow on covtype data.
  * <p>Response column is already categorical.</p>
  */
 @Test
 public void testCovtypeBalanceAndCrossValidation() {
   testBalanceWithCrossValidation("smalldata/covtype/covtype.20k.data", 54, null, 1, 2);
 }

 abstract protected void testBalanceWithCrossValidation(String dataset, int response, int[] ignored_cols, int ntrees, int nfolds);

}
