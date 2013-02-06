
package water.exec;

import water.DRemoteTask;
import water.MRTask;

/**
 *
 * @author peta
 */
public abstract class MRColumnProducer extends MRTask {

  double _min = Double.POSITIVE_INFINITY;
  double _max = Double.NEGATIVE_INFINITY;
  double _tot = 0;


  protected void updateColumnWith(double x) {
    if (x < _min)
      _min = x;
    if (x > _max)
      _max = x;
    _tot += x;
  }

  @Override
  public void reduce(DRemoteTask drt) {
    // unify the min & max guys
    water.exec.MRColumnProducer other = (water.exec.MRColumnProducer) drt;
    if( other._min < _min )
      _min = other._min;
    if( other._max > _max )
      _max = other._max;
    _tot += other._tot;
  }


}
