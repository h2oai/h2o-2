package water;

import water.DTask;

public class NOPTask extends DTask<NOPTask> {
  public NOPTask invoke(H2ONode h2o) { throw H2O.unimpl(); }
  public void compute2() { throw H2O.unimpl(); }

}
