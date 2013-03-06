package water;

import water.DTask.DTaskImpl;

public class NOPTask extends DTaskImpl<NOPTask> {
  public NOPTask invoke(H2ONode h2o) { throw H2O.unimpl(); }
  public void compute2() { throw H2O.unimpl(); }

}
