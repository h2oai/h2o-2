package water;

import water.DTask;

public class NOPTask extends DTask<NOPTask> {
  public void compute2() { throw H2O.unimpl(); }
}
