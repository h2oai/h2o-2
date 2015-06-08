package water.api;

import water.DTask;
import water.H2O;
import water.H2ONode;
import water.RPC;
import water.util.Log;

public class GarbageCollect extends Request {
  private static class GCTask extends DTask<GCTask> {
    public GCTask() {
    }

    @Override public void compute2() {
      Log.info("Calling System.gc() now...");
      System.gc();
      Log.info("System.gc() finished");
      tryComplete();
    }

    @Override public byte priority() {
      return H2O.MIN_HI_PRIORITY;
    }
  }

  @Override public RequestBuilders.Response serve(){
    for (H2ONode node : H2O.CLOUD._memary) {
      GCTask t = new GCTask();
      new RPC<GCTask>(node, t).call().get();
    }

    return RequestBuilders.Response.doneEmpty();
  }
}
