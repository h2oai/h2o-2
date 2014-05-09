
package water.api;

import water.*;
import water.util.Log;

import java.util.Set;

public class UnlockKeys extends Request2 {

  @Override protected Response serve() {
    try {
      Log.info("Unlocking all locked keys on the cluster.");
      UnlockTask cleanup = new UnlockTask();
      cleanup.invokeOnAllNodes();
    } catch( Throwable e ) {
      return Response.error(e);
    }
    return Response.done(this);
  }

  public class UnlockTask extends DRemoteTask<UnlockTask> {
    @Override public void reduce(UnlockTask drt) { }
    @Override public byte priority() { return H2O.GUI_PRIORITY; }

    @Override public void lcompute() {
      // Optional: cancel all jobs
//      for (Job job : Job.all()) {
//        job.cancel();
//        Job.waitUntilJobEnded(job.self());
//      }

      final Set<Key> keySet = H2O.globalKeySet(null);
      for( Key key : keySet ) {
        if (!key.home()) continue; // only unlock local keys
        final Value val = DKV.get(key);
        if( val == null ) continue;
        if (val.rawPOJO() == null) continue; //need to have a POJO to be locked
        if( !val.isLockable() ) continue;
        final Object obj = val.rawPOJO();
        assert(obj instanceof Lockable<?>);
        final Lockable<?> lockable = (Lockable<?>)(obj);
        final Key[] lockers = ((Lockable) obj)._lockers;
        if (lockers != null) {
          // check that none of the locking jobs is still running
          for (Key locker : lockers) {
            if (locker != null && locker.type() == Key.JOB) {
              final Job job = UKV.get(locker);
              if (job != null && job.isRunning())
                throw new UnsupportedOperationException("Cannot unlock all keys since locking jobs are still running.");
            }
          }
          lockable.unlock_all();
          Log.info("Unlocked key '" + key + "' from " + lockers.length + " lockers.");
        }
      }
      Log.info("All keys are now unlocked.");
      tryComplete();
    }
  }

  @Override
  public boolean toHTML(StringBuilder sb) {
    DocGen.HTML.paragraph(sb, "All keys are now unlocked.");
    return true;
  }
}
