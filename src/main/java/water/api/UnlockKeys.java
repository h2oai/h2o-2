
package water.api;

import water.*;
import water.util.Log;

public class UnlockKeys extends Request2 {

  @Override public Response serve() {
    try {
      Log.info("Unlocking all locked keys on the cluster.");
      new UnlockTask().invokeOnAllNodes();
    } catch( Throwable e ) {
      return Response.error(e);
    }
    return Response.done(this);
  }

  public class UnlockTask extends DRemoteTask<UnlockTask> {
    @Override public void reduce(UnlockTask drt) { }
    @Override public byte priority() { return H2O.GUI_PRIORITY; }

    @Override public void lcompute() {
      final H2O.KeyInfo[] kinfo = H2O.KeySnapshot.localSnapshot(true)._keyInfos;
      for(H2O.KeyInfo k:kinfo) {
        if(!k.isLockable()) continue;
        final Value val = DKV.get(k._key);
        if( val == null ) continue;
        final Object obj = val.rawPOJO();
        if( obj == null ) continue; //need to have a POJO to be locked
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
          Log.info("Unlocked key '" + k._key + "' from " + lockers.length + " lockers.");
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
