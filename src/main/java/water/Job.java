package water;

import java.util.UUID;

import water.api.Constants;

public class Job extends Iced {
  static final Key LIST = Key.make(Constants.BUILT_IN_KEY_JOBS, (byte) 0, Key.BUILT_IN_KEY);

  public interface Progress {
    float progress();
  }

  public static class Fail extends Iced {
    String _message;

    public Fail(String message) {
      _message = message;
    }
  }

  private Key    _self;
  private Key    _dest;
  private String _description;
  private long   _startTime;

  private static final class List extends Iced {
    Job[] _jobs;
  }

  static {
    new TAtomic<List>() {
      @Override
      public List atomic(List old) {
        if( old == null ) {
          List empty = new List();
          empty._jobs = new Job[0];
          return empty;
        }
        return old;
      }
    }.invoke(LIST);
  }

  public static Job[] all() {
    return UKV.get(LIST, new List())._jobs;
  }

  public Job(String description, Key dest) {
    // Pinned to self, because it should be almost always updated locally
    _self = Key.make(UUID.randomUUID().toString(), (byte) 0, Key.JOB, H2O.SELF);
    _description = description;
    _startTime = System.currentTimeMillis();
    _dest = dest;
  }

  public void start() {
    DKV.put(_self, new Value(_self, ""));
    new TAtomic<List>() {
      @Override
      public List atomic(List old) {
        Job[] jobs = old._jobs;
        old._jobs = new Job[jobs.length + 1];
        System.arraycopy(jobs, 0, old._jobs, 0, jobs.length);
        old._jobs[old._jobs.length - 1] = Job.this;
        return old;
      }
    }.invoke(LIST);
  }

  public Job() {
  }

  public Key self() {
    return _self;
  }

  public Key dest() {
    return _dest;
  }

  public String description() {
    return _description;
  }

  public long startTime() {
    return _startTime;
  }

  // Overriden for Parse
  public float progress() {
    Job.Progress dest = UKV.get(dest());
    return dest != null ? dest.progress() : 0;
  }

  public <T> T get() {
    // TODO through notifications?
    while( DKV.get(_self) != null ) {
      try {
        Thread.sleep(1);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }
    }
    return (T) UKV.get(_dest);
  }

  public void cancel() {
    cancel(_self);
  }

  public static void cancel(Key self) {
    DKV.remove(self);
    DKV.write_barrier();
  }

  public boolean cancelled() {
    return cancelled(_self);
  }

  public static boolean cancelled(Key self) {
    return DKV.get(self) == null;
  }

  public void remove() {
    DKV.remove(_self);
    new TAtomic<List>() {
      @Override
      public List atomic(List old) {
        Job[] jobs = old._jobs;
        int index = -1;
        for( int i = 0; i < jobs.length; i++ ) {
          if( jobs[i]._self.equals(_self) ) {
            index = i;
            break;
          }
        }
        if( index >= 0 ) {
          old._jobs = new Job[jobs.length - 1];
          int n = 0;
          for( int i = 0; i < jobs.length; i++ )
            if( i != index )
              old._jobs[n++] = jobs[i];
        }
        return old;
      }
    }.invoke(LIST);
  }
}