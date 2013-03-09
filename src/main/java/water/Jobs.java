package water;

import java.util.UUID;

import water.api.Constants;

public abstract class Jobs {
  public static class Job extends Iced {
    public Key    _key;
    public String _description;
    public long   _startTime;
    public Key    _progress;
    public Key    _dest;
  }

  public static class Progress extends Iced {
    public long _value, _limit;

    public Progress() {
    }

    public Progress(long value, long limit) {
      _value = value;
      _limit = limit;
    }

    public final float get() {
      return Math.min(1f, _limit == 0 ? 0 : (float) ((double) _value / _limit));
    }

    // FIXME this should be called increment
    public static void update(Key key, final long delta) {
      new TAtomic<Progress>() {
        @Override
        public Progress atomic(Progress old) {
          old._value += delta;
          return old;
        }

        @Override
        public Progress alloc() {
          return new Progress();
        }
      }.invoke(key);
    }

    // FIXME this should be called update
    public static void set(Key key, final long value) {
      new TAtomic<Progress>() {
        @Override
        public Progress atomic(Progress old) {
          old._value = value;
          return old;
        }

        @Override
        public Progress alloc() {
          return new Progress();
        }
      }.invoke(key);
    }
  }

  public static class Fail extends Iced {
    String _message;

    public Fail(String message) {
      _message = message;
    }
  }

  private static final Key KEY = Key.make(Constants.BUILT_IN_KEY_JOBS, (byte) 0, Key.SINGLETONS);

  private static final class List extends Iced {
    Job[] _jobs;
  }

  static {
    new TAtomic<List>() {
      @Override
      public List alloc() {
        return new List();
      }

      @Override
      public List atomic(List old) {
        if( old == null ) {
          List empty = new List();
          empty._jobs = new Job[0];
          return empty;
        }
        return old;
      }
    }.invoke(KEY);
  }

  public static void init() {
    // Loads class to register KEY
  }

  public static Job[] get() {
    return UKV.get(KEY, new List())._jobs;
  }

  public static Job start(String description, Key dest) {
    return start(description, dest, new Progress());
  }

  public static Job start(String description, Key dest, Progress progress) {
    final Job job = new Job();
    job._key = Key.make(UUID.randomUUID().toString());
    DKV.put(job._key, new Value(job._key, ""));
    job._description = description;
    job._startTime = System.currentTimeMillis();
    job._progress = Key.make(UUID.randomUUID().toString());
    UKV.put(job._progress, progress);
    job._dest = dest;

    new TAtomic<List>() {
      @Override
      public List alloc() {
        return new List();
      }

      @Override
      public List atomic(List old) {
        Job[] jobs = old._jobs;
        old._jobs = new Job[jobs.length + 1];
        System.arraycopy(jobs, 0, old._jobs, 0, jobs.length);
        old._jobs[old._jobs.length - 1] = job;
        return old;
      }
    }.invoke(KEY);
    return job;
  }

  public static void cancel(Key key) {
    DKV.remove(key);
    DKV.write_barrier();
  }

  public static boolean cancelled(Key key) {
    return DKV.get(key) == null;
  }

  public static void remove(final Key key) {
    DKV.remove(key);
    new TAtomic<List>() {
      @Override
      public List alloc() {
        return new List();
      }

      @Override
      public List atomic(List old) {
        Job[] jobs = old._jobs;
        int index = -1;
        for( int i = 0; i < jobs.length; i++ ) {
          if( jobs[i]._key.equals(key) ) {
            index = i;
            break;
          }
        }
        if( index >= 0 ) {
          old._jobs = new Job[jobs.length - 1];
          int n = 0;
          for( int i = 0; i < jobs.length; i++ ) {
            if( i != index )
              old._jobs[n++] = jobs[i];
            else
              UKV.remove(jobs[i]._progress);
          }
        }
        return old;
      }
    }.invoke(KEY);
  }
}
