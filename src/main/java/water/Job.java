package water;

import java.util.Arrays;
import java.util.UUID;

import water.H2O.H2OCountedCompleter;
import water.api.Constants;
import water.api.Progress2;

public class Job extends Request2 {
  // Global LIST of Jobs key.
  static final Key LIST = Key.make(Constants.BUILT_IN_KEY_JOBS, (byte) 0, Key.BUILT_IN_KEY);
  private static final int KEEP_LAST_COUNT = 100;
  public static final long CANCELLED_END_TIME = -1;

  @API(help = "Job key")
  public Key job_key; // Boolean read-only value; exists==>running, not-exists==>canceled/removed

  @API(help = "Destination key", required = true)
  public Key destination_key; // Key holding final value after job is removed

  public String _description;
  public long _startTime;
  public long _endTime;
  transient public H2OCountedCompleter _fjtask; // Top-level task you can block on

  public Key self() { return job_key; }
  public Key dest() { return destination_key; }

  public static abstract class FrameJob extends Job {
    @API(help = "Key with input frame", required = true, filter = source_keyFilter.class)
    public Key source_key;
    class source_keyFilter extends H2OHexKey { public source_keyFilter() { super(""); } }
  }

  public interface Progress {
    float progress();
  }

  public interface ProgressMonitor {
    public void update(long n);
  }

  public static class Fail extends Iced {
    public final String _message;
    public Fail(String message) { _message = message; }
  }

  static final class List extends Iced {
    Job[] _jobs = new Job[0];
  }

  public static void putEmptyJobList() {
    UKV.put(Job.LIST, new Job.List());
  }

  public static Job[] all() {
    List list = UKV.get(LIST);
    return list != null ? list._jobs : new Job[0];
  }

  protected Job() {
    setJobKey(UUID.randomUUID().toString());
  }

  public Job(String description, Key dest) {
    // Pinned to self, because it should be almost always updated locally
    this(UUID.randomUUID().toString(), description, dest);
  }

  protected Job(String keyName, String description, Key dest) {
    // Pinned to self, because it should be almost always updated locally
    setJobKey(keyName);
    _description = description;
    _startTime = System.currentTimeMillis();
    destination_key = dest;
  }

  final void setJobKey(String name) {
    job_key = Key.make(name, (byte) 0, Key.JOB, H2O.SELF);
  }

  public <T extends H2OCountedCompleter> T start(T fjtask) {
    _fjtask = fjtask;
    DKV.put(job_key, new Value(job_key, new byte[0]));
    new TAtomic<List>() {
      @Override public List atomic(List old) {
        if( old == null ) old = new List();
        Job[] jobs = old._jobs;
        old._jobs = Arrays.copyOf(jobs, jobs.length + 1);
        old._jobs[jobs.length] = Job.this;
        return old;
      }
    }.invoke(LIST);
    return fjtask;
  }

  // Overriden for Parse
  public float progress() {
    Freezable f = UKV.get(destination_key);
    if( f instanceof Job )
      return ((Job.Progress) f).progress();
    return 0;
  }

  // Block until the Job finishes.
  public <T> T get() {
    _fjtask.join();             // Block until top-level job is done
    T ans = (T) UKV.get(destination_key);
    remove();                   // Remove self-job
    return ans;
  }

  public void cancel() { cancel(job_key); }
  public static void cancel(final Key self) {
    DKV.remove(self);
    DKV.write_barrier();
    new TAtomic<List>() {
      @Override public List atomic(List old) {
        if( old == null ) old = new List();
        Job[] jobs = old._jobs;
        for( int i = 0; i < jobs.length; i++ ) {
          if( jobs[i].job_key.equals(self) ) {
            jobs[i]._endTime = CANCELLED_END_TIME;
            break;
          }
        }
        return old;
      }
    }.fork(LIST);
  }

  public boolean cancelled() { return cancelled(job_key); }
  public static boolean cancelled(Key self) {
    return DKV.get(self) == null;
  }

  public void remove() {
    DKV.remove(job_key);
    new TAtomic<List>() {
      @Override public List atomic(List old) {
        if( old == null ) return null;
        Job[] jobs = old._jobs;
        for( int i = 0; i < jobs.length; i++ ) {
          if( jobs[i].job_key.equals(job_key) ) {
            if( jobs[i]._endTime != CANCELLED_END_TIME )
              jobs[i]._endTime = System.currentTimeMillis();
            break;
          }
        }
        int count = jobs.length;
        while( count > KEEP_LAST_COUNT ) {
          long min = Long.MAX_VALUE;
          int n = -1;
          for( int i = 0; i < jobs.length; i++ ) {
            if( jobs[i]._endTime != 0 && jobs[i]._startTime < min ) {
              min = jobs[i]._startTime;
              n = i;
            }
          }
          if( n < 0 )
            break;
          jobs[n] = jobs[--count];
        }
        if( count < jobs.length )
          old._jobs = Arrays.copyOf(jobs, count);
        return old;
      }
    }.fork(LIST);
  }

  /** Finds a job with given key or returns null */
  public static final Job findJob(final Key key) {
    Job job = null;
    for( Job current : Job.all() ) {
      if( current.self().equals(key) ) {
        job = current;
        break;
      }
    }
    return job;
  }

  /** Finds a job with given dest key or returns null */
  public static final Job findJobByDest(final Key destKey) {
    Job job = null;
    for( Job current : Job.all() ) {
      if( current.dest().equals(destKey) ) {
        job = current;
        break;
      }
    }
    return job;
  }

  /** Returns job execution time in milliseconds */
  public final long executionTime() { return _endTime - _startTime; }

  // If job is a request

  @Override protected Response serve() {
    H2OCountedCompleter task = new H2OCountedCompleter() {
      @Override public void compute2() {
        run();
        tryComplete();
      }
    };
    H2O.submitTask(start(task));
    return Progress2.redirect(this, job_key, destination_key);
  }

  /**
   * Block synchronously waiting for a job to end, success or not.
   * @param jobkey Job to wait for.
   * @param pollingIntervalMillis Polling interval sleep time.
   */
  public static void waitUntilJobEnded(Key jobkey, int pollingIntervalMillis) {
    boolean done = false;
    while (! done) {
      try { Thread.sleep (pollingIntervalMillis); } catch (Exception _) {}
      Job[] jobs = Job.all();
      boolean found = false;
      for (int i = jobs.length - 1; i >= 0; i--) {
        if (jobs[i].job_key == null) {
          continue;
        }

        if (! jobs[i].job_key.equals(jobkey)) {
          continue;
        }

        // This is the job we are looking for.
        found = true;

        if (jobs[i]._endTime > 0) {
          done = true;
        }

        if (jobs[i].cancelled()) {
          done = true;
        }

        break;
      }

      if (! found) {
        done = true;
      }
    }
  }

  /**
   * Block synchronously waiting for a job to end, success or not.
   * @param jobkey Job to wait for.
   */
  public static void waitUntilJobEnded(Key jobkey) {
    int THREE_SECONDS_MILLIS = 3 * 1000;
    waitUntilJobEnded(jobkey, THREE_SECONDS_MILLIS);
  }

  protected void run() {
    throw new RuntimeException("Should be overridden if job is a request");
  }

  public static class ChunkProgress extends Iced implements Progress {
    final long _nchunks;
    final long _count;
    private final Status _status;
    final String _error;

    public enum Status { Computing, Done, Cancelled, Error };

    public Status status() { return _status; }

    public boolean isDone() { return _status == Status.Done || _status == Status.Error; }
    public String error() { return _error; }

    public ChunkProgress(long chunksTotal) {
      _nchunks = chunksTotal;
      _count = 0;
      _status = Status.Computing;
      _error = null;
    }

    private ChunkProgress(long nchunks, long computed, Status s, String err) {
      _nchunks = nchunks;
      _count = computed;
      _status = s;
      _error = err;
    }

    public ChunkProgress update(int count) {
      if( _status == Status.Cancelled || _status == Status.Error )
        return this;
      long c = _count + count;
      return new ChunkProgress(_nchunks, c, Status.Computing, null);
    }

    public ChunkProgress done() {
      return new ChunkProgress(_nchunks, _nchunks, Status.Done, null);
    }

    public ChunkProgress cancel() {
      return new ChunkProgress(0, 0, Status.Cancelled, null);
    }

    public ChunkProgress error(String msg) {
      return new ChunkProgress(0, 0, Status.Error, msg);
    }

    public final float progress() {
      if( _status == Status.Done ) return 1.0f;
      return Math.min(0.99f, (float) ((double) _count / (double) _nchunks));
    }
  }

  public static class ChunkProgressJob extends Job {
    Key _progress;

    public ChunkProgressJob(String desc, Key dest, long chunksTotal) {
      super(desc, dest);
      _progress = Key.make(Key.make()._kb, (byte) 0, Key.DFJ_INTERNAL_USER, dest.home_node());
      UKV.put(_progress, new ChunkProgress(chunksTotal));
    }

    public void updateProgress(final int c) { // c == number of processed chunks
      if( !cancelled() ) {
        new TAtomic<ChunkProgress>() {
          @Override public ChunkProgress atomic(ChunkProgress old) {
            if( old == null ) return null;
            return old.update(c);
          }
        }.fork(_progress);
      }
    }

    @Override public void remove() {
      super.remove();
      UKV.remove(_progress);
    }

    public final Key progressKey() { return _progress; }

    public void onException(Throwable ex) {
      UKV.remove(dest());
      Value v = DKV.get(progressKey());
      if( v != null ) {
        ChunkProgress p = v.get();
        p = p.error(ex.getMessage());
        DKV.put(progressKey(), p);
      }
      cancel();
    }
  }
}
