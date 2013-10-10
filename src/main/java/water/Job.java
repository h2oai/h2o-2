package water;

import java.util.Arrays;
import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;

import water.DException.DistributedException;
import water.H2O.H2OCountedCompleter;
import water.api.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.*;
import water.util.Utils.ExpectedExceptionForDebug;

public class Job extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // Global LIST of Jobs key.
  static final Key LIST = Key.make(Constants.BUILT_IN_KEY_JOBS, (byte) 0, Key.BUILT_IN_KEY);
  private static final int KEEP_LAST_COUNT = 100;
  public static final long CANCELLED_END_TIME = -1;

  @API(help = "Job key")
  public Key job_key; // Boolean read-only value; exists==>running, not-exists==>canceled/removed

  @API(help = "Destination key", required = true, filter = Default.class)
  public Key destination_key; // Key holding final value after job is removed

  @API(help = "Job description")
  public String description;

  @API(help = "Job start time")
  public long start_time;

  @API(help = "Job end time")
  public long end_time;

  @API(help = "Exception")
  public String exception;

  transient public H2OCountedCompleter _fjtask; // Top-level task you can block on

  public Key self() { return job_key; }
  public Key dest() { return destination_key; }

  protected void logStart() {
    Log.info("    destination_key: " + (destination_key != null ? destination_key : "null"));
  }

  public static abstract class FrameJob extends Job {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Source frame", required = true, filter = Default.class)
    public Frame source;

    @Override protected void logStart() {
      super.logStart();
      if (source == null) {
        Log.info("    source: null");
      }
      else {
        Log.info("    source.numCols(): " + source.numCols());
        Log.info("    source.numRows(): " + source.numRows());
      }
    }
  }

  public static abstract class ColumnsJob extends FrameJob {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Input columns (Indexes start at 0)", filter=colsFilter.class, hide=true)
    public int[] cols;
    class colsFilter extends MultiVecSelect { public colsFilter() { super("source"); } }

    @API(help = "Ignored columns by name", filter=colsFilter.class, displayName="Ignored columns")
    public int[] ignored_cols_by_name;
    class colsNamesFilter extends MultiVecSelect { public colsNamesFilter() {super("source", MultiVecSelectType.NAMES_ONLY); } }

    @Override protected void logStart() {
      super.logStart();
      if (cols == null) {
        Log.info("    cols: null");
      }
      else {
        Log.info("    cols: " + cols.length + " columns selected");
      }

      if (ignored_cols_by_name == null) {
        Log.info("    ignored_cols: null");
      }
      else {
        Log.info("    ignored_cols: " + ignored_cols_by_name.length + " columns ignored");
      }
    }

    @Override protected void init() {
      super.init();

      if( (cols != null && cols.length > 0) && (ignored_cols_by_name != null && ignored_cols_by_name.length > 0) )
        throw new IllegalArgumentException("Arguments 'cols' and 'ignored_cols_by_name' are exclusive");
      if( (cols != null && cols.length > 0) && (ignored_cols_by_name != null && ignored_cols_by_name.length > 0) )
        throw new IllegalArgumentException("Arguments 'cols' and 'ignored_cols_by_name' are exclusive");
      if(cols == null || cols.length == 0) {
        cols = new int[source.vecs().length];
        for( int i = 0; i < cols.length; i++ )
          cols[i] = i;
      }
      int length = cols.length;
      for( int g = 0; ignored_cols_by_name != null && g < ignored_cols_by_name.length; g++ ) {
        for( int i = 0; i < cols.length; i++ ) {
          if(cols[i] == ignored_cols_by_name[g]) {
            length--;
            // Move all, try to keep ordering
            System.arraycopy(cols, i + 1, cols, i, length - i);
            break;
          }
        }
      }
      if( length != cols.length )
        cols = ArrayUtils.subarray(cols, 0, length);
      if( cols.length == 0 )
        throw new IllegalArgumentException("No column selected");
    }

    protected final Vec[] selectVecs(Frame frame) {
      Vec[] vecs = new Vec[cols.length];
      for( int i = 0; i < cols.length; i++ )
        vecs[i] = frame.vecs()[cols[i]];
      return vecs;
    }
  }

  public static abstract class ModelJob extends ColumnsJob {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help="Column to use as class", required=true, filter=responseFilter.class)
    public Vec response;
    class responseFilter extends VecClassSelect { responseFilter() { super("source"); } }

    @API(help="Do Classification or regression", filter=myClassFilter.class)
    public boolean classification = true;
    class myClassFilter extends DoClassBoolean { myClassFilter() { super("source"); } }

    @Override protected void registered() {
      super.registered();
      Argument c = find("ignored_cols_by_name");
      Argument r = find("response");
      int ci = _arguments.indexOf(c);
      int ri = _arguments.indexOf(r);
      _arguments.set(ri, c);
      _arguments.set(ci, r);
      ((FrameKeyMultiVec) c).setResponse((FrameClassVec) r);
    }

    @Override protected void logStart() {
      super.logStart();
      int idx = source.find(response);
      Log.info("    response: "+(idx==-1?"null":source._names[idx]));
      Log.info("    "+(classification ? "classification" : "regression"));
    }

    @Override protected void init() {
      super.init();

      // Does not alter the Response to an Enum column if Classification is
      // asked for: instead use the classification flag to decide between
      // classification or regression.

      for( int i = cols.length - 1; i >= 0; i-- )
        if( source.vecs()[cols[i]] == response )
          cols = ArrayUtils.remove(cols, i);
    }
  }

  public static abstract class ValidatedJob extends ModelJob {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    protected transient Vec[] _train, _valid;
    protected transient Vec _validResponse;
    protected transient String[] _names;
    protected transient String _responseName;

    @API(help = "Validation frame", filter = Default.class)
    public Frame validation;

    @Override protected void logStart() {
      super.logStart();
      if (validation == null) {
        Log.info("    validation: null");
      } else {
        Log.info("    validation.numCols(): " + validation.numCols());
        Log.info("    validation.numRows(): " + validation.numRows());
      }
    }

    @Override protected void init() {
      super.init();

      int rIndex = 0;
      for( int i = 0; i < source.vecs().length; i++ )
        if( source.vecs()[i] == response )
          rIndex = i;
      _responseName = source._names != null && rIndex >= 0 ? source._names[rIndex] : "response";

      _train = selectVecs(source);
      _names = new String[cols.length];
      for( int i = 0; i < cols.length; i++ )
        _names[i] = source._names[cols[i]];

      if( validation != null ) {
        int idx = validation.find(source.names()[rIndex]);
        if( idx == -1 ) throw new IllegalArgumentException("Validation set does not have a response column called "+_responseName);
        _validResponse = validation.vecs()[idx];
      }
    }
  }

  public static abstract class HexJob extends Job {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Source key", required = true, filter = source_keyFilter.class)
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

  public static Job[] all() {
    List list = UKV.get(LIST);
    return list != null ? list._jobs : new Job[0];
  }

  public Job() {
    job_key = defaultJobKey();
    destination_key = defaultDestKey();
    description = getClass().getSimpleName();
  }

  protected Key defaultJobKey() {
    // Pinned to self, because it should be almost always updated locally
    return Key.make(UUID.randomUUID().toString(), (byte) 0, Key.JOB, H2O.SELF);
  }

  protected Key defaultDestKey() {
    return Key.make(getClass().getSimpleName() + "_" + UUID.randomUUID().toString());
  }

  public <T extends H2OCountedCompleter> T start(final T fjtask) {
    _fjtask = fjtask;
    DKV.put(job_key, new Value(job_key, new byte[0]));
    start_time = System.currentTimeMillis();
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

  // Overridden for Parse
  public float progress() {
    Freezable f = UKV.get(destination_key);
    if( f instanceof Progress )
      return ((Progress) f).progress();
    return 0;
  }

  // Block until the Job finishes.
  public <T> T get() {
    try{
      _fjtask.join();             // Block until top-level job is done
    } catch(DistributedException e){
      // rethrow as a runtime exception to stay consistent with FJ and to keep record on where the exception
      // was thrown locally
      throw new RuntimeException(e);
    }
    T ans = (T) UKV.get(destination_key);
    remove();                   // Remove self-job
    return ans;
  }

  public void cancel() { cancel("cancelled by user"); }
  public void cancel(String msg) { cancel(job_key,msg); }
  public static void cancel(final Key self, final String exception) {
    DKV.remove(self);
    DKV.write_barrier();
    new TAtomic<List>() {
      @Override public List atomic(List old) {
        if( old == null ) old = new List();
        Job[] jobs = old._jobs;
        for( int i = 0; i < jobs.length; i++ ) {
          if( jobs[i].job_key.equals(self) ) {
            final Job job = jobs[i];
            job.end_time = CANCELLED_END_TIME;
            job.exception = exception;
            H2OCountedCompleter task = new H2OCountedCompleter() {
              @Override public void compute2() {
                job.onCancelled();
                tryComplete();
              }
            };
            H2O.submitTask(task);
            break;
          }
        }
        return old;
      }
    }.fork(LIST);
  }

  protected void onCancelled() {
  }

  public boolean cancelled() {
    return !running() && end_time == Job.CANCELLED_END_TIME;
  }
  public boolean running() { return running(job_key); }
  public static boolean running(Key self) {
    return DKV.get(self) != null;
  }

  public void remove() {
    DKV.remove(job_key);
    new TAtomic<List>() {
      @Override public List atomic(List old) {
        if( old == null ) return null;
        Job[] jobs = old._jobs;
        for( int i = 0; i < jobs.length; i++ ) {
          if( jobs[i].job_key.equals(job_key) ) {
            if( jobs[i].end_time != CANCELLED_END_TIME )
              jobs[i].end_time = System.currentTimeMillis();
            break;
          }
        }
        int count = jobs.length;
        while( count > KEEP_LAST_COUNT ) {
          long min = Long.MAX_VALUE;
          int n = -1;
          for( int i = 0; i < jobs.length; i++ ) {
            if( jobs[i].end_time != 0 && jobs[i].start_time < min ) {
              min = jobs[i].start_time;
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
  public final long runTimeMs() {
    long until = end_time != 0 ? end_time : System.currentTimeMillis();
    return until - start_time;
  }

  /** Description of a speed criteria: msecs/frob */
  public String speedDescription() { return null; }

  /** Value of the described speed criteria: msecs/frob */
  public long speedValue() { return 0; }

  // If job is a request

  @Override protected Response serve() {
    fork();
    return redirect();
  }

  protected Response redirect() {
    return Progress2.redirect(this, job_key, destination_key);
  }

  //

  public H2OCountedCompleter fork() {
    init();
    H2OCountedCompleter task = new H2OCountedCompleter() {
      @Override public void compute2() {
        Throwable t = null;
        try {
          Job.this.exec();
          Job.this.done();
        } catch (Throwable t_) {
          t = t_;
          if(!(t instanceof ExpectedExceptionForDebug))
            Log.err(t);
        } finally {
          tryComplete();
        }
        if(t != null)
          update(Job.this, Utils.getStackAsString(t));
      }
    };
    H2O.submitTask(start(task));
    return task;
  }

  private static void update(final Job job, final String exception) {
    new TAtomic<List>() {
      @Override public List atomic(List old) {
        if( old != null && old._jobs != null )
          for(Job current : old._jobs)
            if(current == job)
              job.exception = exception;
        return old;
      }
    }.invoke(LIST);
  }

  public void invoke() {
    init();
    exec();
    done();
  }

  /**
   * Invoked before job runs. This is the place to checks arguments are valid or throw
   * IllegalArgumentException. It will get invoked both from the Web and Java APIs.
   */
  protected void init() throws IllegalArgumentException {
  }

  /**
   * Actual job code. Should be blocking until execution is done.
   */
  protected void exec() {
    throw new RuntimeException("Should be overridden if job is a request");
  }

  /**
   * Invoked after job has run for cleanup purposes.
   */
  protected void done() {
    remove();
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

        if (jobs[i].end_time > 0) {
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

  public static class ChunkProgress extends Iced implements Progress {
    final long _nchunks;
    final long _count;
    private final Status _status;
    final String _error;
    protected DException _ex;
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

    @Override public float progress() {
      if( _status == Status.Done ) return 1.0f;
      return Math.min(0.99f, (float) ((double) _count / (double) _nchunks));
    }
  }

  public static class ChunkProgressJob extends Job {
    Key _progress;

    public ChunkProgressJob(long chunksTotal) {
      _progress = Key.make(Key.make()._kb, (byte) 0, Key.DFJ_INTERNAL_USER, destination_key.home_node());
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
