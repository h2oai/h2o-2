package water;

import static water.util.Utils.difference;
import static water.util.Utils.isEmpty;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

import water.H2O.H2OCountedCompleter;
import water.H2O.H2OEmptyCompleter;
import water.api.*;
import water.api.Request.Validator.NOPValidator;
import water.api.RequestServer.API_VERSION;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.*;
import water.util.Utils.ExpectedExceptionForDebug;

public class Job extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  public Job(Key jobKey, Key dstKey){
   job_key = jobKey;
   destination_key = dstKey;
  }
  public static class JobCancelledException extends RuntimeException {
    public JobCancelledException(){super("job was cancelled!");}
    public JobCancelledException(String msg){super("job was cancelled! with msg '" + msg + "'");}
  }
  // Global LIST of Jobs key.
  static final Key LIST = Key.make(Constants.BUILT_IN_KEY_JOBS, (byte) 0, Key.BUILT_IN_KEY);
  private static final int KEEP_LAST_COUNT = 100;
  public static final long CANCELLED_END_TIME = -1;
  private static final int[] EMPTY = new int[0];

  @API(help = "Job key")
  public Key job_key; // Boolean read-only value; exists==>running, not-exists==>canceled/removed

  @API(help = "Destination key", filter = Default.class, json = true, validator = DestKeyValidator.class)
  public Key destination_key; // Key holding final value after job is removed
  static class DestKeyValidator extends NOPValidator<Key> {
    @Override public void validateRaw(String value) {
      if (Utils.contains(value, Key.ILLEGAL_USER_KEY_CHARS))
        throw new IllegalArgumentException("Key '" + value + "' contains illegal character! Please avoid these characters: " + Key.ILLEGAL_USER_KEY_CHARS);
    }
  }

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

  public int gridParallelism() {
    return 1;
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

    @API(help = "Ignored columns by name and zero-based index", filter=colsNamesIdxFilter.class, displayName="Ignored columns")
    public int[] ignored_cols = EMPTY;
    class colsNamesIdxFilter extends MultiVecSelect { public colsNamesIdxFilter() {super("source", MultiVecSelectType.NAMES_THEN_INDEXES); } }

    @API(help = "Ignored columns by name", filter=colsNamesFilter.class, displayName="Ignored columns by name", hide=true)
    public int[] ignored_cols_by_name = EMPTY;
    class colsNamesFilter extends MultiVecSelect { public colsNamesFilter() {super("source", MultiVecSelectType.NAMES_ONLY); } }

    @Override protected void logStart() {
      super.logStart();
      if (cols == null) {
        Log.info("    cols: null");
      } else {
        Log.info("    cols: " + cols.length + " columns selected");
      }

      if (ignored_cols == null) {
        Log.info("    ignored_cols: null");
      } else {
        Log.info("    ignored_cols: " + ignored_cols.length + " columns ignored");
      }
    }

    @Override protected void init() {
      super.init();

      // At most one of the following may be specified.
      int specified = 0;
      if (!isEmpty(cols)) { specified++; }
      if (!isEmpty(ignored_cols)) { specified++; }
      if (!isEmpty(ignored_cols_by_name)) { specified++; }
      if (specified > 1) throw new IllegalArgumentException("Arguments 'cols', 'ignored_cols_by_name', and 'ignored_cols' are exclusive");

      // If the column are not specified, then select everything.
      if (isEmpty(cols)) {
        cols = new int[source.vecs().length];
        for( int i = 0; i < cols.length; i++ )
          cols[i] = i;
      } else {
        if (!checkIdx(source, cols)) throw new IllegalArgumentException("Argument 'cols' specified invalid column!");
      }
      // Make a set difference between cols and (ignored_cols || ignored_cols_by_name)
      if (!isEmpty(ignored_cols) || !isEmpty(ignored_cols_by_name)) {
        int[] icols = ! isEmpty(ignored_cols) ? ignored_cols : ignored_cols_by_name;
        if (!checkIdx(source, icols)) throw new IllegalArgumentException("Argument '"+(!isEmpty(ignored_cols) ? "ignored_cols" : "ignored_cols_by_name")+"' specified invalid column!");
        cols = difference(cols, icols);
        // Setup all variables in consistence way
        ignored_cols = icols;
        ignored_cols_by_name = icols;
      }

      if( cols.length == 0 )
        throw new IllegalArgumentException("No column selected");
    }

    protected final Vec[] selectVecs(Frame frame) {
      Vec[] vecs = new Vec[cols.length];
      for( int i = 0; i < cols.length; i++ )
        vecs[i] = frame.vecs()[cols[i]];
      return vecs;
    }

    protected final Frame selectFrame(Frame frame) {
      Vec[] vecs = new Vec[cols.length];
      String[] names = new String[cols.length];
      for( int i = 0; i < cols.length; i++ ) {
        vecs[i] = frame.vecs()[cols[i]];
        names[i] = frame.names()[cols[i]];
      }
      return new Frame(names, vecs);
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

    @Override protected void registered(API_VERSION ver) {
      super.registered(ver);
      Argument c = find("ignored_cols");
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
      if( idx == -1 ) { 
        Vec vm = response.masterVec();
        if( vm != null ) idx = source.find(vm);
      }
      Log.info("    response: "+(idx==-1?"null":source._names[idx]));
      Log.info("    "+(classification ? "classification" : "regression"));
    }

    @Override protected void init() {
      super.init();
      // Does not alter the Response to an Enum column if Classification is
      // asked for: instead use the classification flag to decide between
      // classification or regression.
      Vec[] vecs = source.vecs();
      for( int i = cols.length - 1; i >= 0; i-- )
        if( vecs[cols[i]] == response )
          cols = Utils.remove(cols,i);
    }
  }

  public static abstract class ValidatedJob extends ModelJob {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    protected transient Vec[] _train, _valid;
    protected transient Vec _validResponse;
    protected transient String[] _names;
    protected transient String _responseName;

    @API(help = "Validation frame", filter = Default.class, mustExist = true)
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

    @Override
    public List clone(){
      List l = new List();
      l._jobs = _jobs.clone();
      for(int i = 0; i < l._jobs.length; ++i)
        l._jobs[i] = (Job)l._jobs[i].clone();
      return l;
    }
  }

  public static Job[] all() {
    List list = UKV.get(LIST);
    return list != null ? list._jobs : new Job[0];
  }

  public Job() {
    job_key = defaultJobKey();
//    destination_key = defaultDestKey();
    description = getClass().getSimpleName();
  }

  protected Key defaultJobKey() {
    // Pinned to self, because it should be almost always updated locally
    return Key.make((byte) 0, Key.JOB, H2O.SELF);
  }

  protected Key defaultDestKey() {
    return Key.make(getClass().getSimpleName() + Key.rand());
  }

  public Job start(final H2OCountedCompleter fjtask) {
    _fjtask = fjtask;
    Futures fs = new Futures();
    DKV.put(job_key, new Value(job_key, new byte[0]),fs);
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
    fs.blockForPending();
    return this;
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
    _fjtask.join();             // Block until top-level job is done
    T ans = (T) UKV.get(destination_key);
    remove();                   // Remove self-job
    return ans;
  }

  public void cancel() {
    cancel((String)null);
  }
  public void cancel(Throwable ex){

    if(_fjtask != null && !_fjtask.isDone())_fjtask.completeExceptionally(ex);
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
//    ex.printStackTrace();
    String stackTrace = sw.toString();
    cancel("Got exception '" + ex.getClass() + "', with msg '" + ex.getMessage() + "'\n" + stackTrace);
  }
  public void cancel(final String msg) {
    DKV.remove(self());
    DKV.write_barrier();
    new TAtomic<List>() {
      transient private Job _job;
      @Override public List atomic(List old) {
        if( old == null ) old = new List();
        Job[] jobs = old._jobs;
        for( int i = 0; i < jobs.length; i++ ) {
          if( jobs[i].job_key.equals(self()) ) {
            jobs[i].end_time = CANCELLED_END_TIME;
            jobs[i].exception = msg;
            _job = jobs[i];
            break;
          }
        }
        return old;
      }
      @Override public void onSuccess(){
        if(_job != null){
          final Job job = _job;
          H2O.submitTask(new H2OCountedCompleter() {
            @Override public void compute2() {job.onCancelled();}
          });
        }
      }
    }.invoke(LIST);
  }

  protected void onCancelled() {
  }
  public boolean cancelled() { return end_time == CANCELLED_END_TIME; }
  public static boolean cancelled(Key key) {
    return DKV.get(key) == null;
  }

  public void remove() {
    end_time = System.currentTimeMillis();
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

  public Job fork() {
    init();
    H2OCountedCompleter task = new H2OCountedCompleter() {
      @Override public void compute2() {
        Throwable t = null;
        try {
          Status status = Job.this.exec();
          if(status == Status.Done)
            Job.this.remove();
        } catch (Throwable t_) {
          t = t_;
          if(!(t instanceof ExpectedExceptionForDebug))
            Log.err(t);
        } finally {
          tryComplete();
        }
        if(t != null)
          Job.this.cancel(t);
      }
    };
    start(task);
    H2O.submitTask(task);
    return this;
  }

  public void invoke() {
    init();
    start(new H2OEmptyCompleter());
    Status status = exec();
    if(status == Status.Done)
      remove();
  }

  /**
   * Invoked before job runs. This is the place to checks arguments are valid or throw
   * IllegalArgumentException. It will get invoked both from the Web and Java APIs.
   */
  protected void init() throws IllegalArgumentException {
    if(destination_key == null)destination_key = defaultDestKey();
  }

  protected enum Status { Running, Done }

  /**
   * Actual job code.
   *
   * @return true if job is done, false if it will still be running after the method returns.
   */
  protected Status exec() {
    throw new RuntimeException("Should be overridden if job is a request");
  }

  public static boolean isJobEnded(Key jobkey) {
    boolean done = false;

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

    return done;
  }

  /**
   * Block synchronously waiting for a job to end, success or not.
   * @param jobkey Job to wait for.
   * @param pollingIntervalMillis Polling interval sleep time.
   */
  public static void waitUntilJobEnded(Key jobkey, int pollingIntervalMillis) {
    while (true) {
      if (isJobEnded(jobkey)) {
        return;
      }

      try { Thread.sleep (pollingIntervalMillis); } catch (Exception _) {}
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

    public ChunkProgressJob(long chunksTotal, Key destinationKey) {
      destination_key = destinationKey;
      _progress = Key.make(Key.make()._kb, (byte) 0, Key.DFJ_INTERNAL_USER, destinationKey.home_node());
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
      cancel(ex);
    }
  }

  public static boolean checkIdx(Frame source, int[] idx) {
    for (int i : idx) if (i<0 || i>source.vecs().length-1) return false;
    return true;
  }
}
