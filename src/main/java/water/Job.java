package water;

import hex.FrameSplitter;
import static water.util.Utils.difference;
import static water.util.Utils.isEmpty;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;

import water.H2O.H2OCountedCompleter;
import water.H2O.H2OEmptyCompleter;
import water.api.*;
import water.api.Request.Validator.NOPValidator;
import water.api.RequestServer.API_VERSION;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.*;
import water.util.Utils.ExpectedExceptionForDebug;

import dontweave.gson.*;

public abstract class Job extends Func {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  /** A system key for global list of Job keys. */
  public static final Key LIST = Key.make(Constants.BUILT_IN_KEY_JOBS, (byte) 0, Key.BUILT_IN_KEY);
  /** Shared empty int array. */
  private static final int[] EMPTY = new int[0];

  @API(help = "Job key")
  public Key job_key;
  @API(help = "Destination key", filter = Default.class, json = true, validator = DestKeyValidator.class)
  public Key destination_key; // Key holding final value after job is removed
  static class DestKeyValidator extends NOPValidator<Key> {
    @Override public void validateRaw(String value) {
      if (Utils.contains(value, Key.ILLEGAL_USER_KEY_CHARS))
        throw new IllegalArgumentException("Key '" + value + "' contains illegal character! Please avoid these characters: " + Key.ILLEGAL_USER_KEY_CHARS);
    }
  }
  // Output parameters
  @API(help = "Job description") public String   description;
  @API(help = "Job start time")  public long     start_time;
  @API(help = "Job end time")    public long     end_time;
  @API(help = "Exception")       public String   exception;
  @API(help = "Job state")       public JobState state;

  transient public H2OCountedCompleter _fjtask; // Top-level task you can block on
  transient protected boolean _cv;

  /** Possible job states. */
  public static enum JobState {
    CREATED,   // Job was created
    RUNNING,   // Job is running
    CANCELLED, // Job was cancelled by user
    FAILED,   // Job crashed, error message/exception is available
    DONE       // Job was successfully finished
  }

  public Job(Key jobKey, Key dstKey){
    job_key = jobKey;
    destination_key = dstKey;
    state = JobState.CREATED;
  }
  public Job() {
    job_key = defaultJobKey();
    description = getClass().getSimpleName();
    state = JobState.CREATED;
  }
  /** Private copy constructor used by {@link JobHandle}. */
  private Job(final Job prior) {
    this(prior.job_key, prior.destination_key);
    this.description = prior.description;
    this.start_time  = prior.start_time;
    this.end_time    = prior.end_time;
    this.state       = prior.state;
    this.exception   = prior.exception;
  }

  public Key self() { return job_key; }
  public Key dest() { return destination_key; }

  public int gridParallelism() {
    return 1;
  }

  protected Key defaultJobKey() {
    // Pinned to this node (i.e., the node invoked computation), because it should be almost always updated locally
    return Key.make((byte) 0, Key.JOB, H2O.SELF);
  }

  protected Key defaultDestKey() {
    return Key.make(getClass().getSimpleName() + Key.rand());
  }

  /** Start this task based on given top-level fork-join task representing job computation.
   * @param fjtask top-level job computation task.
   * @return this job in {@link JobState#RUNNING} state
   *
   * @see JobState
   * @see H2OCountedCompleter
   */
  public /** FIXME: should be final or at least protected */ Job start(final H2OCountedCompleter fjtask) {
    assert state == JobState.CREATED : "Trying to run job which was already run?";
    assert fjtask != null : "Starting a job with null working task is not permitted! Fix you API";
    _fjtask = fjtask;
    start_time = System.currentTimeMillis();
    state      = JobState.RUNNING;
    // Save the full state of the job
    UKV.put(self(), this);
    // Update job list
    new TAtomic<List>() {
      @Override public List atomic(List old) {
        if( old == null ) old = new List();
        Key[] jobs = old._jobs;
        old._jobs = Arrays.copyOf(jobs, jobs.length + 1);
        old._jobs[jobs.length] = job_key;
        return old;
      }
    }.invoke(LIST);
    return this;
  }

  /** Return progress of this job.
   *
   * @return the value in interval &lt;0,1&gt; representing job progress.
   */
  public float progress() {
    Freezable f = UKV.get(destination_key);
    if( f instanceof Progress )
      return ((Progress) f).progress();
    return 0;
  }

  /** Blocks and get result of this job.
   * <p>
   * The call blocks on working task which was passed via {@link #start(H2OCountedCompleter)} method
   * and returns the result which is fetched from UKV based on job destination key.
   * </p>
   * @return result of this job fetched from UKV by destination key.
   * @see #start(H2OCountedCompleter)
   * @see UKV
   */
  public <T> T get() {
    _fjtask.join();             // Block until top-level job is done
    T ans = (T) UKV.get(destination_key);
    remove();                   // Remove self-job
    return ans;
  }

  /** Signal cancellation of this job.
   * <p>The job will be switched to state {@link JobState#CANCELLED} which signals that
   * the job was cancelled by a user. */
  public void cancel() {
    cancel((String)null, JobState.CANCELLED);
  }
  /** Signal exceptional cancellation of this job.
   * @param ex exception causing the termination of job.
   */
  public void cancel(Throwable ex){
    if(ex instanceof JobCancelledException || ex.getMessage() != null && ex.getMessage().contains("job was cancelled"))
      return;
    if(ex instanceof  IllegalArgumentException || ex.getCause() instanceof IllegalArgumentException) {
      cancel("Illegal argument: " + ex.getMessage());
      return;
    }
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
    String stackTrace = sw.toString();
    cancel("Got exception '" + ex.getClass() + "', with msg '" + ex.getMessage() + "'\n" + stackTrace, JobState.FAILED);
    if(_fjtask != null && !_fjtask.isDone()) _fjtask.completeExceptionally(ex);
  }
  /** Signal exceptional cancellation of this job.
   * @param msg cancellation message explaining reason for cancelation
   */
  public void cancel(final String msg) {
    JobState js = msg == null ? JobState.CANCELLED : JobState.FAILED;
    cancel(msg, js);
  }
  private void cancel(final String msg, JobState resultingState ) {
    if(resultingState == JobState.CANCELLED) {
      Log.info("Job " + self() + "("  + description + ") was cancelled.");
    }
    else {
      Log.err("Job " + self() + "("  + description + ") failed.");
      Log.err(msg);
    }
    exception = msg;
    state = resultingState;
    // replace finished job by a job handle
    replaceByJobHandle();
    DKV.write_barrier();
    final Job job = this;
    H2O.submitTask(new H2OCountedCompleter() {
      @Override public void compute2() {
        job.onCancelled();
      }
    });
  }

  /**
   * Callback which is called after job cancellation (by user, by exception).
   */
  protected void onCancelled() {
  }

  /** Returns true if the job was cancelled by the user or crashed.
   * @return true if the job is in state {@link JobState#CANCELLED} or {@link JobState#FAILED}
   */
  public boolean isCancelledOrCrashed() {
    return state == JobState.CANCELLED || state == JobState.FAILED;
  }

  /** Returns true if the job was terminated by unexpected exception.
   * @return true, if the job was terminated by unexpected exception.
   */
  public boolean isCrashed() { return state == JobState.FAILED; }

  /** Returns true if this job is correctly finished.
   * @return returns true if the job finished and it was not cancelled or crashed by an exception.
   */
  public boolean isDone() { return state == JobState.DONE; }

  /** Returns true if this job is running
   * @return returns true only if this job is in running state.
   */
  public boolean isRunning() { return state == JobState.RUNNING; }

  public JobState getState() { return state; }


  /** Returns a list of all jobs in a system.
   * @return list of all jobs including running, done, cancelled, crashed jobs.
   */
  public static Job[] all() {
    List list = UKV.get(LIST);
    Job[] jobs = new Job[list==null?0:list._jobs.length];
    int j=0;
    for( int i=0; i<jobs.length; i++ ) {
      Job job = UKV.get(list._jobs[i]);
      if( job != null ) jobs[j++] = job;
    }
    if( j<jobs.length ) jobs = Arrays.copyOf(jobs,j);
    return jobs;
  }
  /** Check if given job is running.
   *
   * @param job_key job key
   * @return true if job is still running else returns false.
   */
  public static boolean isRunning(Key job_key) {
    Job j = UKV.get(job_key);
    assert j!=null : "Job should be always in DKV!";
    return j.isRunning();
  }
  /**
   * Returns true if job is not running.
   * The job can be cancelled, crashed, or already done.
   *
   * @param jobkey job identification key
   * @return true if job is done, cancelled, or crashed, else false
   */
  public static boolean isEnded(Key jobkey) { return !isRunning(jobkey); }

  /**
   * Marks job as finished and records job end time.
   */
  public void remove() {
    end_time = System.currentTimeMillis();
    if( state == JobState.RUNNING )
      state = JobState.DONE;
    // Overwrite handle - copy end_time, state, msg
    replaceByJobHandle();
  }

  /** Finds a job with given key or returns null.
   *
   * @param jobkey job key
   * @return returns a job with given job key or null if a job is not found.
   */
  public static Job findJob(final Key jobkey) { return UKV.get(jobkey); }

  /** Finds a job with given dest key or returns null */
  public static Job findJobByDest(final Key destKey) {
    Job job = null;
    for( Job current : Job.all() ) {
      if( current.dest().equals(destKey) ) {
        job = current;
        break;
      }
    }
    return job;
  }

  /** Returns job execution time in milliseconds.
   * If job is not running then returns job execution time. */
  public final long runTimeMs() {
    long until = end_time != 0 ? end_time : System.currentTimeMillis();
    return until - start_time;
  }

  /** Description of a speed criteria: msecs/frob */
  public String speedDescription() { return null; }

  /** Value of the described speed criteria: msecs/frob */
  public long speedValue() { return 0; }

  @Override protected Response serve() {
    fork();
    return redirect();
  }

  protected Response redirect() {
    return Progress2.redirect(this, job_key, destination_key);
  }


  /**
   * Forks computation of this job.
   *
   * <p>The call does not block.</p>
   * @return always returns this job.
   */
  public Job fork() {
    init();
    H2OCountedCompleter task = new H2OCountedCompleter() {
      @Override public void compute2() {
        try {
          try {
            // Exec always waits till the end of computation
            Job.this.exec();
            Job.this.remove();
          } catch (Throwable t) {
            if(!(t instanceof ExpectedExceptionForDebug))
              Log.err(t);
            Job.this.cancel(t);
          }
        } finally {
            tryComplete();
        }
      }
    };
    start(task);
    H2O.submitTask(task);
    return this;
  }

  @Override public void invoke() {
    init();
    start(new H2OEmptyCompleter());  // mark job started
    exec(); // execute the implementation
    remove();     // remove the job
  }

  /**
   * Invoked before job runs. This is the place to checks arguments are valid or throw
   * IllegalArgumentException. It will get invoked both from the Web and Java APIs.
   *
   * @throws IllegalArgumentException throws the exception if initialization fails to ensure
   * correct job runtime environment.
   */
  @Override protected void init() throws IllegalArgumentException {
    if (destination_key == null) destination_key = defaultDestKey();
  }

  /**
   * Block synchronously waiting for a job to end, success or not.
   * @param jobkey Job to wait for.
   * @param pollingIntervalMillis Polling interval sleep time.
   */
  public static void waitUntilJobEnded(Key jobkey, int pollingIntervalMillis) {
    while (true) {
      if (Job.isEnded(jobkey)) {
        return;
      }

      try { Thread.sleep (pollingIntervalMillis); } catch (Exception ignore) {}
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
    public enum Status { Computing, Done, Cancelled, Error }

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
      if( isRunning(self()) ) {
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

  /* Update end_time, state, msg, preserve start_time */
  private void replaceByJobHandle() {
    assert state != JobState.RUNNING : "Running job cannot be replaced.";
    final Job self = this;
    new TAtomic<Job>() {
      @Override public Job atomic(Job old) {
        if( old == null ) return null;
        JobHandle jh = new JobHandle(self);
        jh.start_time = old.start_time;
        return jh;
      }
    }.fork(job_key);
  }

  /**
   * A job which operates with a frame.
   *
   * INPUT frame
   */
  public static abstract class FrameJob extends Job {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Source frame", required = true, filter = Default.class, json = true)
    public Frame source;

    /**
     * Annotate the number of columns and rows of the training data set in the job parameter JSON
     * @return JsonObject annotated with num_cols and num_rows of the training data set
     */
    @Override public JsonObject toJSON() {
      JsonObject jo = super.toJSON();
      if (source != null) {
        jo.getAsJsonObject("source").addProperty("num_cols", source.numCols());
        jo.getAsJsonObject("source").addProperty("num_rows", source.numRows());
      }
      return jo;
    }
  }

  /**
   * A job which has an input represented by a frame and frame column filter.
   * The filter can be specified by ignored columns or by used columns.
   *
   * INPUT list ignored columns by idx XOR list of ignored columns by name XOR list of used columns
   *
   * @see FrameJob
   */
  public static abstract class ColumnsJob extends FrameJob {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Input columns (Indexes start at 0)", filter=colsFilter.class, hide=true)
    public int[] cols;
    class colsFilter extends MultiVecSelect { public colsFilter() { super("source"); } }

    @API(help = "Ignored columns by name and zero-based index", filter=colsNamesIdxFilter.class, displayName="Ignored columns")
    public int[] ignored_cols;
    class colsNamesIdxFilter extends MultiVecSelect { public colsNamesIdxFilter() {super("source", MultiVecSelectType.NAMES_THEN_INDEXES); } }

    @API(help = "Ignored columns by name", filter=colsNamesFilter.class, displayName="Ignored columns by name", hide=true)
    public int[] ignored_cols_by_name = EMPTY;
    class colsNamesFilter extends MultiVecSelect { public colsNamesFilter() {super("source", MultiVecSelectType.NAMES_ONLY); } }

    /**
     * Annotate the used and ignored columns in the job parameter JSON
     * For both the used and the ignored columns, the following rules apply:
     * If the number of columns is less or equal than 100, a dense list of used columns is reported.
     * If the number of columns is greater than 100, the number of columns is reported.
     * If the number of columns is 0, a "N/A" is reported.
     * @return JsonObject annotated with used/ignored columns
     */
    @Override public JsonObject toJSON() {
      JsonObject jo = super.toJSON();
      if (!jo.has("source") || source==null) return jo;
      HashMap<String, int[]> map = new HashMap<String, int[]>();
      map.put("used_cols", cols);
      map.put("ignored_cols", ignored_cols);
      for (String key : map.keySet()) {
        int[] val = map.get(key);
        if (val != null) {
          if(val.length>100) jo.getAsJsonObject("source").addProperty("num_" + key, val.length);
          else if(val.length>0) {
            StringBuilder sb = new StringBuilder();
            for (int c : val) sb.append(c + ",");
            jo.getAsJsonObject("source").addProperty(key, sb.toString().substring(0, sb.length()-1));
          } else {
            jo.getAsJsonObject("source").add(key, JsonNull.INSTANCE);
          }
        }
      }
      return jo;
    }

    @Override protected void init() {
      super.init();
      if (_cv) return;

      // At most one of the following may be specified.
      int specified = 0;
      if (!isEmpty(cols)) { specified++; }
      if (!isEmpty(ignored_cols)) { specified++; }
      if (!isEmpty(ignored_cols_by_name)) { specified++; }
      if (specified > 1) throw new IllegalArgumentException("Arguments 'cols', 'ignored_cols_by_name', and 'ignored_cols' are exclusive");

      // Unify all ignored cols specifiers to ignored_cols.
      {
        if (!isEmpty(ignored_cols_by_name)) {
          assert (isEmpty(ignored_cols));
          ignored_cols = ignored_cols_by_name;
          ignored_cols_by_name = EMPTY;
        }
        if (ignored_cols == null) {
          ignored_cols = new int[0];
        }
      }

      // At this point, ignored_cols_by_name is dead.
      assert (isEmpty(ignored_cols_by_name));

      // Create map of ignored columns for speed.
      HashMap<Integer,Integer> ignoredColsMap = new HashMap<Integer,Integer>();
      for ( int i = 0; i < ignored_cols.length; i++) {
        int value = ignored_cols[i];
        ignoredColsMap.put(new Integer(value), new Integer(1));
      }

      // Add UUID cols to ignoredColsMap.  Duplicates get folded into one entry.
      Vec[] vecs = source.vecs();
      for( int i = 0; i < vecs.length; i++ ) {
        if (vecs[i].isUUID()) {
          ignoredColsMap.put(new Integer(i), new Integer(1));
        }
      }

      // Rebuild ignored_cols from the map.  Sort it.
      {
        ignored_cols = new int[ignoredColsMap.size()];
        int j = 0;
        for (Integer key : ignoredColsMap.keySet()) {
          ignored_cols[j] = key.intValue();
          j++;
        }

        Arrays.sort(ignored_cols);
      }

      // If the columns are not specified, then select everything.
      if (isEmpty(cols)) {
        cols = new int[source.vecs().length];
        for( int i = 0; i < cols.length; i++ )
          cols[i] = i;
      } else {
        if (!checkIdx(source, cols)) throw new IllegalArgumentException("Argument 'cols' specified invalid column!");
      }

      // Make a set difference between cols and ignored_cols.
      if (!isEmpty(ignored_cols)) {
        int[] icols = ! isEmpty(ignored_cols) ? ignored_cols : ignored_cols_by_name;
        if (!checkIdx(source, icols)) throw new IllegalArgumentException("Argument 'ignored_cols' or 'ignored_cols_by_name' specified invalid column!");
        cols = difference(cols, icols);
        // Setup all variables in consistent way
        ignored_cols = icols;
        ignored_cols_by_name = icols;
      }

      if( cols.length == 0 ) {
        throw new IllegalArgumentException("No column selected");
      }
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

  /**
   * A columns job that requires a response.
   *
   * INPUT response column from source
   */
  public static abstract class ColumnsResJob extends ColumnsJob {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help="Column to use as class", required=true, filter=responseFilter.class, json = true)
    public Vec response;
    class responseFilter extends VecClassSelect { responseFilter() { super("source"); } }

    @Override protected void registered(API_VERSION ver) {
      super.registered(ver);
      Argument c = find("ignored_cols");
      Argument r = find("response");
      int ci = _arguments.indexOf(c);
      int ri = _arguments.indexOf(r);
      _arguments.set(ri, c);
      _arguments.set(ci, r);
      ((FrameKeyMultiVec) c).ignoreVec((FrameKeyVec)r);
    }

    /**
     * Annotate the name of the response column in the job parameter JSON
     * @return JsonObject annotated with the name of the response column
     */
    @Override public JsonObject toJSON() {
      JsonObject jo = super.toJSON();
      if (source!=null) {
        int idx = source.find(response);
        if( idx == -1 ) {
          Vec vm = response.masterVec();
          if( vm != null ) idx = source.find(vm);
        }
        jo.getAsJsonObject("response").add("name", new JsonPrimitive(idx == -1 ? "null" : source._names[idx]));
      }
      return jo;
    }

    @Override protected void init() {
      super.init();
      // Check if it make sense to build a model
      if (source.numRows()==0)
        throw new H2OIllegalArgumentException(find("source"), "Cannot build a model on empty dataset!");
      // Does not alter the Response to an Enum column if Classification is
      // asked for: instead use the classification flag to decide between
      // classification or regression.
      Vec[] vecs = source.vecs();
      for( int i = cols.length - 1; i >= 0; i-- )
        if( vecs[cols[i]] == response )
          cols = Utils.remove(cols,i);

      final boolean has_constant_response = response.isEnum() ?
              response.domain().length <= 1 : response.min() == response.max();
      if (has_constant_response)
        throw new H2OIllegalArgumentException(find("response"), "Constant response column!");
    }
  }

  /**
   * A job producing a model.
   *
   * INPUT response column from source
   */
  public static abstract class ModelJob extends ModelJobWithoutClassificationField {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help="Do classification or regression", filter=myClassFilter.class, json = true)
    public boolean classification = true; // we need 3-state boolean: unspecified, true/false BUT we solve that by checking UI layer to see if the classification parameter was passed
    class myClassFilter extends DoClassBoolean { myClassFilter() { super("source"); } }

    @Override protected void init() {
      super.init();
      // Reject request if classification is required and response column is float
      //Argument a4class = find("classification"); // get UI control
      //String p4class = input("classification");  // get value from HTTP requests
      // if there is UI control and classification field was passed
      final boolean classificationFieldSpecified = true; // ROLLBACK: a4class!=null ? p4class!=null : /* we are not in UI so expect that parameter is specified correctly */ true;
      if (!classificationFieldSpecified) { // can happen if a client sends a request which does not specify classification parameter
        classification =  response.isEnum();
        Log.warn("Classification field is not specified - deriving according to response! The classification field set to " + classification);
      } else {
        if ( classification && response.isFloat()) throw new H2OIllegalArgumentException(find("classification"), "Requested classification on float column!");
        if (!classification && response.isEnum() ) throw new H2OIllegalArgumentException(find("classification"), "Requested regression on enum column!");
      }
    }
  }

  /**
   * A job producing a model that has no notion of Classification or Regression.
   *
   * INPUT response column from source
   */
  public static abstract class ModelJobWithoutClassificationField extends ColumnsResJob {
    // This exists to support GLM2, which determines classification/regression using the
    // family field, not a second separate field.
  }

  /**
   * Job which produces model and validate it on a given dataset.
   * INPUT validation frame
   */
  public static abstract class ValidatedJob extends ModelJob {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    protected transient Vec[] _train, _valid;
    /** Validation vector extracted from validation frame. */
    protected transient Vec _validResponse;
    /** Validation response domain or null if validation is not specified or null if response is float. */
    protected transient String[] _validResponseDomain;
    /** Source response domain or null if response is float. */
    protected transient String[] _sourceResponseDomain;
    /** CM domain derived from {@link #_validResponseDomain} and {@link #_sourceResponseDomain}. */
    protected transient String[] _cmDomain;
    /** Names of columns */
    protected transient String[] _names;
    /** Name of validation response. Should be same as source response. */
    public transient String _responseName;

    /** Adapted validation frame to a computed model. */
    private transient Frame _adaptedValidation;
    private transient Vec   _adaptedValidationResponse; // Validation response adapted to computed CM domain
    private transient int[][] _fromModel2CM;            // Transformation for model response to common CM domain
    private transient int[][] _fromValid2CM;            // Transformation for validation response to common CM domain

    @API(help = "Validation frame", filter = Default.class, mustExist = true, json = true)
    public Frame validation;

    @API(help = "Number of folds for cross-validation (if no validation data is specified)", filter = Default.class, json = true)
    public int n_folds = 0;

    @API(help = "Fraction of training data (from end) to hold out for validation (if no validation data is specified)", filter = Default.class, json = true)
    public float holdout_fraction = 0;

    @API(help = "Keep cross-validation dataset splits", filter = Default.class, json = true)
    public boolean keep_cross_validation_splits = false;

    @API(help = "Cross-validation models", json = true)
    public Key[] xval_models;

    public int _cv_count = 0;

    /**
     * Helper to compute the actual progress if we're doing cross-validation.
     * This method is supposed to be called by the progress() implementation for CV-capable algos.
     * @param p Progress reported by the main job
     * @return actual progress if CV is done, otherwise returns p
     */
    public float cv_progress(float p) {
      if (n_folds >= 2) {
        return (p + _cv_count) / (n_folds + 1); //divide by 1 more to account for final scoring as extra work
      }
      return p;
    }

    /**
     * Helper to specify which arguments trigger a refresh on change
     * @param ver
     */
    @Override
    protected void registered(RequestServer.API_VERSION ver) {
      super.registered(ver);
      for (Argument arg : _arguments) {
        if ( arg._name.equals("validation")) {
          arg.setRefreshOnChange();
        }
      }
    }

    /**
     * Helper to handle arguments based on existing input values
     * @param arg
     * @param inputArgs
     */
    @Override protected void queryArgumentValueSet(Argument arg, java.util.Properties inputArgs) {
      super.queryArgumentValueSet(arg, inputArgs);
      if (arg._name.equals("n_folds") && validation != null) {
        arg.disable("Only if no validation dataset is provided.");
        n_folds = 0;
      }
    }

    /**
     * Cross-Validate this Job (to be overridden for each instance, which also calls genericCrossValidation)
     * @param splits Frames containing train/test splits
     * @param cv_preds Store the predictions for each cross-validation run
     * @param offsets Array to store the offsets of starting row indices for each cross-validation run
     * @param i Which fold of cross-validation to perform
     */
    public void crossValidate(Frame[] splits, Frame[] cv_preds, long[] offsets, int i) { throw H2O.unimpl(); }

    /**
     * Helper to perform the generic part of cross validation
     * Expected to be called from each specific instance's crossValidate method
     * @param splits Frames containing train/test splits
     * @param offsets Array to store the offsets of starting row indices for each cross-validation run
     * @param i Which fold of cross-validation to perform
     */
    final protected void genericCrossValidation(Frame[] splits, long[] offsets, int i) {
      int respidx = source.find(_responseName);
      assert(respidx != -1) : "response is not found in source!";
      job_key = Key.make(job_key.toString() + "_xval" + i); //make a new Job for CV
      assert(xval_models != null);
      destination_key = xval_models[i];
      source = splits[0];
      validation = splits[1];
      response = source.vecs()[respidx];
      n_folds = 0;
      state = Job.JobState.CREATED; //Hack to allow this job to run
      DKV.put(self(), this); //Needed to pass the Job.isRunning(cvdl.self()) check in FrameTask
      offsets[i + 1] = offsets[i] + validation.numRows();
      _cv = true; //Hack to allow init() to pass for ColumnsJob (allow cols/ignored_cols to co-exist)
      invoke();
    }

    /**
     * Annotate the number of columns and rows of the validation data set in the job parameter JSON
     * @return JsonObject annotated with num_cols and num_rows of the validation data set
     */
    @Override public JsonObject toJSON() {
      JsonObject jo = super.toJSON();
      if (validation != null) {
        jo.getAsJsonObject("validation").addProperty("num_cols", validation.numCols());
        jo.getAsJsonObject("validation").addProperty("num_rows", validation.numRows());
      }
      return jo;
    }

    @Override protected void init() {
      if ( validation != null && n_folds != 0 ) throw new UnsupportedOperationException("Cannot specify a validation dataset and non-zero number of cross-validation folds.");
      if ( n_folds < 0 ) throw new UnsupportedOperationException("The number of cross-validation folds must be >= 0.");
      super.init();
      xval_models = new Key[n_folds];
      for (int i=0; i<xval_models.length; ++i)
        xval_models[i] = Key.make(dest().toString() + "_xval" + i);

      int rIndex = 0;
      for( int i = 0; i < source.vecs().length; i++ )
        if( source.vecs()[i] == response ) {
          rIndex = i;
          break;
        }
      _responseName = source._names != null && rIndex >= 0 ? source._names[rIndex] : "response";

      if (holdout_fraction > 0) {
        if (holdout_fraction >= 1)
          throw new IllegalArgumentException("Holdout fraction must be less than 1.");
        if (validation != null)
          throw new IllegalArgumentException("Cannot specify both a holdout fraction and a validation frame.");
        if (n_folds != 0)
          throw new IllegalArgumentException("Cannot specify both a holdout fraction and a n-fold cross-validation.");

        Log.info("Holding out last " + Utils.formatPct(holdout_fraction) + " of training data.");
        FrameSplitter fs = new FrameSplitter(source, new float[]{1 - holdout_fraction});
        H2O.submitTask(fs).join();
        Frame[] splits = fs.getResult();
        source = splits[0];
        response = source.vecs()[rIndex];
        validation = splits[1];
        Log.warn("Allocating data split frames: " + source._key.toString() + " and " + validation._key.toString());
        Log.warn("Both will be kept after the the model is trained. It's the user's responsibility to manage their lifetime.");
      }

      _train = selectVecs(source);
      _names = new String[cols.length];
      for( int i = 0; i < cols.length; i++ )
        _names[i] = source._names[cols[i]];

      // Compute source response domain
      if (classification) _sourceResponseDomain = getVectorDomain(response);
      // Is validation specified?
      if( validation != null ) {
        // Extract a validation response
        int idx = validation.find(source.names()[rIndex]);
        if( idx == -1 ) throw new IllegalArgumentException("Validation set does not have a response column called "+_responseName);
        _validResponse = validation.vecs()[idx];
        // Compute output confusion matrix domain for classification:
        // - if validation dataset is specified then CM domain is union of train and validation response domains
        //   else it is only domain of response column.
        if (classification) {
          _validResponseDomain  = getVectorDomain(_validResponse);
          if (_validResponseDomain!=null) {
            _cmDomain = Utils.domainUnion(_sourceResponseDomain, _validResponseDomain);
            if (!Arrays.deepEquals(_sourceResponseDomain, _validResponseDomain)) {
              _fromModel2CM = Model.getDomainMapping(_cmDomain, _sourceResponseDomain, false); // transformation from model produced response ~> cmDomain
              _fromValid2CM = Model.getDomainMapping(_cmDomain, _validResponseDomain , false); // transformation from validation response domain ~> cmDomain
            }
          } else _cmDomain = _sourceResponseDomain;
        } /* end of if classification */
      } else if (classification) _cmDomain = _sourceResponseDomain;
    }

    protected String[] getVectorDomain(final Vec v) {
      assert v==null || v.isInt() || v.isEnum() : "Cannot get vector domain!";
      if (v==null) return null;
      String[] r;
      if (v.isEnum()) {
        r = v.domain();
      } else {
        Vec tmp = v.toEnum();
        r = tmp.domain();
        UKV.remove(tmp._key);
      }
      return r;
    }

    /** Returns true if the job has specified validation dataset. */
    protected final boolean  hasValidation() { return validation!=null; }
    /** Returns a domain for confusion matrix. */
    protected final String[] getCMDomain() { return _cmDomain; }
    /** Return validation dataset which can be adapted to a model if it is necessary. */
    protected final Frame    getValidation() { return _adaptedValidation!=null ? _adaptedValidation : validation; };
    /** Returns original validation dataset. */
    protected final Frame    getOrigValidation() { return validation; }
    public final Response2CMAdaptor getValidAdaptor() { return new Response2CMAdaptor(); }

    /** */
    protected final void prepareValidationWithModel(final Model model) {
      if (validation == null) return;
      Frame[] av = model.adapt(validation, false);
      _adaptedValidation = av[0];
      gtrash(av[1]); // delete this after computation
      if (_fromValid2CM!=null) {
        assert classification : "Validation response transformation should be declared only for classification!";
        assert _fromModel2CM != null : "Model response transformation should exist if validation response transformation exists!";
        Vec tmp = _validResponse.toEnum();
        _adaptedValidationResponse = tmp.makeTransf(_fromValid2CM, getCMDomain()); // Add an original response adapted to CM domain
        gtrash(_adaptedValidationResponse); // Add the created vector to a clean-up list
        gtrash(tmp);
      }
    }

    /** A micro helper for transforming model/validation responses to confusion matrix domain. */
    public class Response2CMAdaptor {
      /** Adapt given vector produced by a model to confusion matrix domain. Always return a new vector which needs to be deleted. */
      public Vec adaptModelResponse2CM(final Vec v) { return  v.makeTransf(_fromModel2CM, getCMDomain()); }
      /** Adapt given validation vector to confusion matrix domain. Always return a new vector which needs to be deleted. */
      public Vec adaptValidResponse2CM(final Vec v) { return  v.makeTransf(_fromValid2CM, getCMDomain()); }
      /** Returns validation dataset. */
      public Frame getValidation() { return ValidatedJob.this.getValidation(); }
      /** Return cached validation response already adapted to CM domain. */
      public Vec getAdaptedValidationResponse2CM() { return _adaptedValidationResponse; }
      /** Return cm domain. */
      public String[] getCMDomain() { return ValidatedJob.this.getCMDomain(); }
      /** Returns true if model/validation responses need to be adapted to confusion matrix domain. */
      public boolean needsAdaptation2CM() { return _fromModel2CM != null; }
      /** Return the adapted response name */
      public String adaptedValidationResponse(final String response) { return response + ".adapted"; }
    }
  }

  /**
   *
   */
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

  public static final class List extends Iced {
    Key[] _jobs = new Key[0];

    @Override
    public List clone(){
      List l = new List();
      l._jobs = _jobs.clone();
      for(int i = 0; i < l._jobs.length; ++i)
        l._jobs[i] = (Key)l._jobs[i].clone();
      return l;
    }
  }

  /** Almost lightweight job handle containing the same content
   * as pure Job class.
   */
  public static class JobHandle extends Job {
    public JobHandle(final Job job) { super(job); }
  }
  public static class JobCancelledException extends RuntimeException {
    public JobCancelledException(){super("job was cancelled!");}
    public JobCancelledException(String msg){super("job was cancelled! with msg '" + msg + "'");}
  }

  /** Hygienic method to prevent accidental capture of non desired values. */
  public static <T extends FrameJob> T hygiene(T job) {
    job.source = null;
    return job;
  }

  public static <T extends ValidatedJob> T hygiene(T job) {
    job.source = null;
    job.validation = null;
    return job;
  }
}
