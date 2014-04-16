/**
 *
 */
package water;

import water.api.DocGen;
import water.fvec.Frame;
import water.fvec.Vec;

import java.util.HashSet;

/**
 * Short-time computation which is not a job.
 */
public abstract class Func extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  /** A set containing a temporary vectors which are <strong>automatically</strong> deleted when job is done.
   *  Deletion is by {@link #cleanup()} call. */
  private transient HashSet<Key> _gVecTrash = new HashSet<Key>();
  /** Local trash which can be deleted by user call.
   * @see #emptyLTrash() */
  private transient HashSet<Key> _lVecTrash = new HashSet<Key>();

  /** Invoke this function in blocking way. */
  public void invoke() {
    init();
    exec();
  }

  /** The function implementation.
   * <p>It introduces blocking call which after finish of
   * the function performs cleanup.
   * </p><p>
   * The method should not handle exceptions which it cannot handle and should let
   * them to propagate to upper levels.
   * </p>
   */
  protected final void exec() {
    try {
      execImpl();
    } finally {
      cleanup(); // Perform job cleanup
    }
  }

  /** The real implementation which should be provided by ancestors. */
  protected void execImpl() { throw new RuntimeException("Job does not support exec call! Please implement execImpl method!"); };

  /**
   * Invoked before run. This is the place to checks arguments are valid or throw
   * IllegalArgumentException. It will get invoked both from the Web and Java APIs.
   *
   * @throws IllegalArgumentException throws the exception if initialization fails to ensure
   * correct job runtime environment.
   */
  protected void init() throws IllegalArgumentException { }

  @Override protected Response serve() {
    invoke();
    return Response.done(this);
  }

  /** Clean-up code which is executed after each {@link Job#exec()} call in any case (normal/exceptional). */
  protected void cleanup() {
    // Clean-up global list of temporary vectors
    Futures fs = new Futures();
    cleanupTrash(_gVecTrash, fs);
    cleanupTrash(_lVecTrash, fs);
    fs.blockForPending();
  }
  /** User call which empty local trash of vectors. */
  protected final void emptyLTrash() {
    if (_lVecTrash.isEmpty()) return;
    Futures fs = new Futures();
    cleanupTrash(_lVecTrash, fs);
    fs.blockForPending();
  }
  /** Append all vectors from given frame to a global clean up list.
   * If the Frame itself is in the K-V store, then trash that too.
   * @see #cleanup()
   * @see #_gVecTrash */
  protected final void gtrash(Frame fr) { gtrash(fr.vecs()); if (fr._key != null && UKV.get(fr._key) != null) _gVecTrash.add(fr._key); }
  /** Append given vector to clean up list.
   * @see #cleanup()*/
  protected final void gtrash(Vec ...vec)  { appendToTrash(_gVecTrash, vec); }
  /** Put given frame vectors into local trash which can be emptied by a user calling the {@link #emptyLTrash()} method.
   * @see #emptyLTrash() */
  protected final void ltrash(Frame fr) { ltrash(fr.vecs()); if (fr._key != null && UKV.get(fr._key) != null) _lVecTrash.add(fr._key); }
  /** Put given vectors into local trash.
   * * @see #emptyLTrash() */
  protected final void ltrash(Vec ...vec) { appendToTrash(_lVecTrash, vec); }
  /** Put given vectors into a given trash. */
  private void appendToTrash(HashSet<Key> t, Vec[] vec) {
    for (Vec v : vec) t.add(v._key);
  }
  /** Delete all vectors in given trash. */
  private void cleanupTrash(HashSet<Key> trash, Futures fs) {
    for (Key k : trash) UKV.remove(k, fs);
  }
}
