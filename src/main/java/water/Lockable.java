package water;

import water.api.DocGen;
import water.api.Request.API;

/**
 * Lockable Keys - locked during long running jobs, to prevent overwriting
 * in-use keys.  e.g. model-building: expected to read-lock input ValueArray &
 * Frames, and write-lock the output Model.  Parser should write-lock the
 * output VA/Frame, to guard against double-parsing.
 *
 * Supports:
 *   read_lock      - block until acquire a shared read-lock
 *   try_read_lock  - attempt lock; return false if write-locked after timeout
 *   write_lock     - block until acquire a exclusive write-lock
 *   try_write_lock - attempt lock; return false if read-locked after timeout
 *   unlock         - unlock prior lock; error if not locked
 *   lockers        - Return a list of locking jobs
 *   delete         - block until obtain a write-lock; then delete_impl()
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public abstract class Lockable<T extends Lockable<T>> extends Iced {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  /** The Key being locked */
  @API(help="My Key")
  public final Key _key;

  ///** Write-locker job  is  in _jobs[0 ].
  // *  Read -locker jobs are in _jobs[1+].
  // *  Unlocked has _jobs equal to null.
  // *  Only 1 situation will be true at a time; atomically updated.
  // */
  //@API(help="Jobs locking this key")
  //public Key _lockers[];
  //
  // Create unlocked
  public Lockable( Key key ) { _key = key; }
  //// Create locked by Job
  //public Lockable( Key key, Job locker ) { _key = key; _lockers = new Key[]{locker._job_key}; }
  //
  //
  //// Utility class to ask-state or set-state.
  //// NO ERROR CHECKING, just a primitive abstraction layer.
  //static private abstract class LockImpl extends TAtomic<Lockable> {
  //  final Key _job_key;         // Job doing the locking
  //  Key _lockers[];             // Job state on completion
  //  LockImpl( Key job_key ) { _job_key = job_key; }
  //  boolean is_write_locked() { return _lockers!= null && _lockers[0]==_job_key; }
  //  boolean is_unlocked    () { return _lockers== null; }
  //  Key[] set_write_lock( Key job_key ) { return _lockers=new Key[]{_job_key}; }
  //}

  // Atomic create+overwrite of prior key.
  // If prior key exists, block until acquire a write-lock.
  // The call delete_impl, removing all of a prior key.
  // The replace this object as the new Lockable, still write-locked.
  // "locker" can be null, meaning the special no-Job locker; mostly used by fast tests
  public T delete_and_lock( Job locker ) {
    // Atomically acquire lock
    assert _key != null;        // Must have a Key to be lockable (not all Frames ever exist in the K/V)
  //  Key job_key = job.job_key;
  //  WriteLock wl = (WriteLock)(new WriteLock(job_key).invoke(_key));
  //  if( wl == null )            // Aborted?
  //    throw new IllegalArgumentException("Attempting to write-lock a deleted key");
  //  assert wl.is_write_locked();

    // Default non-locking empty implementation!!!
    Futures fs = new Futures();
    Value val = DKV.get(_key);
    if( val != null )           // Prior exists?
      // Asserts that this _key only refers to Lockables its whole life
      ((T)val.get()).delete_impl(fs); // Delete prior
    DKV.put(_key,this,fs);      // Put initial copy, replacing old guy
    fs.blockForPending();
    return (T)this;
  }

  // Atomic remove self.  Trivial if no self-key.
  public void delete( ) { delete(new Futures()).blockForPending(); }
  public Futures delete( Futures fs ) {
    // Default non-locking empty implementation!!!
    delete_impl(fs);            // Nuke self
    if( _key != null ) DKV.remove(_key,fs);        // Remove self key
    return fs;
  }

  public static void delete( Key k ) {
    if( k == null ) return;
    Lockable lk = DKV.get(k).get();
    lk.delete();
  }

  // Atomically set a new version of self 
  public void update() {
    // Default non-unlocking empty implementation!!!
    // Assert locked by job
    DKV.put(_key,this);         // Just lazy freshen copy of self
  }

  // Atomically set a new version of self & unlock.
  public void unlock( ) { unlock(new Futures()).blockForPending(); }
  public Futures unlock( Futures fs ) {
    // Default non-unlocking empty implementation!!!
    DKV.put(_key,this,fs);      // Just freshen copy of self
    return fs;
  }

  //// Block until acquire a write-lock.  
  //void write_lock( Job job ) {
  //  // Atomically acquire lock
  //  Key job_key = job.job_key;
  //  WriteLock wl = (WriteLock)(new WriteLock(job_key).invoke(_key));
  //  if( wl == null )            // Aborted?
  //    throw new IllegalArgumentException("Attempting to write-lock a deleted key");
  //  assert wl.is_write_locked();
  //}
  //
  //private class WriteLock extends LockImpl {
  //  WriteLock( Key job_key ) { super(job_key); }
  //  @Override public Lockable atomic(Lockable old) {
  //    // "old" is a private defensive copy of the old Lockable - no other
  //    // writers.  We can read it without concern for races.  The outer-class
  //    // new Lockable is what is getting locked.  These might be the same.
  //    if( old == null ) return null; // Nothing to lock???
  //    _lockers = old._lockers;
  //    assert !is_write_locked(); // Double locking by same job
  //    if( !is_unlocked() )      // Blocking for some other Job to finish???
  //      throw H2O.unimpl();
  //    // Update-in-place the defensive copy
  //    old._lockers = set_write_lock(_job_key);
  //    return old;
  //  }
  //}

  // Remove any subparts before removing the whole thing
  protected abstract void delete_impl( Futures fs );
}

