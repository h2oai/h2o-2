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

  /** Write-locker job  is  in _jobs[0 ].  Can be null locker.
   *  Read -locker jobs are in _jobs[1+].
   *  Unlocked has _jobs equal to null.
   *  Only 1 situation will be true at a time; atomically updated.
   *  Transient, because this data is only valid on the master node.
   */
  @API(help="Jobs locking this key")
  public transient Key _lockers[];

  // Create unlocked
  public Lockable( Key key ) { _key = key; }

  // Atomic create+overwrite of prior key.
  // If prior key exists, block until acquire a write-lock.
  // The call delete_impl, removing all of a prior key.
  // The replace this object as the new Lockable, still write-locked.
  // "locker" can be null, meaning the special no-Job locker; for use by expected-fast operations
  //
  // Example: write-lock & remove an old VA, and replace with a new locked Frame
  //     Local-Node                              Master-Node
  // (1)  FR,VA      -->write_lock(job)-->          VA
  // (2)  FR,VA.waiting...                       FR,VA+job-locked atomic xtn loop
  // (3)                                            VA.delete_impl onSuccess
  // (4)  FR         <--update success <--       FR+job-locked

  public T delete_and_lock( Job locker ) {
    // Atomically acquire lock
    assert _key != null;        // Must have a Key to be lockable (not all Frames ever exist in the K/V)
    System.out.println("Locking "+_key+" by "+locker);
    Key job_key = locker == null ? null : locker.job_key;
    new WriteUpdateLock(job_key).invoke(_key);
    return (T)this;
  }

  // Obtain the write-lock on _key, deleting the prior _key.
  // Blocks locally waiting on the master node to resolve.
  // Blocks on master, until the lock can be obtained (burning a F/J till lock acquire).
  // Spins, if several requests are competing for a lock.
  private class WriteUpdateLock extends TAtomic<Lockable> {
    final Key _job_key;         // Job doing the locking
    WriteUpdateLock( Key job_key ) { _job_key = job_key; }
    // This code runs on master, in a loop until the returned value is
    // atomically updated over the Lockable's _key.
    @Override public Lockable atomic(Lockable old) {
      // "old" is a private defensive copy of the old Lockable - no other
      // writers.  We can read it without concern for races.
      if( old != null ) {                // Prior Lockable exists?
        assert !old.is_locked(_job_key); // No double locking by same job
        if( !old.is_unlocked() ) // Blocking for some other Job to finish???
          throw H2O.unimpl();
      }
      // Update-in-place the defensive copy
      assert is_unlocked();
      set_write_lock(_job_key);
      assert is_locked(_job_key);
      return Lockable.this;
    }
    @Override public void onSuccess( Lockable old ) {
      if( old != null ) {
        Futures fs = new Futures();
        old.delete_impl(fs);
        fs.blockForPending();
      }
    }
  }


  // Atomic lock & remove self.
  public void delete( ) { new WriteDeleteLock().invoke(_key); }
  public Futures delete(Futures fs) { delete(); return fs; }
  public static void delete( Key k ) {
    if( k == null ) return;
    Value val = DKV.get(k);
    if( !val.isLockable() ) DKV.remove(k);
    else ((Lockable)val.get()).delete();
  }

  // Obtain the write-lock on _key, then delete.
  private class WriteDeleteLock extends TAtomic<Lockable> {
    // This code runs on master, in a loop until the returned value is
    // atomically updated over the Lockable's _key.
    @Override public Lockable atomic(Lockable old) {
      // "old" is a private defensive copy of the old Lockable - no other
      // writers.  We can read it without concern for races.
      if( old != null &&         // Prior Lockable exists?
          !old.is_unlocked() )   // Blocking for some other Job to finish???
        throw H2O.unimpl();
      // Update-in-place the defensive copy
      assert old.is_unlocked();
      old.set_write_lock(null);
      assert old.is_locked(null);
      return old;
    }
    @Override public void onSuccess( Lockable old ) {
      if( old != null ) {
        Futures fs = new Futures();
        old.delete_impl(fs);
        DKV.remove(_key,fs);    // Delete self also
        fs.blockForPending();
      }
    }
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
    System.out.println("Unlocking "+_key);
    return fs;
  }

  // Accessers for locking state.  No self-checking, just primitive results.
  private boolean is_locked(Key job_key) { 
    if( _lockers==null ) return false;
    for( Key k : _lockers ) if( job_key.equals(k) ) return true;
    return false;
  }
  private boolean is_unlocked() { return _lockers== null; }
  private void set_write_lock( Key job_key ) { _lockers=new Key[]{job_key}; }


  // Remove any subparts before removing the whole thing
  protected abstract void delete_impl( Futures fs );
}

