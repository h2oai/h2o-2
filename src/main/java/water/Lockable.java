package water;

import water.api.DocGen;
import water.api.Request.API;
import water.util.Log;

import java.util.Arrays;

/**
 * Lockable Keys - locked during long running jobs, to prevent overwriting
 * in-use keys.  e.g. model-building: expected to read-lock input ValueArray and
 * Frames, and write-lock the output Model.  Parser should write-lock the
 * output VA/Frame, to guard against double-parsing.
 *
 * Supports:
 *   lock-and-delete-old-and-update (for new Keys)
 *   lock-and-delete                (for removing old Keys)
 *   unlock
 *
 * @author <a href="mailto:cliffc@h2o.ai"></a>
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

  // -----------
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

  // Write-lock 'this', returns OLD guy
  public Lockable write_lock( Key job_key ) {
    Log.debug(Log.Tag.Sys.LOCKS,"write-lock "+_key+" by job "+job_key);
    return ((PriorWriteLock)new PriorWriteLock(job_key).invoke(_key))._old;
  }
  // Write-lock 'this', delete any old thing, returns NEW guy
  public T delete_and_lock( Key job_key ) {
    Lockable old =  write_lock(job_key);
    if( old != null ) {
      Log.debug(Log.Tag.Sys.LOCKS,"lock-then-clear "+_key+" by job "+job_key);
      old.delete_impl(new Futures()).blockForPending();
    }
    return (T)this;
  }


  // Obtain the write-lock on _key, which may already exist, using the current 'this'.
  private class PriorWriteLock extends TAtomic<Lockable> {
    final Key _job_key;         // Job doing the locking
    Lockable _old;              // Return the old thing, for deleting later
    PriorWriteLock( Key job_key ) { _job_key = job_key; }
    @Override public Lockable atomic(Lockable old) {
      _old = old;
      if( old != null ) {       // Prior Lockable exists?
        assert !old.is_wlocked(_job_key) : "Key "+_key+" already locked; lks="+Arrays.toString(old._lockers); // No double locking by same job
        if( old.is_locked(_job_key) ) // read-locked by self? (double-write-lock checked above)
          old.set_unlocked(old._lockers,_job_key); // Remove read-lock; will atomically upgrade to write-lock
        if( !old.is_unlocked() ) // Blocking for some other Job to finish???
          throw new IllegalArgumentException(old.errStr()+" "+_key+" is already in use.  Unable to use it now.  Consider using a different destination name.");
        assert old.is_unlocked() : "Not unlocked when locking "+Arrays.toString(old._lockers)+" for "+_job_key;
      }
      // Update & set the new value
      set_write_lock(_job_key);
      return Lockable.this;
    }
  }

  // -----------
  // Atomic lock & remove self.  Nothing remains when done.

  // Write-lock & delete 'k'.  Will fail if 'k' is locked by anybody.
  public static void delete( Key k ) { delete(k,null); }
  // Write-lock & delete 'k'.  Will fail if 'k' is locked by anybody other than 'job_key'
  public static void delete( Key k, Key job_key ) {
    if( k == null ) return;
    Value val = DKV.get(k);
    if( val == null ) return;              // Or just nothing there to delete
    if( !val.isLockable() ) UKV.remove(k); // Simple things being deleted
    else ((Lockable)val.get()).delete(job_key,0.0f); // Lockable being deleted
  }
  // Will fail if locked by anybody.
  public void delete( ) { delete(null,0.0f); }
  // Will fail if locked by anybody other than 'job_key'
  public void delete( Key job_key, float dummy ) { 
    if( _key != null ) {
      Log.debug(Log.Tag.Sys.LOCKS,"lock-then-delete "+_key+" by job "+job_key);
      new PriorWriteLock(job_key).invoke(_key);
    }
    Futures fs = new Futures();
    delete_impl(fs);
    if( _key != null ) DKV.remove(_key,fs); // Delete self also
    fs.blockForPending();
  }

  // -----------
  // Atomically get a read-lock, preventing future deletes or updates
  public static void read_lock( Key k, Key job_key ) {
    Value val = DKV.get(k);
    if( val.isLockable() )
      ((Lockable)val.get()).read_lock(job_key); // Lockable being locked
  }
  public void read_lock( Key job_key ) { 
    if( _key != null ) {
      Log.debug(Log.Tag.Sys.LOCKS,"shared-read-lock "+_key+" by job "+job_key);
      new ReadLock(job_key).invoke(_key); 
    }
  }

  // Obtain read-lock
  static private class ReadLock extends TAtomic<Lockable> {
    final Key _job_key;         // Job doing the unlocking
    ReadLock( Key job_key ) { _job_key = job_key; }
    @Override public Lockable atomic(Lockable old) {
      if( old == null ) throw new IllegalArgumentException("Nothing to lock!");
      if( old.is_wlocked() )
        throw new IllegalArgumentException( old.errStr()+" "+_key+" is being created;  Unable to read it now.");
      old.set_read_lock(_job_key);
      return old;
    }
  }

  // -----------
  // Atomically set a new version of self
  public void update( Key job_key ) { 
    Log.debug(Log.Tag.Sys.LOCKS,"update write-locked "+_key+" by job "+job_key);
    new Update(job_key).invoke(_key); 
  }

  // Freshen 'this' and leave locked
  private class Update extends TAtomic<Lockable> {
    final Key _job_key;         // Job doing the unlocking
    Update( Key job_key ) { _job_key = job_key; }
    @Override public Lockable atomic(Lockable old) {
      assert old != null && old.is_wlocked();
      _lockers = old._lockers;  // Keep lock state
      return Lockable.this;     // Freshen this
    }
  }
  public static void unlock_lockable(final Key lockable, final Key job){
    new DTask.DKeyTask<DTask.DKeyTask,Lockable>(null,lockable){
      @Override
      public void map(Lockable l) { l.unlock(job);}
    }.invokeTask();
  }
  // -----------
  // Atomically set a new version of self & unlock.
  public void unlock( Key job_key ) { 
    if( _key != null ) {
      Log.debug(Log.Tag.Sys.LOCKS,"unlock "+_key+" by job "+job_key);
      new Unlock(job_key).invoke(_key); 
    }
  }

  // Freshen 'this' and unlock
  private class Unlock extends TAtomic<Lockable> {
    final Key _job_key;         // Job doing the unlocking
    Unlock( Key job_key ) { _job_key = job_key; }
    @Override public Lockable atomic(Lockable old) {
      assert old.is_locked(_job_key) : old.getClass().getSimpleName() + " cannot be unlocked (not locked by job " + _job_key + ").";
      set_unlocked(old._lockers,_job_key);
      return Lockable.this;
    }
  }

  // -----------
  // Accessers for locking state.  Minimal self-checking; primitive results.
  private boolean is_locked(Key job_key) { 
    if( _lockers==null ) return false;
    for( int i=(_lockers.length==1?0:1); i<_lockers.length; i++ ) {
      Key k = _lockers[i];
      if( job_key==k || (job_key != null && k != null && job_key.equals(k)) ) return true;
    }
    return false;
  }
  protected boolean is_wlocked() { return _lockers!=null && _lockers.length==1; }
  private boolean is_wlocked(Key job_key) { return is_wlocked() && (_lockers[0] == job_key || _lockers[0] != null && _lockers[0].equals(job_key)); }
  protected boolean is_unlocked() { return _lockers== null; }
  private void set_write_lock( Key job_key ) { 
    _lockers=new Key[]{job_key}; 
    assert is_locked(job_key) : "Job " + job_key + " must be locked.";
  }
  private void set_read_lock(Key job_key) {
    assert !is_locked(job_key) : this.getClass().getSimpleName() + " is already locked by job " + job_key + "."; // no double locking
    assert !is_wlocked() : this.getClass().getSimpleName() + " is already write locked.";       // not write locked
    _lockers = _lockers == null ? new Key[2] : Arrays.copyOf(_lockers,_lockers.length+1);
    _lockers[_lockers.length-1] = job_key;
    assert is_locked(job_key);
  }
  private void set_unlocked(Key lks[], Key job_key) {
    if( lks.length==1 ) {       // Is write-locked?
      assert job_key==lks[0] || job_key.equals(lks[0]);
      _lockers = null;           // Then unlocked
    } else if( lks.length==2 ) { // One reader
      assert lks[0]==null;       // Not write-locked
      assert lks[1]==job_key || (job_key != null && job_key.equals(lks[1]));
      _lockers = null;          // So unlocked
    } else {                    // Else one of many readers
      assert lks.length>2;
      _lockers = Arrays.copyOf(lks,lks.length-1);
      int j=1;                  // Skip the initial null slot
      for( int i=1; i<lks.length; i++ )
        if(job_key != null && !job_key.equals(lks[i]) || (job_key == null && lks[i] != null)){
            _lockers[j++] = lks[i];
        }
      assert j==lks.length-1;   // Was locked exactly once
    }
    assert !is_locked(job_key);
  }

  // Unlock from all lockers
  public void unlock_all() {
    if( _key != null )
      for (Key k : _lockers) new UnlockSafe(k).invoke(_key);
  }

  private class UnlockSafe extends TAtomic<Lockable> {
    final Key _job_key;         // potential job doing the unlocking
    UnlockSafe( Key job_key ) { _job_key = job_key; }
    @Override public Lockable atomic(Lockable old) {
      if (old.is_locked(_job_key))
        set_unlocked(old._lockers,_job_key);
      return Lockable.this;
    }
  }

  // Remove any subparts before removing the whole thing
  protected abstract Futures delete_impl( Futures fs );
  // Pretty string when locking fails
  protected abstract String errStr();
}
