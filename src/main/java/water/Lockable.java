package water;

import java.util.Arrays;
import water.api.DocGen;
import water.api.Request.API;

/**
 * Lockable Keys - locked during long running jobs, to prevent overwriting
 * in-use keys.  e.g. model-building: expected to read-lock input ValueArray &
 * Frames, and write-lock the output Model.  Parser should write-lock the
 * output VA/Frame, to guard against double-parsing.
 *
 * Supports:
 *   lock-and-delete-old-and-update (for new Keys)
 *   lock-and-delete                (for removing old Keys)
 *   unlock
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
  public T delete_and_lock( Key job_key ) {
    System.out.println("clear&Locking "+_key+" by "+job_key);
    Value val = DKV.get(_key);
    Lockable old = val==null?null:(Lockable)val.get();
    assert old==null || !old.is_locked(job_key) : "Key "+_key+" already locked by "+job_key+", lks="+Arrays.toString(old._lockers);
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
    @Override public Lockable atomic(Lockable old) {
      if( old != null ) {                // Prior Lockable exists?
        assert !old.is_locked(_job_key) : "Key "+_key+" already locked by "+_job_key+", lks="+Arrays.toString(old._lockers); // No double locking by same job
        if( !old.is_unlocked() ) // Blocking for some other Job to finish???
          throw H2O.unimpl();
      }
      // Update-in-place the defensive copy
      set_write_lock(_job_key);
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

  // -----------
  // Atomic lock & remove self.
  public void delete( ) { 
    if( _key != null ) System.out.println("Lock/Deleting "+_key);
    WriteDeleteLock wl = new WriteDeleteLock();
    if( _key==null ) wl.onSuccess(this); // No key, so a local Frame only
    else wl.invoke(_key);                // Remote/global delete
  }
  public static void delete( Key k ) {
    if( k == null ) return;
    Value val = DKV.get(k);
    if( !val.isLockable() ) UKV.remove(k); // Simple things being deleted
    else ((Lockable)val.get()).delete();   // Lockable being deleted
  }

  // Obtain the write-lock on _key, then delete.
  private class WriteDeleteLock extends TAtomic<Lockable> {
    @Override public Lockable atomic(Lockable old) {
      if( old != null &&         // Prior Lockable exists?
          !old.is_unlocked() ) { // Blocking for some other Job to finish???
        System.out.println("lock/del fails because "+_key+" locked by : "+Arrays.toString(old._lockers));
        throw H2O.unimpl();
      }
      // Update-in-place the defensive copy
      old.set_write_lock(null);
      return old;
    }
    @Override public void onSuccess( Lockable old ) {
      if( old != null ) {
        Futures fs = new Futures();
        old.delete_impl(fs);
        if( _key != null ) DKV.remove(_key,fs); // Delete self also
        fs.blockForPending();
      }
    }
  }

  // -----------
  // Atomically get a read-lock, preventing future deletes or updates
  public void read_lock( Key job_key ) {
    System.out.println("Read-locking "+_key+" by "+job_key);
    new ReadLock(job_key).invoke(_key);
  }

  // Obtain read-lock
  private class ReadLock extends TAtomic<Lockable> {
    final Key _job_key;         // Job doing the unlocking
    ReadLock( Key job_key ) { _job_key = job_key; }
    @Override public Lockable atomic(Lockable old) {
      if( old.is_wlocked() ) 
        throw H2O.unimpl();
      old.set_read_lock(_job_key);
      return old;
    }
  }

  // -----------
  // Atomically set a new version of self
  public void update( Key job_key ) {
    System.out.println("Update "+_key);
    new Update(job_key).invoke(_key);
  }

  // Freshen 'this' and leave locked
  private class Update extends TAtomic<Lockable> {
    final Key _job_key;         // Job doing the unlocking
    Update( Key job_key ) { _job_key = job_key; }
    @Override public Lockable atomic(Lockable old) {
      assert old != null && old.is_locked(_job_key);
      _lockers = old._lockers;  // Keep lock state
      return Lockable.this;     // Freshen this
    }
  }

  // -----------
  // Atomically set a new version of self & unlock.
  public void unlock( Key job_key ) { 
    System.out.println("Unlocking "+_key+" by "+job_key);
    new Unlock(job_key).invoke(_key); 
  }

  // Freshen 'this' and unlock
  private class Unlock extends TAtomic<Lockable> {
    final Key _job_key;         // Job doing the unlocking
    Unlock( Key job_key ) { _job_key = job_key; }
    @Override public Lockable atomic(Lockable old) {
      set_unlocked(old._lockers,_job_key);
      return Lockable.this;
    }
  }

  // -----------
  // Accessers for locking state.  Minimal self-checking; primitive results.
  private boolean is_locked(Key job_key) { 
    if( _lockers==null ) return false;
    for( Key k : _lockers ) if( job_key==k || (job_key != null && k != null && job_key.equals(k)) ) return true;
    return false;
  }
  private boolean is_wlocked() { return _lockers!=null && _lockers.length==1; }
  private boolean is_unlocked() { return _lockers== null; }
  private void set_write_lock( Key job_key ) { 
    assert is_unlocked();
    _lockers=new Key[]{job_key}; 
    assert is_locked(job_key);
  }
  private void set_read_lock(Key job_key) {
    assert !is_locked(job_key); // no double locking
    assert !is_wlocked();       // not write locked
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
      assert lks[1]==job_key || job_key.equals(lks[1]);
      _lockers = null;          // So unlocked
    } else {                    // Else one of many readers
      assert lks.length>2;
      _lockers = Arrays.copyOf(lks,lks.length-1);
      int j=0;
      for( int i=1; i<lks.length; i++ )
        if( !job_key.equals(lks[i]) ) 
          _lockers[j++] = lks[i];
      assert j==lks.length-1;   // Was locked exactly once
    }
    assert !is_locked(job_key);
  }


  // Remove any subparts before removing the whole thing
  protected abstract void delete_impl( Futures fs );
}
