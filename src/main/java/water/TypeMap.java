package water;

import java.util.Arrays;

import com.google.common.base.Throwables;

public class TypeMap {
  public static final TypeMap MAP = new TypeMap();
  private static final int INIT_MAP_SIZE = MAP._examplars.length;

  private volatile Freezable[] _examplars = new Freezable[] {
      new FillRequest(null, -1),
      new H2ONode(),
      new HeartBeat(),
  };

  /** Remove my list of exemplars.  I will resync to the new leader over time.
   */
  public void changeLeader() {
    // Better not have handed out any types; new leader will be out of sync
    assert _examplars.length == INIT_MAP_SIZE;
  }

  /**
   * Get a Freezable for deserialization.  Comes pre-cloned.
   */
  public Freezable getType(int id) {
    Freezable f = getTypeImpl(id);
    if( f == null ) f = fill(id);
    return f.newInstance();
  }

  public int getId(Freezable f) {
    int i = getIdImpl(f);
    if( i < 0 ) i = fill(f);
    return i;
  }

  private Freezable getTypeImpl(int id) {
    if( id >= _examplars.length ) return null;
    return _examplars[id];
  }

  private int getIdImpl(Freezable f) {
    String target = f.getClass().getName();
    for( int i = 0; i < _examplars.length; ++i ) {
      if( _examplars[i] == null ) continue;
      if( _examplars[i].getClass().getName().equals(target) ) return i;
    }
    return -1;
  }

  private int fill(Freezable f) {
    String clazz = f.getClass().getName();
    if( H2O.CLOUD.leader() == H2O.SELF ) return registerType(clazz);
    FillRequest fr = new FillRequest(clazz, -1);
    return RPC.call(H2O.CLOUD.leader(), fr).get()._index;
  }

  private Freezable fill(int i) {
    assert H2O.CLOUD.leader() != H2O.SELF : "LEADER should contain the superset of all type maps";
    FillRequest fr = new FillRequest(null, i);
    return getTypeImpl(RPC.call(H2O.CLOUD.leader(), fr).get()._index);
  }

  private void recordType(String clazz, int index) {
    assert H2O.CLOUD.leader() != H2O.SELF;
    Freezable f = getTypeImpl(index);
    if( f != null ) {
      assert f.getClass().getName().equals(clazz);
      return;
    }

    synchronized( this ) {
      if( index >= _examplars.length )
        _examplars = Arrays.copyOf(_examplars, index+1);
      _examplars[index] = alloc(clazz);
    }
  }

  private int registerType(String clazz) {
    assert H2O.CLOUD.leader() == H2O.SELF;
    synchronized( this ) {
      int i = _examplars.length;
      assert i<65535; // Cap at 2 bytes for shorter UDP packets & Timeline recording
      _examplars = Arrays.copyOf(_examplars, i+1);
      _examplars[i] = alloc(clazz);
      return i;
    }
  }

  private static Freezable alloc(String clazz) {
    if( clazz == null ) return null;
    try {
      return (Freezable) Class.forName(clazz).newInstance();
    } catch( Exception e ) {
      throw Throwables.propagate(e);
    }
  }

  private static class FillRequest extends DTask<FillRequest> {
    String _clazz;
    int _index;

    public FillRequest(String clazz, int index) {
      _clazz = clazz;
      _index = index;
    }

    @Override public void compute() { throw H2O.unimpl(); }
    @Override public FillRequest invoke(H2ONode sender) {
      if( _clazz == null ) {
        int i = _index;
        assert 0 <= i && i < MAP._examplars.length;
        Freezable f = MAP.getTypeImpl(i);
        _clazz = f.getClass().getName();
        return this;
      }

      Freezable f = alloc(_clazz);
      int i = MAP.getIdImpl(f);
      if( i < 0 ) i = MAP.registerType(_clazz);
      _index = i;
      return this;
    }

    @Override public void onAck() { MAP.recordType(_clazz, _index); }
    @Override public boolean isHighPriority() { return true; }
  }
}
