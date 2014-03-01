package hex;

import water.*;
import water.nbhm.NonBlockingHashMapLong;

import java.util.Arrays;

public class TypeChange extends MRTask<TypeChange> {
  final int _col_index;
  final boolean _to_enum;
  final ValueArray _ary;
  private long[] _cdVA;

  public TypeChange(int column_index, boolean to_enum, Key key) {
    _col_index = column_index;
    _to_enum = to_enum;
    _ary = DKV.get(key).get(); //DKV.get(ValueArray.getArrayKey(key)).get();
    if(_to_enum) {
      CollectDomainVA cd = new CollectDomainVA(_col_index, new NonBlockingHashMapLong(), _ary);
      _cdVA = cd.invoke(key).domain();
    }
  }

  @Override public void map(Key key) {
    if (!_to_enum) {
      assert _ary._cols[_col_index].isEnum();
    } else {
      assert !(_ary._cols[_col_index].isEnum() && _ary._cols[_col_index].isFloat());
    }

    ValueArray ary = DKV.get(ValueArray.getArrayKey(key)).get();
    AutoBuffer bits = ary.getChunk(key);
    byte[] rawbits = bits.buf();
    int nrows = bits.remaining()/ary.rowSize();
    int off = _ary._cols[_col_index]._off;

    for(int r = 0; r < nrows; r++) {
      if (_to_enum) {
        long l = _ary.data(bits,r,_col_index);
        long j = _cdVA[(int)l];
        switch(_ary._cols[_col_index]._size) {
          case 1:
            rawbits[off] = (byte) j;
            break;
        }
      }
      off += _ary._rowsize;
    }
    DKV.put(key, new Value(key, rawbits));
  }

  @Override
  public void reduce(TypeChange drt) {
    //To change body of implemented methods use File | Settings | File Templates.
  }
  //////////////////////////////////////////////////////////////////////////////////////

  public static class CollectDomainVA extends MRTask<CollectDomainVA> {
    transient NonBlockingHashMapLong<Object> _uniques;
    private int _col_index;
    final ValueArray _ary;

    public CollectDomainVA(int col_index, NonBlockingHashMapLong<Object> uniques, ValueArray ary) {
      _uniques = uniques;
      _col_index = col_index;
      _ary = ary;
    }

    @Override public void map(Key key) {
//      int off = _ary._cols[_col_index]._off;
      ValueArray ary = DKV.get(ValueArray.getArrayKey(key)).get();
      AutoBuffer bits = ary.getChunk(key);
      int nrows = bits.remaining()/ary.rowSize();

      for(int r = 0; r < nrows; r++)
        if(!_ary.isNA(bits, r,_col_index)){
          long l = _ary.data(bits, r, _col_index);
          _uniques.put(l, "");
        }
    }

    @Override
    public void reduce(CollectDomainVA drt) {
      if(_uniques == drt._uniques) return;
      _uniques.putAll(drt._uniques);
    }

    public long[] domain(){
      long[] dom = _uniques.keySetLong();
      Arrays.sort(dom);
      return dom;
    }

    public NonBlockingHashMapLong hashmap() {
      return _uniques;
    }
  }
}