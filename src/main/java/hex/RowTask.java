package hex;

import java.util.Arrays;

import water.*;
import water.ValueArray.Column;

public abstract class RowTask<T extends Freezable> extends MRTask {
  final ValueArray _ary;
  private transient int _rid;
  private transient AutoBuffer _bits;
  private T _res;

  public RowTask(ValueArray ary){_ary = ary;}

  public static final class Row<T extends Freezable> {
    transient final RowTask<T> _tsk;
    public Row(RowTask<T> tsk){_tsk = tsk;}
    public long getICol(int cidx){return _tsk._ary.data(_tsk._bits, _tsk._rid, cidx);}
    public double getDCol(int cidx){return _tsk._ary.datad(_tsk._bits, _tsk._rid, cidx);}
    public Column getColInfo(int cidx){return _tsk._ary._cols[cidx];}

    public void getDCols(int [] cols, double [] vals){
      int j = 0;
      for(int c:cols)
        vals[j++] = getDCol(c);
    }
    public void getICols(int [] cols, long [] vals){
      int j = 0;
      for(int c:cols)
        vals[j++] = getICol(c);
    }
    public boolean isNA(int c){return _tsk._ary.isNA(_tsk._bits, _tsk._rid, c);}
  }

  public static abstract class RowFunction<T extends Iced> extends Iced {
    public abstract void processRow(Row r, T res);
  }
  public abstract static class RowFilter extends Iced {
    public abstract boolean useRow(Row r);
  }

  RowFilter [] _filters = {};

  public void addFilter(RowFilter r){
    int n = _filters.length;
    _filters = Arrays.copyOf(_filters, n+1);
    _filters[n] = r;
  }

  @Override public void map(Key key) {
    _bits = new AutoBuffer(DKV.get(key).memOrLoad());
    final int nrows = _bits.remaining()/_ary.rowSize();
    final Row r = new Row(this);
    _res = newRes();
    for(_rid = 0; _rid < nrows; ++_rid){
      if(_filters != null)
        for(RowFilter f:_filters) if(!f.useRow(r))
          continue;
      map(r,_res);
    }
  }

  public abstract T newRes();
  public abstract void map(Row r, T t);
  public abstract T reduce(T left, T right);

  @Override public void reduce(DRemoteTask drt) {
    RowTask<T> other = (RowTask<T>)drt;
    if(_res == null)
      _res = other._res;
    else
      _res = reduce(_res, other._res);
  }

  public T result(){return _res;}
}
