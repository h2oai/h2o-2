package hex;

import water.H2O.H2OCountedCompleter;
import water.MRTask2;
import water.MemoryManager;
import water.fvec.*;

import java.util.Arrays;

/**
 * Created by tomasnykodym on 11/25/14.
 */
public class OrderTsk extends MRTask2<OrderTsk> {
  final int _n;
  final boolean _rev;

  public long [] _ids;
  double [] _vals;

  public OrderTsk(H2OCountedCompleter cmp, int n, boolean rev) {
    super(cmp);
    _n = n; _rev = rev;
  }

  private boolean addVal(long id, double val) {
    int x = _n-1;
    if(_vals[x] > val) {
      _vals[x] = val;
      _ids[x] = id;
      while (x > 0 && _vals[x - 1] > _vals[x]) {
        double v = _vals[x - 1];
        _vals[x - 1] = _vals[x];
        _vals[x] = v;
        long l = _ids[x - 1];
        _ids[x - 1] = _ids[x];
        _ids[x] = l;
        --x;
      }
    }
    return x < _n;
  }
  @Override public void map(Chunk c) {
    _ids = MemoryManager.malloc8(_n);
    _vals = MemoryManager.malloc8d(_n);
    Arrays.fill(_vals,Double.POSITIVE_INFINITY);
    Arrays.fill(_ids,-1);
    for(int i = c.nextNZ(-1); i < c._len; i = c.nextNZ(i))
      addVal(i + c._start,_rev?-c.at0(i):c.at0(i));
  }

  @Override public void reduce(OrderTsk ot) {
    int i = 0;
    while(i < _n && addVal(ot._ids[i],ot._vals[i])) ++i;
  }

  @Override public void postGlobal(){
    if(_rev)
      for(int i = 0; i < _vals.length; ++i)
        _vals[i] *= -1;
  }

}
