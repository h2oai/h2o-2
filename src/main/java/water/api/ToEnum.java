package water.api;

import com.google.gson.JsonObject;
import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import water.*;
import water.parser.*;
import water.util.Log;

import java.util.Arrays;

public class ToEnum extends Request {
  protected final H2OHexKey      _key       = new H2OHexKey(KEY);
  protected final Int            _col_index = new Int(COL_INDEX, -1);
  protected final Bool           _to_enum   = new Bool(TO_ENUM,true,"");

////  @Override protected void registered(RequestServer.API_VERSION version) { super.registered(version); }
//

  public static class CollectIntDomain extends MRTask<CollectIntDomain> {
    final int _col;
    int [] _dom;

    public CollectIntDomain(int col){_col = col;}

    public void map(Key k) {
      Key aryKey = ValueArray.getArrayKey(k);
      ValueArray ary = DKV.get(aryKey).get();
      int [] dom = new int[256];
      int n = 0;
      AutoBuffer ab = new AutoBuffer(DKV.get(k).getBytes());
      int nrows = ab.remaining()/ary._rowsize;
      final ValueArray.Column col = ary._cols[_col];
      for(int i = 0; i < nrows; ++i){
        if(ary.isNA(ab,i,col))continue;
        int val = (int)ary.data(ab,i,col);
        int id = Arrays.binarySearch(dom,0,n,val);
        if(id < 0){
          if(n == dom.length){
            if(n == water.parser.Enum.MAX_ENUM_SIZE)throw new RuntimeException("too many unique elements!");
            dom = Arrays.copyOf(dom,Math.min(n+(n >>1),water.parser.Enum.MAX_ENUM_SIZE));
          }
          id = -id - 1;
          for(int j = n; j > id; --j)dom[j] = dom[j-1];
            dom[id] = val;
          ++n;
        }
      }
      _dom = Arrays.copyOf(dom,n);
    }
    public void reduce(CollectIntDomain other){
      if(_dom == null)_dom = other._dom;
      else if(other._dom != null && !Arrays.equals(_dom,other._dom)){
        // merge sort the domains
        int [] res = new int[_dom.length + other._dom.length];
        Arrays.fill(res,Integer.MAX_VALUE);
        int i = 0, j = 0, k = 0;
        while(i < _dom.length && j < other._dom.length){
          if(_dom[i] < other._dom[j])res[k++] = _dom[i++];
          else if(_dom[i] == other._dom[j]){
            res[k++] = _dom[i++];j++;
          } else
            res[k++] = other._dom[j++];
        }
        assert (i == _dom.length || j == other._dom.length);
        for(int ii = i; ii < _dom.length; ++ii)
          res[k++] = _dom[ii];
        for(int jj = j; jj < other._dom.length; ++jj)
          res[k++] = other._dom[jj];
        if(k > water.parser.Enum.MAX_ENUM_SIZE)throw new RuntimeException("too many unique elements!");
        _dom = Arrays.copyOf(res,k);
      }
    }
  }

  public static class EnumIntSwapTask extends MRTask<EnumIntSwapTask> {
    final int _col;
    final int [] _dom;
    final boolean _i2e;
    final int base;

    public EnumIntSwapTask(int col, int [] dom,boolean i2e){
      _col = col; _dom = dom; _i2e = i2e;
      base = _i2e?0:dom[0];
    }

    public void map(Key k) {
      Key aryKey = ValueArray.getArrayKey(k);
      ValueArray ary = DKV.get(aryKey).get();
      final byte [] bits = DKV.get(k).getBytes();
      AutoBuffer ab = new AutoBuffer(bits);
      int nrows = ab.remaining()/ary._rowsize;
      final ValueArray.Column col = ary._cols[_col];
      int off = col._off;
      for(int i = 0; i < nrows; ++i,off += ary._rowsize){
        if(ary.isNA(ab,i,col))continue; // NA stays NA
        int val = (int)ary.data(ab,i,col);
        int id = _i2e?Arrays.binarySearch(_dom,val):_dom[val];
        assert !_i2e || (id >= 0 && id < _dom.length):"unexpected id for val = " + val + ", id = " + id + ", domain = " + Arrays.toString(_dom);
        switch(col._size){
          case 1: bits[off] = (byte)(id - base); break;
          case 2: UDP.set2(bits,off,(short)(id-base)); break;
          case 4: UDP.set4(bits,off,id-base); break;
          default: assert false;
        }
      }
      DKV.put(k,new Value(k,bits));
    }
    public void reduce(EnumIntSwapTask other){/* no reduce needed */}
  }

  @Override
  protected Response serve() {
    try {
      final int column_index = _col_index.value();
      final boolean to_enum = _to_enum.value();
      String colname =  _col_index.toString();
      Log.info("Factorizing column " + colname);
      final ValueArray ary = _key.value();
      final ValueArray.Column col = ary._cols[column_index];
      final Key key = ary._key;
//      Value v = _key.value();
//      String key = v._key.toString();
      H2O.H2OCountedCompleter fjt = new H2O.H2OCountedCompleter() {
        @Override public void compute2(){
          int [] dom;
          if(to_enum){
            CollectIntDomain domtsk = new CollectIntDomain(column_index);
            domtsk.invoke(key);
            dom = domtsk._dom;
            String [] strdom = new String[dom.length];
            for(int i = 0; i < strdom.length; ++i)
              strdom[i] = String.valueOf(dom[i]);
            col._domain = strdom;
            col._min = 0;
            col._max = strdom.length-1;
          } else {
            String [] strdom = ary._cols[column_index]._domain;
            dom = new int[strdom.length];
            for(int i = 0; i < dom.length; ++i)
              dom[i] = Integer.valueOf(strdom[i]);
            col._domain = null;
            col._min = dom[0];
            col._max = dom[dom.length-1];
          }
          EnumIntSwapTask etsk = new EnumIntSwapTask(column_index,dom,to_enum);
          etsk.invoke(key);
          col._base = to_enum?0:dom[0];
          // finally, update the header
          DKV.put(key,ary);
          tryComplete();
        }
      };
      H2O.submitTask(fjt);
      fjt.get();
      return Inspect.redirect(new JsonObject(), null, key);
    } catch( Throwable e ) {
      return Response.error(e);
    }
  }
}
