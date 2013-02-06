package hex;

import Jama.Matrix;
import java.util.Arrays;
import water.Iced;
import water.MemoryManager;

public final class GramMatrix extends Iced {
  int _n;
  private double [][] _xx;
  private double [] _xy;

  public GramMatrix(int n) {
    _n = n;
    _xx = new double [n][];
    _xy = MemoryManager.malloc8d(n);
    for(int i = 0; i < n; ++i)
      _xx[i] = MemoryManager.malloc8d(i+1);
  }

  public void addRow(double [] x, int [] indexes, double y){
    for(int i = 0; i < x.length; ++i){
      for(int j = 0; j < x.length; ++j){
        if(indexes[j] > indexes[i])break;
        _xx[indexes[i]][indexes[j]] += x[i]*x[j];
      }
      _xy[indexes[i]] += x[i] * y;
    }

  }

  public void add(GramMatrix other){
    assert _n == other._n:"trying to merge incompatible gram matrices";
    for(int i = 0; i < _xx.length; ++i) {
      _xy[i] += other._xy[i];
      for(int j = 0; j < _xx[i].length; ++j)
        _xx[i][j] += other._xx[i][j];
    }
  }

  public Matrix getXX(){
    Matrix xx = new Matrix(_xx.length, _xx.length);
    for( int i = 0; i < _n; ++i ) {
      for( int j = 0; j < _xx[i].length; ++j ) {
          xx.set(i, j, _xx[i][j]);
          xx.set(j, i, _xx[i][j]);
      }
    }
    return xx;
  }
  public Matrix getXY(){
    return new Matrix(_xy, _xy.length);
  }

  public boolean hasNaNsOrInfs() {
    for(int i = 0; i < _xx.length; ++i){
      if(Double.isNaN(_xy[i]) || Double.isInfinite(_xy[i])) return true;
      for(double d:_xx[i])
        if(Double.isNaN(d) || Double.isInfinite(d))
          return true;
    }
    return false;
  }
  public String toString(){
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < _xx.length; ++i){
      sb.append(_xy[i] + " | ");
      sb.append(Arrays.toString(_xx[i]) + "\n");
    }
    return sb.toString();
  }

}
