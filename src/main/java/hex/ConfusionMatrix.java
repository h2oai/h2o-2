package hex;

import java.util.Arrays;

import water.Iced;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

public final class ConfusionMatrix extends Iced {
  public long [][] _arr;
  long _n;
  double _threshold;

  public ConfusionMatrix clone(){
    ConfusionMatrix res = new ConfusionMatrix(0);
    res._arr = _arr.clone();
    for(int i = 0; i < _arr.length; ++i)
      res._arr[i] = _arr[i].clone();
    res._n = _n;
    res._threshold = _threshold;
    return res;
  }
  public enum ErrMetric {
    MAXC,
    SUMC,
    TOTAL;

    public double computeErr(ConfusionMatrix cm){
      double [] cerr = cm.classErr();
      double res = 0;
      switch(this){
      case MAXC:
         res = cerr[0];
        for(double d:cerr)if(d > res)res = d;
        break;
      case SUMC:
        for(double d:cerr)res += d;
        break;
      case TOTAL:
        res = cm.err();
        break;
      default:
        throw new Error("unexpected err metric " + this);
      }
      return res;
    }

  }


  public ConfusionMatrix(int n){
    _arr = new long[n][n];
  }
  public void add(int i, int j){
    add(i, j, 1);
  }
  public void add(int i, int j, int c){
    _arr[i][j] += c;
    _n += c;
  }

  public final double [] classErr(){
    double [] res = new double[_arr.length];
    for(int i = 0; i < res.length; ++i)
      res[i] = classErr(i);
    return res;
  }
  public final int size() {return _arr.length;}

  public final double classErr(int c){
    long s = 0;
    for( long x : _arr[c] )
      s += x;
    if( s==0 ) return 0.0;    // Either 0 or NaN, but 0 is nicer
    return (double)(s-_arr[c][c])/s;
  }

  public double err(){
    long err = _n;
    for(int i = 0; i < _arr.length;++i){
      err -= _arr[i][i];
    }
    return (double)err/_n;
  }

  public void add(ConfusionMatrix other){
    _n += other._n;
    for(int i = 0; i < _arr.length; ++i)
      for(int j = 0; j < _arr.length; ++j)
        _arr[i][j] += other._arr[i][j];
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    for(long[] r:_arr)
      sb.append(Arrays.toString(r) + "\n");
    return sb.toString();
  }
  public JsonArray toJson(){
    JsonArray res = new JsonArray();
    JsonArray header = new JsonArray();
    header.add(new JsonPrimitive("Actual / Predicted"));
    for(int i = 0; i < _arr.length;++i)
      header.add(new JsonPrimitive("class " + i));
    header.add(new JsonPrimitive("Error"));
    res.add(header);
    for(int i = 0; i < _arr.length; ++i){
      JsonArray row = new JsonArray();
      row.add(new JsonPrimitive("class " + i));
      long s = 0;
      for(int j = 0; j < _arr.length; ++j){
        s += _arr[i][j];
        row.add(new JsonPrimitive(_arr[i][j]));
      }
      double err = s - _arr[i][i];
      err /= s;
      row.add(new JsonPrimitive(err));
      res.add(row);
    }
    JsonArray totals = new JsonArray();
    totals.add(new JsonPrimitive("Totals"));
    long S = 0;
    long DS = 0;
    for(int i = 0; i < _arr.length; ++i){
      long s = 0;
      for(int j = 0; j < _arr.length; ++j)
        s += _arr[j][i];
      totals.add(new JsonPrimitive(s));
      S += s;
      DS += _arr[i][i];
    }
    double err = (S - DS)/(double)S;
    totals.add(new JsonPrimitive(err));
    res.add(totals);
    return res;
  }
}
