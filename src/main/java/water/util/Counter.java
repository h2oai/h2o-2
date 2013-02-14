package water.util;

import water.api.Constants;

import com.google.gson.JsonObject;

public class Counter {
  double _min = Double.MAX_VALUE, _max = Double.MIN_VALUE;
  int    _count;
  double _total;
  public void add(double what) {
    _total += what;
    _min = Math.min(what, _min);
    _max = Math.max(what, _max);
    ++_count;
  }
  public double mean() { return _total / _count; }
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    json.addProperty(Constants.MIN,  _min);
    json.addProperty(Constants.MEAN, mean());
    json.addProperty(Constants.MAX,  _max);
    return json;
  }
  @Override public String toString() {
    return _count==0 ? " / / " : String.format("%4.1f / %4.1f / %4.1f", _min, mean(), _max);
  }
}