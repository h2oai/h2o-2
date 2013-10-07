package hex;

import com.amazonaws.util.json.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import water.*;
import water.fvec.*;

public class SummaryTask2 extends MRTask2<SummaryTask2> {
  public Summary2[] _summaries = null;
  public SummaryTask2(Frame fr) {
    _fr = fr;
  }
  public Summary2[] get_summaries () {
    if (_summaries == null) {
      _summaries = new Summary2[_fr.vecs().length];
      for (int i = 0; i < _summaries.length; i++)
        _summaries[i] = new Summary2(_fr.vecs()[i], _fr.names()[i], null);

      doAll(_fr);
    }
    return _summaries;
  }

  public static JsonObject get_json_summaries (Summary2[] summaries) {
    JsonObject res = new JsonObject();
    JsonArray sums = new JsonArray();

    for (Summary2 s: summaries)
      sums.add(s.toJson());
    res.add("columns", sums);
    return res;
  }

  public void map(Chunk[] cs) {
    assert cs.length == _summaries.length;
    for (int i = 0; i < cs.length; i++)
      _summaries[i].add(cs[i]);
  }
  public void reduce(SummaryTask2 other) {
    assert other._summaries.length == _summaries.length;
    for (int i = 0; i < _summaries.length; i++)
      _summaries[i].add(other._summaries[i]);
  }
}
