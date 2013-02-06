package water.api;

import water.Key;
import water.ValueArray;

public class Exec extends Request {
  private final Str _exec = new Str(EXPRESSION);
  private final Str _dest = new Str(DEST_KEY, "Result.hex");
  private final Bool _safe = new Bool(ESCAPE_NAN, false, "Escape NaN and Infinity in the result JSON");

  @Override
  protected Response serve() {
    String s = _exec.value();
    try {
      Key k = water.exec.Exec.exec(s, _dest.value());
      ValueArray va = ValueArray.value(k);
      Response r = new Inspect(k).serveValueArray(va);
      if( _safe.value() ) r.escapeIllegalJsonElements();
      return r;
    } catch( Exception e ) {
      return Response.error(e.getMessage());
    }
  }
}
