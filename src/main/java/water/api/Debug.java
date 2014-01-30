package water.api;

import water.*;
import water.util.Log;

public class Debug extends Request {
  @Override protected Response serve() {
    int kcnt=0;
    for( Key key : H2O.localKeySet() ) {
      kcnt++;
      Value v = H2O.raw_get(key);
      Log.debug("K: ",key," V:",(v==null?"null":""+v._max));
    }
    return Response.error("Dumped "+kcnt+" keys");
  }
}
