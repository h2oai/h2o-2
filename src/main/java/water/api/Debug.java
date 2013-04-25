package water.api;

import water.*;
import water.util.L;
import water.util.L.Tag.Sys;

public class Debug extends Request {
  @Override protected Response serve() {
    int kcnt=0;
    for( Key key : H2O.keySet() ) {
      kcnt++;
      Value v = H2O.raw_get(key);
      L.debug(this,Sys.WATER, "K: ",key," V:",(v==null?"null":""+v._max));
    }
    return Response.error("Dumped "+kcnt+" keys");
  }
}
