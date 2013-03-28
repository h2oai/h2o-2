package water.api;

import com.google.gson.JsonObject;
import java.util.Properties;
import water.*;

public class Debug extends Request {
  @Override protected Response serve() {
    int kcnt=0;
    for( Key key : H2O.keySet() ) {
      kcnt++;
      Value v = H2O.raw_get(key);
      System.out.println("K: "+key+" V:"+(v==null?"null":""+v._max));
    }
    return Response.error("Dumped "+kcnt+" keys");
  }
}
