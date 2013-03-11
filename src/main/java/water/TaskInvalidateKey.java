package water;

import java.util.concurrent.Future;

public class TaskInvalidateKey extends TaskPutKey {
  private TaskInvalidateKey(Key key){super(key,null);}
  @Override public byte priority(){return H2O.INVALIDATE_PRIORITY;}

  static void invalidate( H2ONode h2o, Key key, Futures fs ) {
    Future f = RPC.call(h2o,new TaskInvalidateKey(key));
    if( fs != null ) fs.add(f);
  }

}
