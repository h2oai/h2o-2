package water.web;

import java.util.Properties;

import water.DKV;
import water.H2O;
import water.Key;
import water.UDP;
import water.Value;
import water.hdfs.TaskStore2HDFS;

/**
 *
 * @author tomas
 */
public class Store2HDFS extends H2OPage {

  @Override public String serveImpl(Server s, Properties args, String sessionID) throws PageError {
    Key srcKey = ServletUtil.check_key(args,"Key");

    String err = TaskStore2HDFS.store2Hdfs(srcKey);
    if( err != null ) return H2OPage.error(err);

    RString res = new RString("Successfully stored on HDFS into <a href='/Inspect?Key=%$key'>%key</a>");
    res.replace("key", srcKey);

    return H2OPage.success(res.toString());
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
}
