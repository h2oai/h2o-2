package hex;
import java.io.*;
import water.*;

/**
 * Shuffle the rows of some dataset, such that the natural placement of the
 * resulting ValueArray onto Nodes results in some good property.  The user
 * passes in a property function.  The obvious use-case is to get a good random
 * shuffle.
 *
 * @author cliffc@0xdata.com
 */
public abstract class NodeShuffle {

  static public String run( Key keyout, ValueArray aryin, String sfunc ) {
    H2O cloud = H2O.CLOUD;
    int numnode = cloud._memary.length;
    StringBuilder sb = new StringBuilder();
    sb.append(sfunc+ " shuffle of ").append(aryin._key).append(" to ").append(keyout).append(" for a ").append(numnode).append(" node, ").append(numnode).append(" place Cloud");
    long start = System.currentTimeMillis();

    long now = System.currentTimeMillis();
    sb.append("<p>Took in ").append(now-start).append("msec");
    return sb.toString();
  }
}

