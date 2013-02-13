package water;

import java.text.SimpleDateFormat;
import java.util.Date;

// Rock-simple Timer
// @author <a href="mailto:cliffc@0xdata.com"></a>
public class Timer {
  public final long _start = System.currentTimeMillis();
  public long time() { return System.currentTimeMillis() - _start; }
  public String toString() {
    final long now = System.currentTimeMillis();
    return PrettyPrint.msecs(now - _start, false) + " (Wall clock time: " +
        new SimpleDateFormat("dd-MMM hh:mm").format(new Date(now)) + ") ";
  }
}
