package water;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Timer {

  public final long _start = System.currentTimeMillis();

  public long time() { return System.currentTimeMillis() - _start; }

  public String toString() {
    final long now = System.currentTimeMillis();
    return PrettyPrint.msecs(now - _start, false) + " (Wall clock time: " +
        new SimpleDateFormat("dd-MMM hh:mm").format(new Date(now)) + ") ";
  }

  public String startAsString() { return new SimpleDateFormat("[dd-MMM hh:mm:ss]").format(new Date(_start)); }

}
