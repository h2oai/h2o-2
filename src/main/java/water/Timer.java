package water;

import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * Simple Timer class.
 **/
public class Timer {

  public final long _start = System.currentTimeMillis();

  /**Return the difference between when the timer was created and the current time. */
  public long time() { return System.currentTimeMillis() - _start; }

  /**Return the difference between when the timer was created and the current time as a
   * string along with the time of creation in date format. */
  public String toString() {
    final long now = System.currentTimeMillis();
    return PrettyPrint.msecs(now - _start, false) + " (Wall clock time: " +
        new SimpleDateFormat("dd-MMM hh:mm").format(new Date(now)) + ") ";
  }

  /** return the start time of this timer.**/
  public String startAsString() { return new SimpleDateFormat("[dd-MMM hh:mm:ss:SSS]").format(new Date(_start)); }

}
