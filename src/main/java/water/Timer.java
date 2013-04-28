package water;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
/**
 * Simple Timer class.
 **/
public class Timer {

  /** SimpleDataFormat is not thread safe. To avoid constructing them repeatedly we store them into thread
   * local variables. */
  private static final ThreadLocal<SimpleDateFormat> utcFormat = new ThreadLocal<SimpleDateFormat>() {
    @Override protected SimpleDateFormat initialValue() {
      SimpleDateFormat format = new SimpleDateFormat("dd-MMM hh:mm:ss.SSS");
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      return format;
    }
  };
  private static final ThreadLocal<SimpleDateFormat> utcShortFormat = new ThreadLocal<SimpleDateFormat>() {
    @Override protected SimpleDateFormat initialValue() {
      SimpleDateFormat format = new SimpleDateFormat("hh:mm:ss.SSS");
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      return format;
    }
  };

  public final long _start = System.currentTimeMillis();

  /**Return the difference between when the timer was created and the current time. */
  public long time() { return System.currentTimeMillis() - _start; }

  /**Return the difference between when the timer was created and the current time as a
   * string along with the time of creation in date format. */
  public String toString() {
    final long now = System.currentTimeMillis();
    return PrettyPrint.msecs(now - _start, false) + " (Wall: " + utcFormat.get().format(new Date(now)) + ") ";
  }

  /** return the start time of this timer.**/
  public String startAsString() { return utcFormat.get().format(new Date(_start)); }
  /** return the start time of this timer.**/
  public String startAsShortString() { return utcShortFormat.get().format(new Date(_start)); }

}
