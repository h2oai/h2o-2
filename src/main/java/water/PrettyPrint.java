package water;

import java.util.concurrent.TimeUnit;

public class PrettyPrint {
  public static String msecs(long msecs, boolean truncate) {
    final long hr = TimeUnit.MILLISECONDS.toHours (msecs); msecs -= TimeUnit.HOURS .toMillis(hr);
    final long min = TimeUnit.MILLISECONDS.toMinutes(msecs); msecs -= TimeUnit.MINUTES.toMillis(min);
    final long sec = TimeUnit.MILLISECONDS.toSeconds(msecs); msecs -= TimeUnit.SECONDS.toMillis(sec);
    final long ms = TimeUnit.MILLISECONDS.toMillis (msecs);
    if( !truncate ) return String.format("%02d:%02d:%02d.%03d", hr, min, sec, ms);
    if( hr != 0 ) return String.format("%2d:%02d:%02d.%03d", hr, min, sec, ms);
    if( min != 0 ) return String.format("%2d min %2d.%03d sec", min, sec, ms);
    return String.format("%2d.%03d sec", sec, ms);
  }

  public static String bytes(long bytes) {
    if( bytes < 0 ) return "N/A";
    if( bytes < 1L<<10 ) return String.format("%d B" , bytes);
    if( bytes < 1L<<20 ) return String.format("%.1f KB", bytes/(double)(1L<<10));
    if( bytes < 1L<<30 ) return String.format("%.1f MB", bytes/(double)(1L<<20));
    if( bytes < 1L<<40 ) return String.format("%.2f GB", bytes/(double)(1L<<30));
    if( bytes < 1L<<50 ) return String.format("%.3f TB", bytes/(double)(1L<<40));
    return String.format("%.3f PB", bytes/(double)(1L<<50));
  }

  public static String bytesPerSecond(long bytes) {
    if( bytes < 0 ) return "N/A";
    return bytes(bytes)+"/S";
  }
}
