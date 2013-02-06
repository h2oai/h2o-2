package water;
import java.util.concurrent.DelayQueue;

/**
 * The Thread that looks for UDPAsyncTasks that are timing out
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPTimeOutThread extends Thread {
  public UDPTimeOutThread() { super("UDP Timeout"); }

  // List of "in progress" tasks.  When they time-out we do the time-out action
  // which is possibly a re-send if we suspect a dropped UDP packet, or a
  // fail-out if the target has died.
  static DelayQueue<RPC> PENDING = new DelayQueue<RPC>();

  // The Run Method.

  // Started by main() on a single thread, handle timing-out UDP packets
  public void run() {
    Thread.currentThread().setPriority(Thread.NORM_PRIORITY);
    while( true ) {
      try {
        RPC t = PENDING.take();
        // One-shot timeout effect.  Retries need to re-insert back in the queue
        if( H2O.CLOUD._memset.contains(t._target) )
          // A little unusual effect happens here: the resend typically does
          // I/O and perhaps large I/O - and we're resending because the
          // original task never got finished, possibly because it also got
          // blocked for I/O.  All this means that this resend call probably
          // blocks the UDPTimeoutThread for a long time as it sends it's I/O.
          // I've seen delays of more than a second under load.  However, these
          // delays shouldn't cause much of a problem: if the I/O path is
          // slammed then stalled other tasks are OK to wait even longer, as
          // they'll just be blocked on the same overloaded I/O.
          t.call();
        else
          t.cancel(true);
      } catch( InterruptedException e ) {
        // Interrupted while waiting for a packet?
        // Blow it off and go wait again...
      }
    }
  }
}
