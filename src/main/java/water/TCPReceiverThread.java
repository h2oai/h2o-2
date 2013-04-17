package water;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import water.api.Timeline;

/**
 * The Thread that looks for TCP Cloud requests.
 *
 * This thread just spins on reading TCP requests from other Nodes.
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TCPReceiverThread extends Thread {
  public static ServerSocketChannel SOCK;
  public TCPReceiverThread() { super("TCP Receiver"); }

  // The Run Method.
  // Started by main() on a single thread, this code manages reading TCP requests
  @SuppressWarnings("resource")
  public void run() {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
    ServerSocketChannel errsock = null;
    boolean saw_error = false;

    while( true ) {
      try {
        // Cleanup from any prior socket failures.  Rare unless we're really sick.
        if( errsock != null ) { // One time attempt a socket close
          final ServerSocketChannel tmp2 = errsock; errsock = null;
          tmp2.close();       // Could throw, but errsock cleared for next pass
        }
        if( saw_error ) Thread.sleep(100); // prevent deny-of-service endless socket-creates
        saw_error = false;

        // ---
        // More common-case setup of a ServerSocket
        if( SOCK == null ) {
          SOCK = ServerSocketChannel.open();
          SOCK.socket().setReceiveBufferSize(AutoBuffer.BBSIZE);
          SOCK.socket().bind(H2O.SELF._key);
        }

        // Block for TCP connection and setup to read from it.
        AutoBuffer ab = new AutoBuffer(SOCK.accept());
        int ctrl = ab.getCtrl();

        // Record the last time we heard from any given Node
        ab._h2o._last_heard_from = System.currentTimeMillis();
        TimeLine.record_recv(ab, true,0);
        // Hand off the TCP connection to the proper handler
        switch( UDP.udp.UDPS[ctrl] ) {
          //case exec:     H2O.submitTask(new FJPacket(ab,ctrl)); break;
        case exec:     RPC.remote_exec(ab).close(); break;
        case ack:      RPC.tcp_ack (ab); break;
        case timeline: TimeLine.tcp_call(ab); break;
        default: throw new RuntimeException("Unknown TCP Type: " + ab.getCtrl());
        }

      } catch( java.nio.channels.AsynchronousCloseException ex ) {
        break;                  // Socket closed for shutdown
      } catch( Exception e ) {
        // On any error from anybody, close all sockets & re-open
        System.err.println("IO error on TCP port "+H2O.UDP_PORT+": "+e);
        e.printStackTrace();
        saw_error = true;
        errsock = SOCK ;  SOCK = null; // Signal error recovery on the next loop
      }
    }
  }
}
