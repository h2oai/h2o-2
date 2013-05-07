package water;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import water.api.Timeline;
import water.util.Log;

/**
 * The Thread that looks for TCP Cloud requests.
 *
 * This thread just spins on reading TCP requests from other Nodes.
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TCPReceiverThread extends Thread {
  public static TCPReceiverThread TCPTHR;
  // Last N block/run durations in even (block time) and odd (run time) pairs.
  // The _idx points past the last valid point, and if even means the thread is
  // currently blocking and odd means it is blocking.
  final int _dms[] = new int[256];
  int _idx=1;
  long _timeRun, _timeBlock;

  public static ServerSocketChannel SOCK;
  public TCPReceiverThread() { super("TCPReceive"); }

  // The Run Method.
  // Started by main() on a single thread, this code manages reading TCP requests
  @SuppressWarnings("resource")
  public void run() {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
    ServerSocketChannel errsock = null;
    boolean saw_error = false;
    _timeRun = System.currentTimeMillis();

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

        // Record run-time between connections
        _timeBlock = System.currentTimeMillis();
        int runMS   = (int)Math.min(_timeBlock - _timeRun,Integer.MAX_VALUE);
        _dms[_idx] = runMS;     // Record recent run time
        _idx = (_idx+1)&(_dms.length-1);

        // Block for TCP connection and setup to read from it.
        SocketChannel sock = SOCK.accept();
        _timeRun = System.currentTimeMillis();

        // Record block-time between connections
        int blockMS = (int)Math.min(_timeRun - _timeBlock,Integer.MAX_VALUE);
        _dms[_idx] = blockMS;   // Record recent block time
        _idx = (_idx+1)&(_dms.length-1);

        // Read the TCP connection and handle it
        AutoBuffer ab = new AutoBuffer(sock);
        int ctrl = ab.getCtrl();

        // Record the last time we heard from any given Node
        ab._h2o._last_heard_from = _timeRun;
        TimeLine.record_recv(ab, true,0);
        // Hand off the TCP connection to the proper handler
        switch( UDP.udp.UDPS[ctrl] ) {
          //case exec:     H2O.submitTask(new FJPacket(ab,ctrl)); break;
        case exec:     RPC.remote_exec(ab); break;
        case ack:      RPC.tcp_ack (ab); break;
        case timeline: TimeLine.tcp_call(ab); break;
        default: throw new RuntimeException("Unknown TCP Type: " + ab.getCtrl());
        }

      } catch( java.nio.channels.AsynchronousCloseException ex ) {
        break;                  // Socket closed for shutdown
      } catch( Exception e ) {
        // On any error from anybody, close all sockets & re-open
        Log.err("IO error on TCP port "+H2O.UDP_PORT+": ",e);
        saw_error = true;
        errsock = SOCK ;  SOCK = null; // Signal error recovery on the next loop
      }
    }
  }

  // Approximate duty-cycle as a ratio of blocked to (blocked+runtime) over the
  // last 128 TCP reads.
  public int dutyCyclePercent() {
    // Update the most recent block-time or run-time for in-progress work
    long now = System.currentTimeMillis();
    int idx = _idx;             // Read once
    _dms[idx] = (int)Math.min(now - ((idx&1)==0 ? _timeBlock : _timeRun),Integer.MAX_VALUE);
    // Compute total run/block time
    int rms=0, bms=0;
    for( int i=0; i<_dms.length; i+=2 ) {
      bms += _dms[i+0];
      rms += _dms[i+1];
    }
    double duty = (double)rms/(bms+rms)*100.0;
    return Math.max(Math.min((int)(duty+0.5),100),0);
  }
}
