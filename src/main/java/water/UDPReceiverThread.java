package water;
import java.nio.channels.DatagramChannel;

/**
 * The Thread that looks for UDP Cloud requests.
 *
 * This thread just spins on reading UDP packets from the kernel and either
 * dispatching on them directly itself (if the request is known short) or
 * queuing them up for worker threads.
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPReceiverThread extends Thread {
  static private int  _unknown_packets_per_sec = 0;
  static private long _unknown_packet_time = 0;
  public UDPReceiverThread() {
    super("Direct UDP Receiver");
  }

  // ---
  // Started by main() on a single thread, this code manages reading UDP packets
  @SuppressWarnings("resource")
  public void run() {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
    DatagramChannel sock = H2O._udpSocket, errsock = null;
    boolean saw_error = false;

    while( true ) {
      try {
        // Cleanup from any prior socket failures.  Rare unless we're really sick.
        if( errsock != null ) { // One time attempt a socket close
          final DatagramChannel tmp2 = errsock; errsock = null;
          tmp2.close();       // Could throw, but errsock cleared for next pass
        }
        if( saw_error ) Thread.sleep(1000); // prevent deny-of-service endless socket-creates
        saw_error = false;

        // ---
        // Common-case setup of a socket
        if( sock == null ) {
          sock = DatagramChannel.open();
          sock.socket().bind(H2O.SELF._key);
        }

        // Receive a packet & handle it
        basic_packet_handling(new AutoBuffer(sock));

      } catch( java.nio.channels.AsynchronousCloseException ex ) {
        break;                  // Socket closed for shutdown
      } catch( Exception e ) {
        // On any error from anybody, close all sockets & re-open
        System.err.println("UDP Receiver error on port "+H2O.UDP_PORT);
        e.printStackTrace(System.err);
        saw_error = true;
        errsock  = sock ;  sock  = null; // Signal error recovery on the next loop
      }
    }
  }

  // Basic packet handling:
  //   - Timeline record it
  static public void basic_packet_handling( AutoBuffer ab ) {
    // Record the last time we heard from any given Node
    TimeLine.record_recv(ab);
    ab._h2o._last_heard_from = System.currentTimeMillis();

    // Snapshots are handled *IN THIS THREAD*, to prevent more UDP packets from
    // being handled during the dump.  Also works for packets from outside the
    // Cloud... because we use Timelines to diagnose Paxos failures.
    int ctrl = ab.getCtrl();
    ab.getPort(); // skip the port bytes
    if( ctrl == UDP.udp.timeline.ordinal() ) {
      UDP.udp.timeline._udp.call(ab).close();
      return;
    }

    // Suicide packet?  Short-n-sweet...
    if( ctrl == UDP.udp.rebooted.ordinal())
      UDPRebooted.checkForSuicide(ctrl, ab);

    // Get the Cloud we are operating under for this packet
    H2O cloud = H2O.CLOUD;
    // Check cloud membership; stale ex-members are "fail-stop" - we mostly
    // ignore packets from them (except paxos packets).
    boolean is_member = cloud._memset.contains(ab._h2o);

    // Paxos stateless packets & ACKs just fire immediately in a worker
    // thread.  Dups are handled by these packet handlers directly.  No
    // current membership check required for Paxos packets
    final int ACK = UDP.udp.ack.ordinal();
    if( UDP.udp.UDPS[ctrl]._paxos ||
        (is_member && ctrl <= ACK) ) {
      H2O.FJP_HI.execute(new FJPacket(ab));
      return;
    }

    // Some non-Paxos packet from a non-member.  Probably should record &
    // complain.
    if( !is_member ) {
      // Filter unknown-packet-reports.  In bad situations of poisoned Paxos
      // voting we can get a LOT of these packets/sec, flooding the console.
      _unknown_packets_per_sec++;
      long timediff = ab._h2o._last_heard_from - _unknown_packet_time;
      if( timediff > 1000 ) {
        System.err.println("Non-member packets: "+_unknown_packets_per_sec+"/sec, last one from "+ab._h2o);
        _unknown_packets_per_sec = 0;
        _unknown_packet_time = ab._h2o._last_heard_from;
      }
      ab.close();
      return;
    }

    // Convert Unreliable DP to Reliable DP.
    // If we got this packet already (it is common for the sender to send
    // dups), we do not want to do the work *again* - so we do not want to
    // enqueue it for work.  Also, if we've *replied* to this packet before
    // we just want to send the dup reply back.
    DTask old = ab._h2o.record_task(ab.getTask());
    if( old != null ) {       // We've seen this packet before?
      if( old instanceof NOPTask ) {
        // This packet has not been ACK'd yet.  Hence it's still a
        // work-in-progress locally.  We have no answer yet to reply with
        // but we do not want to re-offer the packet for repeated work.
        // Just ignore the packet.
      } else {
        // This is an old re-send of the same thing we've answered to before.
        // Send back the same old answer ACK.  If we sent via TCP before, then
        // we know the answer got there so just send a control-ACK back.  If we
        // sent via UDP, resend the whole answer.
        AutoBuffer rab = new AutoBuffer(ab._h2o).putTask(UDP.udp.ack,ab.getTask());
        if( old._repliedTcp ) rab.put1(RPC.SERVER_TCP_SEND);
        else old.write(rab.put1(RPC.SERVER_UDP_SEND));
        rab.close();
        assert !rab.hasTCP();
      }
      ab.close();
    } else {                  // Else not a repeat-packet
      // Announce new packet to workers.
      // "execlo" goes to "normal" priority queue.
      // "exechi" goes to "high"   priority queue.
      UDP.udp.UDPS[ctrl].pool().execute(new FJPacket(ab));
    }
  }
}
