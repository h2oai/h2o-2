package water.util;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

import water.*;
import water.H2ONode.H2Okey;

/**
 * Wrapper around timeline snapshot. Implements iterator interface (events are
 * ordered according to send/receive dependencies across the nodes and trivial time
 * dependencies inside node)
 *
 * @author tomas
 */
public final class TimelineSnapshot implements
    Iterable<TimelineSnapshot.Event>, Iterator<TimelineSnapshot.Event> {
  long[][] _snapshot;
  Event[] _events;
  HashMap<Event, Event> _edges;
  public HashMap<Event, ArrayList<Event>> _sends;
  H2O _cloud;
  boolean _processed;

  public TimelineSnapshot(H2O cloud, long[][] snapshot) {
    _cloud = cloud;
    _snapshot = snapshot;
    _edges = new HashMap<Event, Event>();
    _sends = new HashMap<Event, ArrayList<Event>>();
    _events = new Event[snapshot.length];

    // DEBUG: print out the evetn stack as we got it
//    System.out.println("# of nodes: " + _events.length);
//    for (int j = 0; j < TimeLine.length(); ++j) {
//      System.out.print("row# " + j + ":");
//      for (int i = 0; i < _events.length; ++i) {
//        System.out.print("  ||  " + new Event(i, j));
//      }
//      System.out.println("  ||");
//    }

    for (int i = 0; i < _events.length; ++i) {
      // For a new Snapshot, most of initial entries are all zeros. Skip them
      // until we start finding entries... which will be the oldest entries.
      // The timeline is age-ordered (per-thread, we hope the threads are
      // fairly consistent)
      _events[i] = new Event(i, 0);
      if (_events[i].isEmpty()) {
        if (!_events[i].next())
          _events[i] = null;
      }
      if (_events[i] != null)
        processEvent(_events[i]);
      assert (_events[i] == null) || (_events[i]._arr[1] < 1024);
    }

    // now build the graph (i.e. go through all the events once)
    for (@SuppressWarnings("unused") Event e : this) ;

    _processed = true;
    for (int i = 0; i < _events.length; ++i) {
      // For a new Snapshot, most of initial entries are all zeros. Skip them
      // until we start finding entries... which will be the oldest entries.
      // The timeline is age-ordered (per-thread, we hope the threads are
      // fairly consistent)
      _events[i] = new Event(i, 0);
      if (_events[i].isEmpty()) {
        if (!_events[i].next())
          _events[i] = null;
      }
      assert (_events[i] == null) || (_events[i]._arr[1] < 1024);
    }
  }

  // convenience wrapper around event stored in snapshot
  // contains methods to access event data, move to the next previous event
  // and to test whether two events form valid sender/receiver pair
  //
  // it is also needed to keep track of sen/rcv dependencies when iterating over
  // events in timeline
  public class Event {
    int[] _arr = new int[2];
    long[] _val;
    boolean _blocked;

    public Event(int nodeId, int eventIdx) {
      _arr[0] = nodeId;
      _arr[1] = eventIdx;
      _val = _snapshot[nodeId];
    }

    @Override
    public final int hashCode() {
      return Arrays.hashCode(_arr);
    }

    @Override
    public final boolean equals(Object o) {
      if (o instanceof Event) {
        Event other = (Event) o;
        return (_arr[0] == other._arr[0]) && (_arr[1] == other._arr[1]);
      }
      return false;
    }

    public final int nodeId() {
      return _arr[0];
    }

    public final int eventIdx() {
      return _arr[1];
    }

    public final int send_recv() {
      return TimeLine.send_recv(_val, _arr[1]);
    }

    public final boolean isSend() {
      return send_recv() == 0;
    }

    public final boolean isRecv() {
      return send_recv() == 1;
    }

    public final InetAddress addrPack() {
      return TimeLine.inet(_val, _arr[1]);
    }

    public final String addrString() {
      InetAddress inet = addrPack();
      return (inet.isMulticastAddress() ? "multicast" : inet.toString())
          + (isRecv() ? ":" + portPack() : "");
    }

    public final int portPack() {
      assert !isSend(); // the port is always sender's port!!!
      int i = (int) TimeLine.l0(_val, _arr[1]);
      return (0xFFFF) & (i >> 8);
    }

    public final long dataLo() {
      return TimeLine.l0(_val, _arr[1]);
    }

    public final long dataHi() {
      return TimeLine.l8(_val, _arr[1]);
    }

    public String toString() {
      int udp_type = (int) (dataLo() & 0xff); // First byte is UDP packet type
      UDP.udp udpType = UDP.udp.UDPS[udp_type];
      String operation = isSend() ? " SEND " : " RECV ";
      String host1 = addrString();
      String host2 = _cloud._memary[nodeId()]._key.toString() + ":"
          + _cloud._memary[nodeId()]._key.getPort();
      String networkPart = isSend() ? (host2 + " -> " + host1) : (host1
          + " -> " + host2);

      return "Node(" + nodeId() + ": " + ns() + ") " + udpType.toString()
          + operation + networkPart + ", data = '"
          + Long.toHexString(this.dataLo()) + ','
          + Long.toHexString(this.dataHi()) + "'";
    }

    /**
     * Check if two events form valid sender/receiver pair.
     *
     * Two events are valid sender/receiver pair iff the ports, adresses and
     * payload match.
     *
     * @param other
     * @return true iff the two events form valid sender/receiver pair
     */
    final boolean match(Event other) {
      // check we're matching send and receive
      if (send_recv() == other.send_recv())
        return false;
      // compare the packet payload matches
      long myl0 = dataLo();
      long otherl0 = other.dataLo();
      int my_udp_type = (int) (myl0 & 0xff); // first byte is udp type
      int other_udp_type = (int) (otherl0 & 0xff); // first byte is udp type
      if (my_udp_type != other_udp_type)
        return false;
      UDP.udp e = UDP.udp.UDPS[my_udp_type];
      switch (e) {
      case heartbeat:
      case rebooted:
      case timeline:
//      case log:
        // compare only first 3 bytes here (udp type and port)
        if ((myl0 & 0xFFFFFFl) != (otherl0 & myl0 & 0xFFFFFFl))
          return false;
        break;
      case ack:
      case ackack:
//      case atomic:
//      case getkey:
//      case getkeys:
//      case putkey:
      case execlo:
      case exechi:
        // compare 3 ctrl bytes + 4 bytes task #
        if ((myl0 & 0xFFFFFFFFFFFFFFl) != (otherl0 & 0xFFFFFFFFFFFFFFl))
          return false;
        break;
      case paxos_accept:
      case paxos_accepted:
      case paxos_nack:
      case paxos_promise:
      case paxos_proposal:
        // compare ctrl bytes 3 + 12 bytes of payload for paxos
        if (myl0 != otherl0)
          return false;
        if ((dataHi() & 0xFFFFFFFFFFFFFFl) != (other.dataHi() & 0xFFFFFFFFFFFFFFl))
          return false;
        break;
      default:
        throw new Error("unexpected udp packet type " + e.toString());
      }

      // now compare addresses
      H2Okey myAddrHost = _cloud._memary[_arr[0]]._key;
      H2Okey otherAddrHost = _cloud._memary[other._arr[0]]._key;
      return isSend() ? (myAddrHost.equals(other.addrPack()) && (addrPack()
          .isMulticastAddress() || addrPack().equals(otherAddrHost)))
          : (otherAddrHost.equals(addrPack()) && (other.addrPack()
              .isMulticastAddress() || other.addrPack().equals(myAddrHost)));
    }

    public final boolean isEmpty() {
      return (_arr[1] < TimeLine.length()) ? TimeLine.isEmpty(_val, _arr[1])
          : false;
    }

    public final Event clone() {
      return new Event(_arr[0], _arr[1]);
    }

    boolean prev(int minIdx) {
      int min = Math.max(minIdx, -1);
      if (_arr[1] <= minIdx)
        return false;
      while (--_arr[1] > min)
        if (!isEmpty())
          return true;
      ;
      return false;
    }

    boolean prev() {
      return prev(-1);
    }

    Event previousEvent(int minIdx) {
      Event res = new Event(_arr[0], _arr[1]);
      return (res.prev(minIdx)) ? res : null;
    }

    Event previousEvent() {
      return previousEvent(-1);
    }

    boolean next(int maxIdx) {
      int max = Math.min(maxIdx, TimeLine.length());
      if (_arr[1] >= max)
        return false;
      while (++_arr[1] < max)
        if (!isEmpty())
          return true;
      return false;
    }

    boolean next() {
      return next(TimeLine.length());
    }

    Event nextEvent(int maxIdx) {
      Event res = new Event(_arr[0], _arr[1]);
      return (res.next(maxIdx)) ? res : null;
    }

    Event nextEvent() {
      return nextEvent(TimeLine.length());
    }

    /**
     * Used to determine ordering of events not bound by any dependency.
     *
     * Events compared according to following rules: Receives go before sends.
     * For two sends, pick the one with receives with smallest timestamp (ms)
     * otherwise pick the one with smallest timestamp (ms)
     *
     * @param other
     *          other Event to compare
     * @return
     */
    public final int compareTo(Event other) {
      if (other == null)
        return -1;
      int res = other.send_recv() - send_recv(); // recvs should go bfr senfs
      if (res == 0) {
        if (isSend()) {
          // compare by the time of receivers
          long myMaxMs = 0;
          long otherMaxMs = 0;
          ArrayList<Event> myRecvs = _sends.get(this);
          ArrayList<Event> otherRecvs = _sends.get(other);
          for (Event e : myRecvs)
            if (e.ms() > myMaxMs)
              myMaxMs = e.ms();
          for (Event e : otherRecvs)
            if (e.ms() > otherMaxMs)
              otherMaxMs = e.ms();
          res = (int) (myMaxMs - otherMaxMs);
        }
        if (res == 0)
          res = (int) (ms() - other.ms());
      }
      return res;
    }

    public long ns() {
      return TimeLine.ns(_val, _arr[1]);
    }

    public long ms() {
      return TimeLine.ms(_val, _arr[1]);
    }
  }

  /**
   * Check whether two events can be put together in sender/recv relationship.
   *
   * Events must match, also each sender can have only one receiver per node.
   *
   * @param senderCnd
   * @param recvCnd
   * @return
   */
  private boolean isSenderRecvPair(Event senderCnd, Event recvCnd) {
    if (senderCnd.isSend() && recvCnd.isRecv() && senderCnd.match(recvCnd)) {
      ArrayList<Event> recvs = _sends.get(senderCnd);
      if (recvs.isEmpty() || senderCnd.addrPack().isMulticastAddress()) {
        for (Event e : recvs) {
          if (e.nodeId() == recvCnd.nodeId())
            return false;
        }
        return true;
      }
    }
    return false;
  }

  /**
   * Process new event. For sender, check if there are any blocked receives
   * waiting for this send. For receiver, try to find matching sender, otherwise
   * block.
   *
   * @param idx
   */
  void processEvent(Event e) {
    assert !_processed;
    // Event e = _events[idx];
    if (e.isSend()) {
      _sends.put(e, new ArrayList<TimelineSnapshot.Event>());
      for (Event otherE : _events) {
        if ((otherE != null) && (otherE != e) && otherE._blocked
            && otherE.match(e)) {
          _edges.put(otherE, e);
          _sends.get(e).add(otherE);
          otherE._blocked = false;
        }
      }
    } else { // look for matching send, otherwise set _blocked
      assert !_edges.containsKey(e);
      int senderIdx = -1;
      for (int i = 0; i < _events.length; ++i) {
        // TODO: Matt is not sure if the .equals() below is correct
        //  in the world of IO nirvana.
        if (_cloud._memary[i]._key.getPort() == e.portPack()
            && _cloud._memary[i]._key.getAddress().equals(e.addrPack())) {
          senderIdx = i;
          break;
        }
      }
      if (senderIdx == -1) { // should not happen?
        // no possible sender - return and do not block
        System.err.println("no sender found! port = " + e.portPack()
            + ", ip = " + e.addrPack().toString());
        return;
      }
      if (senderIdx != e.nodeId()) {
        Event senderCnd = _events[senderIdx];
        if (senderCnd != null) {
          if (isSenderRecvPair(senderCnd, e)) {
            _edges.put(e, senderCnd.clone());
            _sends.get(senderCnd).add(e);
            return;
          }
          senderCnd = senderCnd.clone();
          while (senderCnd.prev()) {
            if (isSenderRecvPair(senderCnd, e)) {
              _edges.put(e, senderCnd);
              _sends.get(senderCnd).add(e);
              return;
            }
          }
        }
        e._blocked = true;// (senderIdx != e.nodeId());
      }
    }
    assert (e == null) || (e._arr[1] < 1024);
  }

  @Override
  public Iterator<TimelineSnapshot.Event> iterator() {
    return this;
  }

  /**
   * Just check if there is any non null non-issued event.
   */
  @Override
  public boolean hasNext() {
    for (int i = 0; i < _events.length; ++i)
      if (_events[i] != null && (!_events[i].isEmpty() || _events[i].next())) {
        assert (_events[i] == null)
            || ((_events[i]._arr[1] < 1024) && !_events[i].isEmpty());
        return true;
      } else {
        assert (_events[i] == null)
            || ((_events[i]._arr[1] < 1024) && !_events[i].isEmpty());
        _events[i] = null;
      }
    return false;
  }

  public Event getDependency(Event e) {
    return _edges.get(e);
  }

  /**
   * Get the next event of the timeline according to the ordering. Ordering is
   * performed in this method. Basically there are n ordered stream of events
   * with possible dependenencies caused by send/rcv relation.
   *
   * Sends are always eligible to be scheduled. Receives are eligible only if
   * their matching send was already issued. In situation when current events of
   * all streams are blocked (should not happen!) the oldest one is unblocked
   * and issued.
   *
   * Out of all eligible events, the smallest one (according to Event.compareTo)
   * is picked.
   */
  @Override
  public TimelineSnapshot.Event next() {
    if (!hasNext())
      throw new NoSuchElementException();
    int selectedIdx = -1;

    for (int i = 0; i < _events.length; ++i) {
      if (_events[i] == null || _events[i]._blocked)
        continue;
      if (_events[i].isRecv()) { // check edge dependency
        Event send = _edges.get(_events[i]);
        if ((send != null) && (_events[send.nodeId()] != null)
            && send.eventIdx() >= _events[send.nodeId()].eventIdx())
          continue;
      }
      selectedIdx = ((selectedIdx == -1) || _events[i]
          .compareTo(_events[selectedIdx]) < 0) ? i : selectedIdx;
    }
    if (selectedIdx == -1) { // we did not select anything -> all event streams
                             // must be blocked return the oldest one (assuming
                             // corresponding send was in previous snapshot)
      // System.out.println("*** all blocked ***");
      selectedIdx = 0;
      long selectedNs = (_events[selectedIdx] != null) ? _events[selectedIdx]
          .ns() : Long.MAX_VALUE;
      long selectedMs = (_events[selectedIdx] != null) ? _events[selectedIdx]
          .ms() : Long.MAX_VALUE;
      for (int i = 1; i < _events.length; ++i) {
        if (_events[i] == null)
          continue;

        if ((_events[i].ms() < selectedMs) && (_events[i].ns() < selectedNs)) {
          selectedIdx = i;
          selectedNs = _events[i].ns();
          selectedMs = _events[i].ms();
        }
      }
    }
    assert (selectedIdx != -1);
    assert (_events[selectedIdx] != null)
        && ((_events[selectedIdx]._arr[1] < 1024) && !_events[selectedIdx]
            .isEmpty());
    Event res = _events[selectedIdx];
    _events[selectedIdx] = _events[selectedIdx].nextEvent();
    if (_events[selectedIdx] != null && !_processed)
      processEvent(_events[selectedIdx]);
    // DEBUG
//    if (_processed)
//      if (res.isRecv())
//        System.out.println("# " + res + " PAIRED WITH "
//            + (_edges.containsKey(res) ? _edges.get(res) : "*** NONE ****"));
//      else
//        System.out.println("# " + res + " receivers: "
//            + _sends.get(res).toString());
    return res;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
