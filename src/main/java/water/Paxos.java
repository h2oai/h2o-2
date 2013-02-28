package water;
import java.util.*;

/**
 * Paxos
 *
 * Used to define Cloud membership.  See:
 *   http://en.wikipedia.org/wiki/Paxos_%28computer_science%29
 *
 * This Paxos implementation communicates via combination of multi-cast on the
 * local subnet and point-to-point.  Multi-cast is used to announce the
 * existence of *this* Cloud to any other Cloud in the subnet - which is
 * basically a Client request to all Servers to run a subnet-local round of
 * leadership and membership.  If all Servers who hears this request are
 * already in the same Cloud, then no action is required.

 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public abstract class Paxos {
  private static final boolean DEBUG = Boolean.getBoolean("water.paxos.debug");

  public static class State extends Iced {
    public long _promise;
    public long _oldProposal;
    public long _idLo;
    public long _idHi;
    public H2ONode[] _members;

    public UUID uuid() { return new UUID(_idHi, _idLo); }
  }

  static H2ONode LEADER = H2O.SELF; // Leader has the lowest IP of any in the set
  static HashSet<H2ONode> PROPOSED_MEMBERS = new HashSet();
  static HashSet<H2ONode> ACCEPTED         = new HashSet();
  static { PROPOSED_MEMBERS.add(H2O.SELF);  }

  // We have a one-off internal buffer.
  // This buffer serves many duties:
  // - Storage of most recent proposal
  // - Storage of most recent promise
  // - Something to write to the UDP output buffer
  static State _state = new State();
  static { _state._members = PROPOSED_MEMBERS.toArray(new H2ONode[1]); }


  // If we receive a Proposal, we need to see if it larger than any we have
  // received before.  If so, then we need to Promise to honor it.  Also, if we
  // have Accepted prior Proposals, we need to include that info in any Promise.
  static long PROPOSAL_MAX;
  static long PROPOSAL_MAX_SENT;

  // Whether or not we have common knowledge
  public static volatile boolean _commonKnowledge = false;
  // Whether or not we're allowing distributed-writes.  The cloud is not
  // allowed to change shape once we begin writing.
  public static volatile boolean _cloudLocked = false;

  // ---
  // This is a packet announcing what Cloud this Node thinks is the current
  // Cloud.
  static synchronized void doHeartbeat( H2ONode h2o ) {
    // If this packet is for *this* Cloud, just carry on (the heartbeat has
    // already been recorded.
    H2O cloud = H2O.CLOUD;
    if( h2o.is_cloud_member(cloud) ) {
      // However, do a 1-time printing when we realize all members of the cloud
      // are mutally agreed upon a new cloud shape.  This is not the same as a
      // Paxos vote, which only requires a Quorum.  This happens after everybody
      // has agreed to the cloud AND published that result to this node.
      boolean ck = true;        // Assume true
      for( H2ONode h2o2 : cloud._memary )
        if( !h2o2.is_cloud_member(cloud) )
          ck = false;           // This guy has not heartbeat'd that "he's in"
      if( ck == false && _commonKnowledge == true && _cloudLocked )
        killCloud(); // Cloud-wide kill because things changed after key inserted
      if( !_commonKnowledge && ck ) { // Everybody just now agrees on the Cloud
        Paxos.class.notify(); // Also, wake up a worker thread stuck in DKV.put
        System.out.printf("[h2o] Paxos Cloud of size %d formed: %s\n",
                          cloud._memset.size(), cloud._memset.toString());
      }
      _commonKnowledge = ck;    // Set or clear "common knowledge"
      return;                   // Do nothing!
    }

    print("hart: mismatched cloud announcement",h2o);

    // If this dude is supposed to be *in* our Cloud then maybe it's a slow or
    // delayed heartbeat packet, or maybe he's missed the Accepted announcement.
    // In either case, pound the news into his head.
    if( cloud._memset.contains(h2o) ) {
      // if( H2O.isIDFromPrevCloud(h2o) ) {
      if ( 1==1 ) {
        // In situations of rapid cloud formation, we could have:
        // A Cloud of {A,B} is voted on.
        // A Cloud of {A,B,C,D} is voted on by A,C,D forming a quorum.  B is slow.
        // New members E,F,G appear to A, so A's proposed list is now {A-G}
        // B, still slow, heartbeats cloud {A,B}
        // At this point: B is in {A,B}, A is in {A,B,C} which includes B,
        // but A is busy working on {A-G}.
        print("hart: is member but did not get the news1",cloud._memset);
        print("hart: is member but did not get the news2",PROPOSED_MEMBERS);
        if( PROPOSED_MEMBERS.equals(cloud._memset) ) { // But if things are not moving fast...
          _state._members = PROPOSED_MEMBERS.toArray(new H2ONode[0]);
          UDPPaxosAccepted.build_and_multicast(_state); // Then try to update the slow guy
        }
        return;
      } else {
        // Trigger new round of Paxos voting: remove this guy from our cloud
        // (since he thinks he does not belong), and try again.
        PROPOSED_MEMBERS.remove(h2o);
      }
    } else {
      // Got a heartbeat from some dude not in the Cloud.  Probably napping
      // Node woke up and hasn't yet smelled the roses (i.e., isn't aware the
      // Cloud shifted and kicked him out).  Could be a late heartbeat from
      // him.  Offer to vote him back in.
      if( !addProposedMember(h2o) )
        print("hart: already part of proposal",PROPOSED_MEMBERS);
    }

    // Trigger a Paxos proposal because there is somebody new, or somebody old
    doChangeAnnouncement(cloud);
  }

  // Remove any laggards.  Laggards should notice they are being removed from
  // the Cloud - and if they do they can complain about it and get
  // re-instated.  If they don't notice... then they need to be removed.
  // Recompute leader.
  private static void removeLaggards() {
    long now = System.currentTimeMillis();
    changeLeader(null);
    for( Iterator<H2ONode> i = PROPOSED_MEMBERS.iterator(); i.hasNext(); ) {
      H2ONode h2o = i.next();
      // Check if node timed out
      long msec = now - h2o._last_heard_from;
      if( msec > HeartBeatThread.TIMEOUT ) {
        assert h2o != H2O.SELF; // Not timing-out self???
        print("kill: Removing laggard ",h2o);
        i.remove();
      } else {                  // Else find the lowest IP address to be leader
        if( h2o.compareTo(LEADER) < 0 ) changeLeader(h2o);
      }
    }
    return;
  }

  // Handle a mis-matched announcement; either a self-heartbeat has noticed a
  // laggard in our current Cloud, or we got a heartbeat from somebody outside
  // the cloud.  The caller must have already synchronized.
  synchronized static void doChangeAnnouncement( H2O cloud ) {

    // Remove laggards and recompute leader
    removeLaggards();

    // At this point, we have changed the current Proposal: either added due to
    // a heartbeat from an outsider, or tossed out a laggard or both.

    // Look again at the new proposed Cloud membership.  If it matches the
    // existing Cloud... then we like things the way they are and we want to
    // ignore this change announcement.  This can happen if e.g. somebody is
    // trying to vote-in a laggard; they have announced a Cloud with the
    // laggard and we're skeptical.
    if( cloud._memset.equals(PROPOSED_MEMBERS) ) {
      assert cloud._memary[0] == LEADER;
      print("chng: no change from current cloud, ignoring change request",PROPOSED_MEMBERS);
      return;
    }
    // Reset all memory of old accepted proposals; we're on to a new round of voting
    _state._members = PROPOSED_MEMBERS.toArray(new H2ONode[0]);

    // The Leader Node for this proposed Cloud will act as the distinguished
    // Proposer of a new Cloud membership.  Non-Leaders will act as passive
    // Accepters.

    if( H2O.SELF == LEADER ) {
      // If we are proposing a leadership change, we need to do the Basic Paxos
      // algorithm from scratch.  If we are keeping the leadership but, e.g.
      // adding or removing a local Node then we can go for the Multi-Paxos
      // steady-state response.

      // See if we are changing cloud leaders?
      if( cloud._memary[0] == LEADER ) {
        // TODO
        //  throw new Error("Unimplemented: multi-paxos same-leader optimization");
      }

      // We're fighting over Cloud leadership!  We need to throw out a 'Prepare
      // N' so we can propose a new Cloud, where the proposal is bigger than
      // any previously submitted by this Node.  Note that only people who
      // think they should be leaders get to toss out proposals.
      ACCEPTED.clear(); // Reset the Accepted count: we got no takings on this new Proposal
      long proposal_num = PROPOSAL_MAX+1;
      // note PROPOSAL_MAX_SENT never gets zeroed, unlike PROPOSAL_MAX
      if ( PROPOSAL_MAX_SENT > PROPOSAL_MAX ) {
        proposal_num = PROPOSAL_MAX_SENT+1;
      }
      PROPOSAL_MAX_SENT = proposal_num;

      UUID uuid = UUID.randomUUID();
      _state._idLo = uuid.getLeastSignificantBits();
      _state._idHi = uuid.getMostSignificantBits();
      Paxos.print("send: Prepare "+proposal_num+" for leadership fight ",PROPOSED_MEMBERS);
      UDPPaxosProposal.build_and_multicast(proposal_num);
    } else {
      // Non-Leaders act as passive Accepters.  All Nodes should respond in a
      // timely fashion, including Leaders - if they fail the basic heartbeat
      // timeout, then they may be voted out of the Cloud...  which will start
      // another leadership fight at that time.  Meanwhile, this is effectively a
      // Paxos Client request to the Paxos Leader / Proposer ... not to us.
      // Ignore it.
      print("do  : got cloud change request as an acceptor; ignoring it",PROPOSED_MEMBERS);
    }
  }

  // ---
  // This is a packet announcing a Proposal, which includes an 8-byte Proposal
  // number and the guy who sent it (and thinks he should be leader).
  static synchronized int doProposal( final long proposal_num, final H2ONode proposer ) {
    print("recv: Proposal num "+proposal_num+" by ",proposer);

    // We got a Proposal from somebody.  We can toss this guy in the world-
    // state we're holding but we won't be pulling in all HIS Cloud
    // members... we wont find out about them until other people start
    // announcing themselves... so adding this dude here is an optimization.
    addProposedMember(proposer);

    // Is the Proposal New or Old?
    if( proposal_num < PROPOSAL_MAX ) { // Old Proposal!  We can ignore it...
      // But we want to NAK this guy
      if( proposer != H2O.SELF )
        UDPPaxosNack.build_and_multicast(PROPOSAL_MAX-1, proposer);
      return print("do_proposal NAK; self:" + H2O.SELF + " target:"+proposer + " proposal " + proposal_num, proposer);
    } else if( proposal_num == PROPOSAL_MAX ) { // Dup max proposal numbers?
      if( proposer == LEADER )                  // Ignore dups from leader
        return print("do_proposal: ignoring duplicate proposal", proposer);
      // Ahh, a dup proposal from non-leader.  Must be an old proposal
      if( proposer != H2O.SELF )
        UDPPaxosNack.build_and_multicast(PROPOSAL_MAX, proposer);
      return print("do_proposal NAK; self:" + H2O.SELF + " target:"+proposer + " proposal " + proposal_num, proposer);
    }

    // A new larger Proposal number appeared; keep track of it
    PROPOSAL_MAX = proposal_num;
    ACCEPTED.clear(); // If I was acting as a Leader, my Proposal just got whacked

    if( LEADER == proposer ) {    // New Proposal from what could be new Leader?
      // Now send a Promise to the Proposer that I will ignore Proposals less
      // than PROPOSAL_MAX (8 bytes) and include any prior ACCEPTED Proposal
      // number (8 bytes) and old the Proposal's Value
      assert _state._oldProposal < proposal_num; // we have not already promised what is about to be proposed
      _state._promise = proposal_num;
      if( H2O.SELF == LEADER ) {
        print("send: promise# "+proposal_num+" via direct call instead of UDP", proposer);
        return doPromise(_state, proposer);
      } else {
        print("send: promise# "+proposal_num, proposer);
        return UDPPaxosPromise.singlecast(_state, proposer);
      }
    }
    // Else proposal from some guy who I do not think should be leader in the
    // New World Order.  If I am not Leader in the New World Order, let Leader
    // duke it out with this guy.
    if( H2O.SELF != LEADER )
      return print("do  : Proposal from non-leader to non-leader; ignore; leader should counter-propose",PROPOSED_MEMBERS);

    // I want to be leader, and this guy is messing with my proposal.  Try
    // again to make him realize I should be leader.
    doChangeAnnouncement(H2O.CLOUD);
    return 0;
  }

  // Received a Nack on a proposal
  static synchronized void doNack( long proposal_num, final H2ONode h2o ) {
    print("recv: Nack num "+proposal_num+" by ",h2o);

    addProposedMember(h2o);
    // Nacking the named proposal
    if( proposal_num >= PROPOSAL_MAX ) {
      PROPOSAL_MAX = proposal_num;       // At least bump proposal to here
      doChangeAnnouncement(H2O.CLOUD); // Re-vote from the start
    }
  }

  // Recieved a Promise from an Acceptor to ignore Proposals below a certain
  // number.  Must be synchronized.  Buf is freed upon return.
  static synchronized int doPromise( State state, H2ONode h2o ) {
    print("recv: Promise", PROPOSED_MEMBERS, state);
    long promised_num = state._promise;

    if( PROPOSAL_MAX==0 )
      return print("do  : nothing, received Promise "+promised_num+" but no proposal in progress",PROPOSED_MEMBERS);

    // Check for late-arriving promise, after a better Leader has announced
    // himself to me... and I do not want to be leader no more.
    if( LEADER != H2O.SELF )
      return print("do  : nothing: recieved promise ("+promised_num+"), but I gave up being leader" ,PROPOSED_MEMBERS);

    // Hey!  Got a Promise from somebody, while I am a leader!
    // I always get my OWN proposals, which raise PROPOSAL_MAX, so this is for
    // some old one.  I only need to track the current "largest proposal".
    if( promised_num < PROPOSAL_MAX )
      return print("do  : nothing: promise ("+promised_num+") is too old to care about ("+PROPOSAL_MAX+")",h2o);

    // Extract any prior accepted proposals
    long prior_proposal = state._oldProposal;
    // Extract the prior accepted Value also
    HashSet<H2ONode> prior_value = new HashSet(Arrays.asList(_state._members));

    // Does this promise match the membership I like?  If so, we'll accept the
    // promise.  If not, we'll blow it off.. and hope for promises for what I
    // like.  In normal Paxos, we have to report back any Value PREVIOUSLY
    // promised, even for new proposals.  But in this case, the wrong promise
    // basically triggers a New Round of Paxos voting... so this becomes a
    // stale promise for the old round
    if( prior_proposal > 0 && !PROPOSED_MEMBERS.equals(prior_value) )
      return print("do  : nothing, because this is a promise for the wrong thing",prior_value);

    ACCEPTED.add(h2o);

    // See if we hit the Quorum needed
    final int quorum = (PROPOSED_MEMBERS.size()>>1)+1;
    if( ACCEPTED.size() < quorum )
      return print("do  : No Quorum yet "+ACCEPTED+"/"+quorum,PROPOSED_MEMBERS);
    if( ACCEPTED.size() > quorum )
      return print("do  : Nothing; Quorum exceeded and already sent AcceptRequest "+ACCEPTED+"/"+quorum,PROPOSED_MEMBERS);

    // We hit Quorum.  We can now ask the Acceptors to accept this proposal.
    // Build & multicast an Accept! packet.  It is our own proposal with the 8
    // bytes of Accept number set, and includes the members as the agreed Value
    _state._oldProposal = PROPOSAL_MAX;
    _state._members = PROPOSED_MEMBERS.toArray(new H2ONode[0]);
    UDPPaxosAccept.build_and_multicast(_state);
    return print("send: AcceptRequest because hit Quorum ",PROPOSED_MEMBERS,_state);
  }

  // Recieved an Accept Request from some Proposer after he hit a Quorum.  The
  // packet has 8 bytes of proposal number, and a membership list.  Buf is
  // freed when done.
  static synchronized int doAccept( State state, H2ONode h2o ) {
    print("recv: AcceptRequest ", null, state);
    long proposal_num = state._oldProposal;
    if( PROPOSAL_MAX==0 )
      return print("do  : nothing, received Accept! "+proposal_num+" but no proposal in progress",PROPOSED_MEMBERS,state);

    if( proposal_num < PROPOSAL_MAX )
      // We got an out-of-date AcceptRequest which we can ignore.  The Leader
      // should have already started a new proposal round
      return print("do  : ignoring out of date AcceptRequest ",null,state);

    PROPOSAL_MAX = proposal_num;

    // At this point, all Acceptors should tell all Learners via Accepted
    // messages about the new agreement.  However, the Leader is also an
    // Acceptor so we'll let him do one broadcast to all Learners.
    if( LEADER != H2O.SELF ) {
      _state = state;           // Remember the proposed value
      return print("do  : record proposal state; Accept but I am not Leader, no need to send Accepted",PROPOSED_MEMBERS,_state);
    }

    UDPPaxosAccepted.build_and_multicast(state);
    return print("send: Accepted from leader only",PROPOSED_MEMBERS,state);
  }

  /** Recieved an Accepted packet from the Leader after he hit quorum. */
  static synchronized int doAccepted( State state, H2ONode h2o ) {
    // Record most recent ping time from sender
    long proposal_num = state._promise;
    HashSet<H2ONode> members = new HashSet(Arrays.asList(state._members));
    print("recv: Accepted ", members, state);
    if( !members.contains(H2O.SELF) ) { // Not in this set?
      // This accepted set excludes me, so we need to start another round of
      // voting.  Pick up the largest proposal to-date, and start voting again.
      if( proposal_num > PROPOSAL_MAX ) PROPOSAL_MAX = proposal_num;
      return print("do  : Leader missed me; I am still not in the new Cloud, so refuse the Accept and let my Heartbeat publish me again",members,state);
    }

    if( proposal_num == PROPOSAL_MAX && state.uuid().equals(H2O.CLOUD._id) )
      return print("do  : Nothing: Accepted with same cloud membership list",members,state);

    // We just got a proposal to change the cloud
    if( _commonKnowledge ) {    // We thought we knew what was going on?
      assert !_cloudLocked;
      _commonKnowledge = false; // No longer sure about things
      System.out.println("[h2o] Paxos Cloud voting in progress");
    }

    H2O.CLOUD.set_next_Cloud(state.uuid(), members);
    PROPOSAL_MAX=0; // Reset voting; next proposal will be for a whole new cloud
    _state._promise = _state._oldProposal = 0;
    return print("do  : Accepted so set new cloud membership list",members,state);
  }

  // Before we start doing distributed writes... block until the cloud
  // stablizes.  After we start doing distrubuted writes, it is an error to
  // change cloud shape - the distributed writes will be in the wrong place.
  static void lockCloud() {
    if( _cloudLocked ) return; // Fast-path cutout
    synchronized(Paxos.class) {
      while( !_commonKnowledge )
        try { Paxos.class.wait(); } catch( InterruptedException ie ) { }
    }
    _cloudLocked = true;
  }

  static private boolean addProposedMember(H2ONode n){
    assert n._heartbeat != null;
    if(!PROPOSED_MEMBERS.contains(n)){
		// H2O.OPT_ARGS.soft is a demo/hibernate mode launch
      if( _cloudLocked  && (H2O.OPT_ARGS.soft == null) ) {
        System.err.println("[h2o] Killing "+n+" because the cloud is locked.");
        UDPRebooted.T.locked.send(n);
        return false;
      }
      if( !n._heartbeat.check_cloud_md5() ) {
        if( H2O.CLOUD.size() > 1 ) {
          System.err.println("[h2o] Killing "+n+"  because of jar mismatch.");
          UDPRebooted.T.mismatch.send(n);
        } else {
          System.err.println("[h2o] Attempting to join "+n+" with a jar mismatch. Killing self.");
          System.exit(-1);
        }
        return false;
      }
    }

    if( PROPOSED_MEMBERS.add(n) ) {
      if( n.compareTo(LEADER) < 0 ) changeLeader(n);
      return true;
    }
    return false;
  }

  static void changeLeader( H2ONode newLeader ) {
    TypeMap.MAP.changeLeader();
    LEADER = newLeader;
  }

  static void killCloud() {
    UDPRebooted.T.error.send(H2O.SELF);
    System.err.println("[h2o] Cloud changing after Keys distributed - fatal error.");
    System.err.println("[h2o] Received kill "+3+" from "+H2O.SELF);
    System.exit(-1);
  }

  static int print( String msg, HashSet<H2ONode> members, State state ) {
    return print(msg,members," promise:"+state._promise+" old:"+state._oldProposal);
  }
  static int print( String msg, H2ONode h2o ) {
    return print(msg, new HashSet(Arrays.asList(h2o)), "");
  }
  static int print( String msg, HashSet<H2ONode> members ) {
    return print(msg,members,"");
  }
  static int print( String msg, HashSet<H2ONode> members, String msg2 ) {
    if( DEBUG ) System.out.println(msg+members+msg2);
    return 0;                   // handy flow-coding return
  }
}
