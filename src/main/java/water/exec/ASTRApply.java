package water.exec;

import java.util.*;

import water.*;
import water.fvec.*;
import water.nbhm.NonBlockingHashMap;
import water.util.FrameUtils;

/** Parse a generic R string and build an AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
// R's Apply.  Function is limited to taking a single column and returning
// a single column.  Double is limited to 1 or 2, statically determined.
class ASTRApply extends ASTOp {
  static final String VARS[] = new String[]{ "", "ary", "dbl1.2", "fcn"};
  ASTRApply( ) { super(VARS,
                       new Type[]{ Type.ARY, Type.dblary(), Type.dblary(), Type.fcn(new Type[]{Type.dblary(),Type.ARY}) },
                       OPF_PREFIX,
                       OPP_PREFIX,
                       OPA_RIGHT); }
  protected ASTRApply( String vars[], Type ts[], int form, int prec, int asso) { super(vars,ts,form,prec,asso); }
  @Override String opStr(){ return "apply";}
  @Override ASTOp make() {return new ASTRApply();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    // Peek everything from the stack
    final ASTOp op = env.fcn(-1);    // ary->dblary but better be ary[,1]->dblary[,1]
    double d = env.dbl(-2);    // MARGIN: ROW=1, COLUMN=2 selector
    Frame fr = env.ary(-3);    // The Frame to work on
    if( d==2 || d== -1 ) {     // Work on columns?
      int ncols = fr.numCols();

      double ds[][] = null; // If results are doubles, gather in small array
      Frame fr2 = null;     // If the results are Vecs, gather them in this Frame
      String err = "apply requires that "+op+" return 1 column";
      if( op._t.ret().isDbl() ) ds = new double[ncols][1];
      else                     fr2 = new Frame(new String[0],new Vec[0]);

      // Apply the function across columns
      try {
        Vec vecs[] = fr.vecs();
        for( int i=0; i<ncols; i++ ) {
          env.push(op);
          env.push(new Frame(new String[]{fr._names[i]},new Vec[]{vecs[i]}));
          env.fcn(-2).apply(env, 2, null);
          if( ds != null ) {    // Doubles or Frame results?
            ds[i][0] = env.popDbl();
          } else {                // Frame results
            fr2.add(fr._names[i], env.popXAry().theVec(err));
          }
        }
      } catch( IllegalArgumentException iae ) {
        env.subRef(fr2,null);
        throw iae;
      }
      env.pop(4);
      if( ds != null ) env.push(FrameUtils.frame(new String[]{"C1"},ds));
      else env.push(fr2);
      assert env.isAry();
      return;
    }
    if( d==1 || d==-2) {      // Work on rows
      // apply on rows is essentially a map function
      Type ts[] = new Type[2];
      ts[0] = Type.unbound();
      ts[1] = Type.ARY;
      Type ft1 = Type.fcn(ts);
      Type ft2 = op._t.find();  // Should be a function type
      if( !ft1.union(ft2) ) {
        if( ft2._ts.length != 2 )
          throw new IllegalArgumentException("FCN " + op.toString() + " cannot accept one argument.");
        if( !ft2._ts[1].union(ts[1]) )
          throw new IllegalArgumentException("Arg " + op._vars[1] + " typed " + ft2._ts[1].find() + " but passed as " + ts[1]);
        assert false;
      }
      // find out return type
      double[] rowin = new double[fr.vecs().length];
      for (int c = 0; c < rowin.length; c++) rowin[c] = fr.vecs()[c].at(0);
      final int outlen = op.map(env,rowin,null).length;
      final Env env0 = env;
      MRTask2 mrt = new MRTask2() {
        @Override public void map(Chunk[] cs, NewChunk[] ncs) {
          double rowin [] = new double[cs.length];
          double rowout[] = new double[outlen];
          for (int row = 0; row < cs[0]._len; row++) {
            for (int c = 0; c < cs.length; c++) rowin[c] = cs[c].at0(row);
            op.map(env0, rowin, rowout);
            for (int c = 0; c < ncs.length; c++) ncs[c].addNum(rowout[c]);
          }
        }
      };
      String[] names = new String[outlen];
      for (int i = 0; i < names.length; i++) names[i] = "C"+(i+1);
      Frame res = mrt.doAll(outlen,fr).outputFrame(names, null);
      env.poppush(4,res,null);
      return;
    }
    throw new IllegalArgumentException("MARGIN limited to 1 (rows) or 2 (cols)");
  }
}

// --------------------------------------------------------------------------
// Same as "apply" but defaults to columns.
class ASTSApply extends ASTRApply {
  static final String VARS[] = new String[]{ "", "ary", "fcn"};
  ASTSApply( ) { super(VARS,
                       new Type[]{ Type.ARY, Type.ARY, Type.fcn(new Type[]{Type.dblary(),Type.ARY}) },
                       OPF_PREFIX,
                       OPP_PREFIX,
                       OPA_RIGHT); }
  @Override String opStr(){ return "sapply";}
  @Override ASTOp make() {return new ASTSApply();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    // Stack: SApply, ary, fcn
    //   -->: RApply, ary, 2, fcn
    assert env.isFcn(-3);
    env._fcn[env._sp-3] = new ASTRApply();
    ASTOp fcn = env.popFcn();   // Pop, no ref-cnt
    env.push(2.0);
    env.push(1);
    env._fcn[env._sp-1] = fcn;  // Push, no ref-cnt
    super.apply(env,argcnt+1,null);
  }
}

// --------------------------------------------------------------------------
// PLYR's DDPLY.  GroupBy by any other name.  Type signature:
//   #RxN  ddply(RxC,subC, 1xN function( subRxC ) { ... } )
//   R - Rows in original frame
//   C - Cols in original frame
//   subC - Subset of C; either a single column entry, or a 1 Vec frame with a list of columns.
//   subR - Subset of R, where all subC values are the same.
//   N - Return column(s).  Can be 1, and so fcn can return a dbl instead of 1xN
//  #R - # of unique combos in the original "subC" set

class ASTddply extends ASTOp {
  static final String VARS[] = new String[]{ "#RxN", "RxC", "subC", "fcn_subRxC"};
  ASTddply( ) { this(VARS, new Type[]{ Type.ARY, Type.ARY, Type.dblary(), Type.fcn(new Type[]{Type.dblary(),Type.ARY}) }); }
  ASTddply(String vars[], Type types[] ) { super(vars,types,OPF_PREFIX,OPP_PREFIX,OPA_RIGHT); }

  @Override String opStr(){ return "ddply";}
  @Override ASTOp make() {return new ASTddply();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Frame fr = env.ary(-3);    // The Frame to work on

    // Either a single column, or a collection of columns to group on.
    int cols[];
    if( !env.isAry(-2) ) {      // Single column?
      if( Double.isNaN(env.dbl(-2)) ) throw new IllegalArgumentException("NA not a valid column");
      cols = new int[]{(int)env.dbl(-2)-1};
    } else {                    // Else a collection of columns?
      Frame cs = env.ary(-2);
      if( cs.numCols() !=  1  ) throw new IllegalArgumentException("Only one column-of-columns for column selection");
      if( cs.numRows() > 1000 ) throw new IllegalArgumentException("Too many columns selected");
      cols = new int[(int)cs.numRows()];
      Vec vec = cs.vecs()[0];
      for( int i=0; i<cols.length; i++ )
        if( vec.isNA(i) ) throw new IllegalArgumentException("NA not a valid column");
        else cols[i] = (int)vec.at8(i)-1;
    }
    // Another check for sane columns
    for( int c : cols )
      if( c < 0 || c >= fr.numCols() )
        throw new IllegalArgumentException("Column "+(c+1)+" out of range for frame columns "+fr.numCols());

    // Was pondering a SIMD-like execution model, running the fcn "once" - but
    // in parallel for all groups.  But this isn't going to work: each fcn
    // execution will take different control paths.  Also the functions side-
    // effects' must only happen once, and they will make multiple passes over
    // the Frame passed in.
    //
    // GroupIDs' can vary from 1 group to 1-per-row.  Are formed by the cross-
    // product of the selection cols.  Will be hashed to find Group - NBHML
    // mapping row-contents to group.  Index is a sample row.  NBHML per-node,
    // plus roll-ups.  Result/Value is Group structure pointing to NewChunks
    // holding row indices.

    // Pass 1: Find Groups.
    // Build a NBHSet of unique double[]'s holding selection cols.
    // These are the unique groups, found per-node, rolled-up globally
    // Record the rows belonging to each group, locally.
    ddplyPass1 p1 = new ddplyPass1(true,cols).doAll(fr);

    // Pass 2: Build Groups.
    // Wrap Vec headers around all the local row-counts.
    int numgrps = p1._groups.size();
    int csz = H2O.CLOUD.size();
    ddplyPass2 p2 = new ddplyPass2(p1,numgrps,csz).invokeOnAllNodes();
    // vecs[] iteration order exactly matches p1._groups.keySet()
    Vec vecs[] = p2.close();
    // Push the execution env around the cluster
    Key envkey = Key.make();
    UKV.put(envkey,env);

    // Pass 3: Send Groups 'round the cluster
    // Single-threaded per-group work.
    // Send each group to some remote node for execution
    Futures fs = new Futures();
    int grpnum=0; // vecs[] iteration order exactly matches p1._groups.keySet()
    int nlocals[] = new int[csz]; // Count of local group#
    RemoteExec re = null;         // Sample RemoteExec
    for( Group g : p1._groups.keySet() ) {
      // vecs[] iteration order exactly matches p1._groups.keySet()
      Vec rows = vecs[grpnum++]; // Rows for this Vec
      Vec[] data = fr.vecs();    // Full data columns
      Vec[] gvecs = new Vec[data.length];
      Key[] keys = rows.group().addVecs(data.length);
      for( int c=0; c<data.length; c++ )
        gvecs[c] = new SubsetVec(rows._key,data[c]._key,keys[c],rows._espc);
      Frame fg = new Frame(fr._names,gvecs);
      // Non-blocking, send a group to a remote node for execution
      final int nidx = g.hashCode()%csz;
      fs.add(RPC.call(H2O.CLOUD._memary[nidx],(re=new RemoteExec(nlocals[nidx]++,p2._nlocals[nidx],g._ds,fg,envkey))));
    }
    fs.blockForPending();       // Wait for all functions to finish

    // Pass 4: Fold together all results; currently stored in NewChunk[]s per node
    Vec vres[] = new ddplyPass4(envkey,cols.length,re._ncols).invokeOnAllNodes().close();

    // Result Frame
    String[] names = new String[cols.length+re._ncols];
    for( int i = 0; i < cols.length; i++) {
      names[i] = fr._names[cols[i]];
      vres[i]._domain = fr.vecs()[cols[i]]._domain;
    }
    for( int i = cols.length; i < names.length; i++) names[i] = "C"+(i-cols.length+1);
    Frame res = new Frame(names,vres);

    // Delete the group row vecs
    for( Vec v : vecs ) UKV.remove(v._key,fs);
    UKV.remove(envkey,fs);
    fs.blockForPending();
    env.poppush(4,res,null);
  }

  // ---
  // Group descrption: unpacked selected double columns
  protected static class Group extends Iced {
    public double _ds[];
    public int _hash;
    Group( int len ) { _ds = new double[len]; }
    Group( double ds[] ) { _ds = ds; _hash=hash(); }
    // Efficiently allow groups to be hashed & hash-probed
    private void fill( int row, Chunk chks[], int cols[] ) {
      for( int c=0; c<cols.length; c++ ) // For all selection cols
        _ds[c] = chks[cols[c]].at0(row); // Load into working array
      _hash = hash();
    }
    private int hash() {
      long h=0;                 // hash is sum of field bits
      for( double d : _ds ) h += Double.doubleToRawLongBits(d);
      // Doubles are lousy hashes; mix up the bits some
      h ^= (h>>>20) ^ (h>>>12);
      h ^= (h>>> 7) ^ (h>>> 4);
      return (int)((h^(h>>32))&0x7FFFFFFF);
    }
    @Override public boolean equals( Object o ) {
      return o instanceof Group && Arrays.equals(_ds,((Group)o)._ds); }
    @Override public int hashCode() { return _hash; }
    @Override public String toString() { return Arrays.toString(_ds); }
  }


  // ---
  // Pass1: Find unique groups, based on a subset of columns.
  // Collect rows-per-group, locally.
  protected static class ddplyPass1 extends MRTask2<ddplyPass1> {
    // INS:
    private boolean _gatherRows; // TRUE if gathering rows-per-group, FALSE if just getting the groups
    private int _cols[];   // Selection columns
    private Key _uniq;     // Unique Key for this entire ddply pass
    ddplyPass1( boolean rows, int cols[] ) { _gatherRows=rows; _cols = cols; _uniq=Key.make(); }
    // OUTS: mapping from groups to row#s that are in that group
    protected NonBlockingHashMap<Group,NewChunk> _groups;

    // *Local* results from ddplyPass1 are kept locally in this tmp structure.
    // Pass2 reads them out & reclaims the space.
    private static NonBlockingHashMap<Key,ddplyPass1> PASS1TMP = new NonBlockingHashMap<Key,ddplyPass1>();

    // Make a NewChunk to hold rows, that has a random Key and is not
    // associated with any Vec.  We'll fold these into a Vec later when we know
    // cluster-wide what the Groups (and hence Vecs) are.
    private static final NewChunk XNC = new NewChunk(null,H2O.SELF.index());
    private NewChunk makeNC( ) { return _gatherRows==false ? XNC : new NewChunk(null,H2O.SELF.index()); }

    // Build a Map mapping Groups to a NewChunk of row #'s
    @Override public void map( Chunk chks[] ) {
      _groups = new NonBlockingHashMap<Group,NewChunk>();
      Group g = new Group(_cols.length);
      NewChunk nc = makeNC();
      Chunk C = chks[_cols[0]];
      int len = C._len;
      long start = C._start;
      for( int row=0; row<len; row++ ) {
        // Temp array holding the column-selection data
        g.fill(row,chks,_cols);
        NewChunk nc_old = _groups.putIfAbsent(g,nc);
        if( nc_old==null ) {    // Add group signature if not already present
          nc_old = nc;          // Jammed 'nc' into the table to hold rows
          g = new Group(_cols.length); // Need a new <Group,NewChunk> pair
          nc = makeNC();
        }
        if( _gatherRows )             // Gathering rows?
          nc_old.addNum(start+row,0); // Append rows into the existing group
      }
    }
    // Fold together two Group/NewChunk Maps.  For the same Group, append
    // NewChunks (hence gathering rows together).  Since the custom serializers
    // do not send the rows over the wire, we have only *local* row-counts.
    @Override public void reduce( ddplyPass1 p1 ) {
      assert _groups != p1._groups;
      // Fold 2 hash tables together.
      // Get the larger hash table in m0, smaller in m1
      NonBlockingHashMap<Group,NewChunk> m0 =    _groups;
      NonBlockingHashMap<Group,NewChunk> m1 = p1._groups;
      if( m0.size() < m1.size() ) { NonBlockingHashMap<Group,NewChunk> tmp=m0; m0=m1; m1=tmp; }
      // Iterate over smaller table, folding into larger table.
      for( Group g : m1.keySet() ) {
        NewChunk nc0 = m0.get(g);
        NewChunk nc1 = m1.get(g);
        if( nc0 == null ) m0.put(g,nc1);
        // unimplemented: expected to blow out on large row counts, where we
        // actually need a collection of chunks, not 1 uber-chunk
        else if( _gatherRows ) {
          // All longs are monotonically in-order.  Not sure if this is needed
          // but it's an easy invariant to keep and it makes reading row#s easier.
          if( nc0._len > 0 && nc1._len > 0 && // len==0 for reduces from remotes (since no rows sent)
              nc0.at8_impl(nc0._len-1) >= nc1.at8_impl(0) )   nc0.addr(nc1);
          else                                                nc0.add (nc1);
        }
      }
      _groups = m0;
      p1._groups = null;
    }
    @Override public String toString() { return _groups==null ? null : _groups.toString(); }
    // Save local results for pass2
    @Override public void closeLocal() { if( _gatherRows ) PASS1TMP.put(_uniq,this); }

    // Custom serialization for NBHM.  Much nicer when these are auto-gen'd.
    // Only sends Groups over the wire, NOT NewChunks with rows.
    @Override public AutoBuffer write( AutoBuffer ab ) {
      super.write(ab);
      ab.putZ(_gatherRows);
      ab.putA4(_cols);
      ab.put(_uniq);
      if( _groups == null ) return ab.put4(0);
      ab.put4(_groups.size());
      for( Group g : _groups.keySet() ) ab.put(g);
      return ab;
    }

    @Override public ddplyPass1 read( AutoBuffer ab ) {
      super.read(ab);
      assert _groups == null;
      _gatherRows = ab.getZ();
      _cols = ab.getA4();
      _uniq = ab.get();
      int len = ab.get4();
      if( len == 0 ) return this;
      _groups = new NonBlockingHashMap<Group,NewChunk>();
      for( int i=0; i<len; i++ )
        _groups.put(ab.get(Group.class),new NewChunk(null,-99));
      return this;
    }
    @Override public void copyOver( Freezable dt ) {
      ddplyPass1 that = (ddplyPass1)dt;
      super.copyOver(that);
      this._gatherRows = that._gatherRows;
      this._cols   = that._cols;
      this._uniq   = that._uniq;
      this._groups = that._groups;
    }
  }

  // ---
  // Pass 2: Build Groups.
  // Wrap Frame/Vec headers around all the local row-counts.
  private static class ddplyPass2 extends DRemoteTask<ddplyPass2> {
    // Key uniquely identifying a pass1 collection of NewChunks
    Key _p1key;
    // One new Vec per Group, holding just rows
    AppendableVec _avs[];
    // The Group descripters
    double _dss[][];
    // Count of groups-per-node (computed once on home node)
    transient int _nlocals[];

    ddplyPass2( ddplyPass1 p1, int numgrps, int csz ) {
      _p1key = p1._uniq;        // Key to finding the pass1 data
      // One new Vec per Group, holding just rows
      _avs = new AppendableVec[numgrps];
      _dss = new double       [numgrps][];
      _nlocals = new int      [csz];
      int i=0;
      for( Group g : p1._groups.keySet() ) {
        _dss[i] = g._ds;
        _avs[i++] = new AppendableVec(Vec.VectorGroup.VG_LEN1.addVec());
        _nlocals[g.hashCode()%csz]++;
      }
    }

    // Local (per-Node) work.  Gather the chunks together into the Vecs
    @Override public void lcompute() {
      ddplyPass1 p1 = ddplyPass1.PASS1TMP.remove(_p1key);
      Futures fs = new Futures();
      int cidx = H2O.SELF.index();
      for( int i=0; i<_dss.length; i++ ) { // For all possible groups
        // Get the newchunk of local rows for a group
        Group g = new Group(_dss[i]);
        NewChunk nc = p1._groups == null ? null : p1._groups.get(g);
        if( nc != null && nc._len > 0 ) { // Fill in fields we punted on during construction
          nc._vec = _avs[i];  // Assign a proper vector
          nc.close(cidx,fs);  // Close & compress chunk
        } else {              // All nodes have a chunk, even if its empty
          DKV.put(_avs[i].chunkKey(cidx), new C0LChunk(0,0),fs);
        }
      }
      fs.blockForPending();
      _p1key = null;            // No need to return these
      _dss = null;
      tryComplete();
    }
    @Override public void reduce( ddplyPass2 p2 ) {
      for( int i=0; i<_avs.length; i++ )
        _avs[i].reduce(p2._avs[i]);
    }
    // Close all the AppendableVecs & return normal Vecs.
    Vec[] close() {
      Futures fs = new Futures();
      Vec vs[] = new Vec[_avs.length];
      for( int i=0; i<_avs.length; i++ ) vs[i] = _avs[i].close(fs);
      fs.blockForPending();
      return vs;
    }
  }

  // ---
  // Called once-per-group, it executes the given function on the group.
  private static class RemoteExec extends DTask<RemoteExec> implements Freezable {
    // INS
    final int _grpnum, _numgrps; // This group # out of total groups
    double _ds[];                // Displayable name for this group
    Frame _fr;                   // Frame for this group
    Key _envkey;                 // Key for the execution environment
    // OUTS
    int _ncols;                  // Number of result columns

    public static final NonBlockingHashMap<Key,NewChunk[]> _results = new NonBlockingHashMap<Key,NewChunk[]>();

    RemoteExec( int grpnum, int numgrps, double ds[], Frame fr, Key envkey ) {
      _grpnum = grpnum; _numgrps = numgrps; _ds=ds; _fr=fr; _envkey=envkey;
      // Always 1 higher priority than calling thread... because the caller will
      // block & burn a thread waiting for this MRTask2 to complete.
      Thread cThr = Thread.currentThread();
      _priority = (byte)((cThr instanceof H2O.FJWThr) ? ((H2O.FJWThr)cThr)._priority+1 : super.priority());
    }

    final private byte _priority;
    @Override public byte priority() { return _priority; }

    // Execute the function on the group
    @Override public void compute2() {
      Env shared_env = UKV.get(_envkey);
      // Clone a private copy of the environment for local execution
      Env env = shared_env.capture(true);
      ASTOp op = env.fcn(-1);

      env.push(op);
      env.push(_fr);

      op.apply(env,2/*1-arg function*/,null);

      // Inspect the results; figure the result column count
      assert shared_env._sp+1 == env._sp; // Exactly one thing pushed
      Frame fr = null;
      if( env.isAry() && (fr=env.ary(-1)).numRows() != 1 )
        throw new IllegalArgumentException("Result of ddply can only return 1 row but instead returned "+fr.numRows());
      _ncols = fr == null ? 1 : fr.numCols();

      // Inject (once-per-node) an array of NewChunks for results.
      // Racily done by all groups on all nodes; first group with results
      // defines the legal "shape" of the results.
      NewChunk[] nchks = _results.get(_envkey);
      if( nchks == null ) {     // Quick check for existing results array
        nchks = new NewChunk[_ds.length+_ncols]; // Build a suitable results array
        final int cidx = H2O.SELF.index();
        for( int i=0; i<nchks.length; i++ )
          nchks[i] = new NewChunk(null,cidx,_numgrps);
        // Atomically attempt to racily insert
        NewChunk xs[] = _results.putIfMatchUnlocked(_envkey,nchks,null);
        if( xs != null ) nchks=xs; // Keep any prior, if we lost the race
      } else if( nchks.length != _ds.length+_ncols ) {
        throw new IllegalArgumentException("Results of ddply must return the same column count, but one group returned "+(nchks.length-_ds.length)+" columns and this group is returning "+_ncols);
      }

      // Copy the data into the NewChunks
      for( int i=0; i<_ds.length; i++ ) nchks[i].set_impl(_grpnum,_ds[i]); // The group data
      Vec vecs[] = fr==null ? null : fr.vecs();
      for( int i=0; i<_ncols; i++ ) // The group results
        nchks[_ds.length+i].set_impl(_grpnum,fr==null ? env.dbl(-1) : vecs[i].at(0));
      if( fr != null ) fr.delete();

      // No need to return any results here.
      _fr.delete();
      _fr = null;
      _ds = null;
      _envkey= null;
      tryComplete();
    }
  }

  // ---
  // Pass 4: Build Groups.
  // Wrap Frame/Vec headers around all the local row-counts.
  private static class ddplyPass4 extends DRemoteTask<ddplyPass4> {
    // Key uniquely identifying the results
    Key _envkey;
    // One new Vec per result column
    AppendableVec _avs[];
    ddplyPass4( Key envkey, int ccols, int ncols ) {
      _envkey = envkey;
      // Result AppendableVecs
      Key keys[] = Vec.VectorGroup.VG_LEN1.addVecs(ccols+ncols);
      _avs = new AppendableVec[ccols+ncols];
      for( int i=0; i<_avs.length; i++ )
        _avs[i] = new AppendableVec(keys[i]);
    }
    // Local (per-Node) work.  Gather the chunks together into the Vecs
    @Override public void lcompute() {
      NewChunk nchks[] = RemoteExec._results.remove(_envkey);
      if( nchks != null ) {
        if( nchks.length != _avs.length )
          throw new IllegalArgumentException("Results of ddply must return the same column count, but one group returned "+nchks.length+" columns and this group is returning "+_avs.length);
        Futures fs = new Futures();
        for( int i=0; i<_avs.length; i++ ) {
          NewChunk nc = nchks[i];
          nc._vec = _avs[i];      // Assign a proper vector
          nc.close(fs);           // Close & compress chunk
        }
        fs.blockForPending();
      }
      _envkey = null;           // No need to return these
      tryComplete();
    }
    @Override public void reduce( ddplyPass4 p4 ) {
      for( int i=0; i<_avs.length; i++ )
        _avs[i].reduce(p4._avs[i]);
    }
    // Close all the AppendableVecs & return normal Vecs.
    Vec[] close() {
      Futures fs = new Futures();
      Vec vs[] = new Vec[_avs.length];
      for( int i=0; i<_avs.length; i++ ) vs[i] = _avs[i].close(fs);
      fs.blockForPending();
      return vs;
    }
  }
}


// --------------------------------------------------------------------------
// unique(ary)
// Returns only the unique rows

class ASTUnique extends ASTddply {
  static final String VARS[] = new String[]{ "", "ary"};
  ASTUnique( ) { super(VARS, new Type[]{ Type.ARY, Type.ARY }); }
  @Override String opStr(){ return "unique";}
  @Override ASTOp make() {return new ASTUnique();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Thread cThr = Thread.currentThread();
    int priority = (cThr instanceof H2O.FJWThr) ? ((H2O.FJWThr)cThr)._priority : -1;
    Frame fr = env.peekAry();
    int cols[] = new int[fr.numCols()];
    for( int i=0; i<cols.length; i++ ) cols[i]=i;
    ddplyPass1 p1 = new ddplyPass1( false, cols ).doAll(fr);
    double dss[][] = new double[p1._groups.size()][];
    int i=0;
    for( Group g : p1._groups.keySet() )
      dss[i++] = g._ds;
    Frame res = FrameUtils.frame(fr._names,dss);
    env.poppush(2,res,null);
  }
}
