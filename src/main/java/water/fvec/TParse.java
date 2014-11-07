package water.fvec;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.nbhm.NonBlockingHashMap;
import water.parser.*;
import water.parser.CustomParser.StreamDataOut;
import water.parser.Enum;
import water.util.FrameUtils;
import water.util.Log;
import water.util.Utils.IcedHashMap;
import water.util.Utils.IcedInt;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public final class TParse extends Job {
  public final Key  _progress;      // Job progress Key
  private MultiFileTParseTask _mfpt; // Access to partially built vectors for cleanup after parser crash
  public static enum Compression { NONE, ZIP, GZIP }

  public static Key [] filterEmptyFiles(Key [] keys){
    Arrays.sort(keys);
    // first check if there are any empty files and if so remove them
    Vec [] vecs = new Vec [keys.length];
    int c = 0;
    for(int i = 0; i < vecs.length; ++i) {
      vecs[i] = getVec(keys[i]);
      if(vecs[i].length() == 0) c++;
    }
    if(c > 0){ // filter out empty files
      Key[] ks = new Key[keys.length-c];
      Vec[] vs = new Vec[vecs.length-c];
      int j = 0;
      for(int i = 0; i < keys.length; ++i)
        if(vecs[i].length() != 0){
          ks[j] = keys[i];
          vs[j] = vecs[i];
          ++j;
        }
      keys = ks;
    }
    return keys;
  }
  // --------------------------------------------------------------------------
  // Parse an array of csv input/file keys into an array of distributed output Vecs
  public static Frame parse(Key okey, Key [] keys) {
    return parse(okey,keys,true);
  }

  public static Frame parse(Key okey, Key[] keys, boolean delete_on_done) {
    return forkParseDataset(okey, keys, delete_on_done).get();
  }
  // Same parse, as a backgroundable Job
  public static TParse forkParseDataset(final Key dest, Key[] keys, boolean delete_on_done) {
    keys = filterEmptyFiles(keys);
    // Some quick sanity checks: no overwriting your input key, and a resource check.
    long sum=0;
    for( Key k : keys ) {
      if( dest.equals(k) )
        throw new IllegalArgumentException("Destination key "+dest+" must be different from all sources");
      sum += DKV.get(k).length(); // Sum of all input filesizes
    }
    long memsz=0;               // Cluster memory
    for( H2ONode h2o : H2O.CLOUD._memary )
      memsz += h2o.get_max_mem();
    if( sum > memsz*4 )
      throw new IllegalArgumentException("Total input file size of "+PrettyPrint.bytes(sum)+" is much larger than total cluster memory of "+PrettyPrint.bytes(memsz)+", please use either a larger cluster or smaller data.");

    TParse job = new TParse(dest, keys);
    new Frame(job.dest(),new String[0],new Vec[0]).delete_and_lock(job.self()); // Lock BEFORE returning
    for( Key k : keys ) Lockable.read_lock(k,job.self()); // Lock BEFORE returning
    TParserFJTask fjt = new TParserFJTask(job, keys, delete_on_done); // Fire off background parse
    // Make a wrapper class that only *starts* when the ParserFJTask fjt
    // completes - especially it only starts even when fjt completes
    // exceptionally... thus the fjt onExceptionalCompletion code runs
    // completely before this empty task starts - providing a simple barrier.
    // Threads blocking on the job will block on the "cleanup" task, which will
    // block until the fjt runs the onCompletion or onExceptionCompletion code.
    H2OCountedCompleter cleanup = new H2OCountedCompleter() {
      @Override public void compute2() { }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) { return true; }
    };
    fjt.setCompleter(cleanup);
    job.start(cleanup);
    H2O.submitTask(fjt);
    return job;
  }
  // Setup a private background parse job
  private TParse(Key dest, Key[] fkeys) {
    destination_key = dest;
    // Job progress Key
    _progress = Key.make((byte) 0, Key.JOB);
    UKV.put(_progress, ParseProgress.make(fkeys));
  }

  // Simple internal class doing background parsing, with trackable Job status
  public static class TParserFJTask extends H2OCountedCompleter {
    final TParse _job;
    Key[] _keys;
    boolean _delete_on_done;

    public TParserFJTask(TParse job, Key[] keys, boolean delete_on_done) {
      _job = job;
      _keys = keys;
      _delete_on_done = delete_on_done;
    }
    @Override public void compute2() {
      parse_impl(_job, _keys, _delete_on_done);
      tryComplete();
    }

    // Took a crash/NPE somewhere in the parser.  Attempt cleanup.
    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
      Futures fs = new Futures();
      if( _job != null ) {
        UKV.remove(_job.destination_key,fs);
        UKV.remove(_job._progress,fs);
        // Find & remove all partially-built output vecs & chunks
        if( _job._mfpt != null ) _job._mfpt.onExceptionCleanup(fs);
      }
      // Assume the input is corrupt - or already partially deleted after
      // parsing.  Nuke it all - no partial Vecs lying around.
      for( Key k : _keys ) UKV.remove(k,fs);
      fs.blockForPending();
      // As soon as the job is canceled, threads blocking on the job will
      // wake up.  Better have all cleanup done first!
      if( _job != null ) _job.cancel(ex);
      return true;
    }
  }

  // --------------------------------------------------------------------------
  // Parser progress
  static class ParseProgress extends Iced {
    final long _total;
    long _value;
    DException _ex;
    ParseProgress(long val, long total){_value = val; _total = total;}
    // Total number of steps is equal to total bytecount across files
    static ParseProgress make( Key[] fkeys ) {
      long total = 0;
      for( Key fkey : fkeys )
        total += getVec(fkey).length();
      return new ParseProgress(0,total);
    }
    public void setException(DException ex){_ex = ex;}
    public DException getException(){return _ex;}
  }
  static void onProgress(final long len, final Key progress) {
    new TAtomic<ParseProgress>() {
      @Override public ParseProgress atomic(ParseProgress old) {
        if (old == null) return null;
        old._value += len;
        return old;
      }
    }.fork(progress);
  }

  @Override public float progress() {
    ParseProgress progress = UKV.get(_progress);
    if( progress == null || progress._total == 0 ) return 0;
    return progress._value / (float) progress._total;
  }
  @Override public void remove() {
    DKV.remove(_progress);
    super.remove();
  }

  /** Task to update enum values to match the global numbering scheme.
   *  Performs update in place so that values originally numbered using
   *  node-local unordered numbering will be numbered using global numbering.
   *  @author tomasnykodym
   */
  private static class EnumUpdateTask extends MRTask2<EnumUpdateTask> {
    private transient int[][][] _emap;
    final Key _eKey;
    private final ValueString [][] _gDomain;
    private final Enum [][] _lEnums;
    private final int  [] _chunk2Enum;
    private final int [] _colIds;
    private EnumUpdateTask(ValueString [][] gDomain, Enum [][]  lEnums, int [] chunk2Enum, Key lDomKey, int [] colIds){
      _gDomain = gDomain; _lEnums = lEnums; _chunk2Enum = chunk2Enum; _eKey = lDomKey; _colIds = colIds;
    }

    private int[][] emap(int nodeId) {
      if( _emap == null ) _emap = new int[_lEnums.length][][];
      if( _emap[nodeId] == null ) {
        int[][] emap = new int[_gDomain.length][];
        for( int i = 0; i < _gDomain.length; ++i ) {
          if( _gDomain[i] != null ) {
            assert _lEnums[nodeId] != null : "missing lEnum of node "  + nodeId + ", enums = " + Arrays.toString(_lEnums);
            final Enum e = _lEnums[nodeId][_colIds[i]];
            emap[i] = new int[e.maxId()+1];
            Arrays.fill(emap[i], -1);
            for(int j = 0; j < _gDomain[i].length; ++j) {
              ValueString vs = _gDomain[i][j];
              if( e.containsKey(vs) ) {
                assert e.getTokenId(vs) <= e.maxId():"maxIdx = " + e.maxId() + ", got " + e.getTokenId(vs);
                emap[i][e.getTokenId(vs)] = j;
              }
            }
          }
        }
        _emap[nodeId] = emap;
      }
      return _emap[nodeId];
    }

    @Override public void map(Chunk [] chks){
      int[][] emap = emap(_chunk2Enum[chks[0].cidx()]);
      final int cidx = chks[0].cidx();
      for(int i = 0; i < chks.length; ++i) {
        Chunk chk = chks[i];
        if(_gDomain[i] == null) // killed, replace with all NAs
          DKV.put(chk._vec.chunkKey(chk.cidx()),new C0DChunk(Double.NaN,chk._len));
        else for( int j = 0; j < chk._len; ++j){
          if( chk.isNA0(j) )continue;
          long l = chk.at80(j);
          if (l < 0 || l >= emap[i].length)
            reportBrokenEnum(chk, i, j, l, emap);
          if(emap[i][(int)l] < 0)
            throw new RuntimeException(H2O.SELF.toString() + ": missing enum at col:" + i + ", line: " + j + ", val = " + l + ", chunk=" + chk.getClass().getSimpleName());
          chk.set0(j, emap[i][(int)l]);
        }
        chk.close(cidx, _fs);
      }
    }
    private void reportBrokenEnum( Chunk chk, int i, int j, long l, int[][] emap ) {
      Chunk chk2 = chk._chk2;
      chk._chk2 = null;
      StringBuilder sb = new StringBuilder("Enum renumber task, column # " + i + ": Found OOB index " + l + " (expected 0 - " + emap[i].length + ", global domain has " + _gDomain[i].length + " levels) pulled from " + chk.getClass().getSimpleName() +  "\n");
      int k = 0;
      for(; k < Math.min(5,chk._len); ++k)
        sb.append("at8[").append(k + chk._start).append("] = ").append(chk.at80(k)).append(", chk2 = ").append(chk2 != null ? chk2.at80(k) : "").append("\n");
      k = Math.max(k,j-2);
      sb.append("...\n");
      for(; k < Math.min(chk._len,j+2); ++k)
        sb.append("at8[").append(k + chk._start).append("] = ").append(chk.at80(k)).append(", chk2 = ").append(chk2 != null ? chk2.at80(k) : "").append("\n");
      sb.append("...\n");
      k = Math.max(k,chk._len-5);
      for(; k < chk._len; ++k)
        sb.append("at8[").append(k + chk._start).append("] = ").append(chk.at80(k)).append(", chk2 = ").append(chk2 != null ? chk2.at80(k) : "").append("\n");
      throw new RuntimeException(sb.toString());
    }
  }

  // --------------------------------------------------------------------------
//  private static class EnumFetchTask extends MRTask<EnumFetchTask> {
//    private final Key _k;
//    private final int[] _ecols;
//    private final int _homeNode; // node where the computation started, enum from this node MUST be cloned!
//    private Enum[] _gEnums;      // global enums per column
//    private Enum[][] _lEnums;    // local enums per node per column
//    private EnumFetchTask(int homeNode, Key k, int[] ecols){_homeNode = homeNode; _k = k;_ecols = ecols;}
//    @Override public void map(Key key) {
//      _lEnums = new Enum[H2O.CLOUD.size()][];
//      if(MultiFileTParseTask._enums.containsKey(_k)){
//        _lEnums[H2O.SELF.index()] = _gEnums = MultiFileTParseTask._enums.get(_k);
//        // if we are the original node (i.e. there will be no sending over
//        // wire), we have to clone the enums not to share the same object
//        // (causes problems when computing column domain and renumbering maps).
//        if( H2O.SELF.index() == _homeNode ) {
//          _gEnums = _gEnums.clone();
//          for(int i = 0; i < _gEnums.length; ++i)
//            _gEnums[i] = _gEnums[i].clone();
//        }
//        MultiFileTParseTask._enums.remove(_k);
//      }
//    }
//
//    @Override public void reduce(EnumFetchTask etk) {
//      if(_gEnums == null) {
//        _gEnums = etk._gEnums;
//        _lEnums = etk._lEnums;
//      } else if (etk._gEnums != null) {
//        for( int i : _ecols ) _gEnums[i].merge(etk._gEnums[i]);
//        for( int i = 0; i < _lEnums.length; ++i )
//          if( _lEnums[i] == null ) _lEnums[i] = etk._lEnums[i];
//          else assert etk._lEnums[i] == null;
//      }
//    }
//  }

  // --------------------------------------------------------------------------
  // Run once on all nodes; fill in missing zero chunks
  private static class SVFTask extends MRTask<SVFTask> {
    private final Frame _f;
    private SVFTask( Frame f ) { _f = f; }
    @Override public void map(Key key) {
      Vec v0 = _f.anyVec();
      for( int i = 0; i < v0.nChunks(); ++i ) {
        if( !v0.chunkKey(i).home() ) continue;
        // First find the nrows as the # rows of non-missing chunks; done on
        // locally-homed chunks only - to keep the data distribution.
        int nlines = 0;
        for( Vec vec : _f.vecs() ) {
          Value val = H2O.get(vec.chunkKey(i)); // Local-get only
          if( val != null ) {
            nlines = ((Chunk)val.get())._len;
            break;
          }
        }

        // Now fill in appropriate-sized zero chunks
        for( Vec vec : _f.vecs() ) {
          Key k = vec.chunkKey(i);
          if( !k.home() ) continue; // Local keys only
          Value val = H2O.get(k);   // Local-get only
          if( val == null )         // Missing?  Fill in w/zero chunk
            H2O.putIfMatch(k, new Value(k,new C0DChunk(0, nlines)), null);
        }
      }
    }
    @Override public void reduce( SVFTask drt ) {}
  }

  private static Vec getVec(Key key) {
    Object o = UKV.get(key);
    return o instanceof Vec ? (ByteVec) o : ((Frame) o).vecs()[0];
  }
  private static String [] genericColumnNames(int ncols){
    String [] res = new String[ncols];
    for(int i = 0; i < res.length; ++i) res[i] = "C" + String.valueOf(i+1);
    return res;
  }

  // Log information about the dataset we just parsed.
  private static void logParseResults(TParse job, Frame fr) {
    try {
      long numRows = fr.anyVec().length();
      Log.info("Parse result for " + job.dest() + " (" + Long.toString(numRows) + " rows):");

      Vec[] vecArr = fr.vecs();
      for( int i = 0; i < vecArr.length; i++ ) {
        Vec v = vecArr[i];
        boolean isCategorical = v.isEnum();
        boolean isConstant = (v.min() == v.max());
        String CStr = String.format("C%d:", i+1);
        String typeStr = String.format("%s", (v._isUUID ? "UUID" : (isCategorical ? "categorical" : "numeric")));
        String minStr = String.format("min(%f)", v.min());
        String maxStr = String.format("max(%f)", v.max());
        long numNAs = v.naCnt();
        String naStr = (numNAs > 0) ? String.format("na(%d)", numNAs) : "";
        String isConstantStr = isConstant ? "constant" : "";
        String numLevelsStr = isCategorical ? String.format("numLevels(%d)", v.domain().length) : "";

        boolean printLogSeparatorToStdout = false;
        boolean printColumnToStdout;
        {
          // Print information to stdout for this many leading columns.
          final int MAX_HEAD_TO_PRINT_ON_STDOUT = 10;

          // Print information to stdout for this many trailing columns.
          final int MAX_TAIL_TO_PRINT_ON_STDOUT = 10;

          if (vecArr.length <= (MAX_HEAD_TO_PRINT_ON_STDOUT + MAX_TAIL_TO_PRINT_ON_STDOUT)) {
            // For small numbers of columns, print them all.
            printColumnToStdout = true;
          } else if (i < MAX_HEAD_TO_PRINT_ON_STDOUT) {
            printColumnToStdout = true;
          } else if (i == MAX_HEAD_TO_PRINT_ON_STDOUT) {
            printLogSeparatorToStdout = true;
            printColumnToStdout = false;
          } else if ((i + MAX_TAIL_TO_PRINT_ON_STDOUT) < vecArr.length) {
            printColumnToStdout = false;
          } else {
            printColumnToStdout = true;
          }
        }

        if (printLogSeparatorToStdout) {
          System.out.println("Additional column information only sent to log file...");
        }

        if (printColumnToStdout) {
          // Log to both stdout and log file.
          Log.info(String.format("    %-8s %15s %20s %20s %15s %11s %16s", CStr, typeStr, minStr, maxStr, naStr, isConstantStr, numLevelsStr));
        }
        else {
          // Log only to log file.
          Log.info_no_stdout(String.format("    %-8s %15s %20s %20s %15s %11s %16s", CStr, typeStr, minStr, maxStr, naStr, isConstantStr, numLevelsStr));
        }
      }
      Log.info(FrameUtils.chunkSummary(fr).toString());
    }
    catch (Exception ignore) {}   // Don't fail due to logging issues.  Just ignore them.
  }

  // --------------------------------------------------------------------------
  // Top-level parser driver
  private static void parse_impl(TParse job, Key[] fkeys, boolean delete_on_done) {
    if( fkeys.length == 0) { job.cancel();  return;  }
    // Remove any previous instance and insert a sentinel (to ensure no one has
    // been writing to the same keys during our parse)!
    MultiFileTParseTask mfpt = job._mfpt = new MultiFileTParseTask(job._progress);
    mfpt.invoke(fkeys);
    NonBlockingHashMap<Key, Doc> docs = mfpt.docs();
    AppendableVec[] appendableVecs = new AppendableVec[mfpt._dout._doc.size()];


    // Calculate enum domain
//    int n = 0;
//    int [] ecols = new int[mfpt._dout._nCols];
//    for( int i = 0; i < ecols.length; ++i )
//      if(mfpt._dout._vecs[i].shouldBeEnum())
//        ecols[n++] = i;
//    ecols =  Arrays.copyOf(ecols, n);
//    if( ecols.length > 0 ) {
//      EnumFetchTask eft = new EnumFetchTask(H2O.SELF.index(), mfpt._eKey, ecols).invokeOnAllNodes();
//      Enum[] enums = eft._gEnums;
//      ValueString[][] ds = new ValueString[ecols.length][];
//      int j = 0;
//      for( int i : ecols ) mfpt._dout._vecs[i]._domain = ValueString.toString(ds[j++] = enums[i].computeColumnDomain());
//      eut = new EnumUpdateTask(ds, eft._lEnums, mfpt._chunk2Enum, mfpt._eKey, ecols);
//    }
//    Frame fr = new Frame(job.dest(),setup._columnNames != null?setup._columnNames:genericColumnNames(mfpt._dout._nCols),mfpt._dout.closeVecs());
//    // SVMLight is sparse format, there may be missing chunks with all 0s, fill them in
//    new SVFTask(fr).invokeOnAllNodes();
//    // Update enums to the globally agreed numbering
//    if( eut != null ) {
//      Vec[] evecs = new Vec[ecols.length];
//      for( int i = 0; i < evecs.length; ++i ) evecs[i] = fr.vecs()[ecols[i]];
//      eut.doAll(evecs);
//    }
//
//    logParseResults(job, fr);
//
//    // Release the frame for overwriting
//    fr.unlock(job.self());
//    // Remove CSV files from H2O memory
    if( delete_on_done ) for( Key k : fkeys ) Lockable.delete(k,job.self());
    else for( Key k : fkeys ) {
      Lockable l = UKV.get(k);
      l.unlock(job.self());
    }
    job.remove();
  }

  // --------------------------------------------------------------------------
  // We want to do a standard MRTask with a collection of file-keys (so the
  // files are parsed in parallel across the cluster), but we want to throttle
  // the parallelism on each node.
  private static class MultiFileTParseTask extends MRTask<MultiFileTParseTask> {
    // Shared against all concurrent unrelated parses, a map to the node-local
    // The Key used to sort out *this* parse's Doc[]
    private Key _progress;
    // Mapping from Chunk# to cluster-node-number holding the enum mapping.
    // It is either self for all the non-parallel parses, or the Chunk-home for parallel parses.
    // All column data for this one file
    // A mapping of Key+ByteVec to rolling total Chunk counts.
    private IcedHashMap<Key,IcedInt> _fileChunkOffsets;

    // OUTPUT fields:
    TParseDataOut _dout;
    public transient NonBlockingHashMap<Key, Doc> _docs;
    public String _parserr;              // NULL if parse is OK, else an error string

    MultiFileTParseTask(Key progress) {
      _progress = progress;
      _runSingleThreaded = true;
    }

    @Override public MultiFileTParseTask dfork(Key... keys) {
      _fileChunkOffsets = new IcedHashMap<Key, IcedInt>();
      int len = 0;
      for( Key k:keys) {
        _fileChunkOffsets.put(k,new IcedInt(len));
        len += getVec(k).nChunks();
      }

      // Mapping from Chunk# to cluster-node-number
      return super.dfork(keys);
    }

    // Called once per file
    @Override public void map( Key key ) {
      // Get parser setup info for this chunk
      ByteVec vec = (ByteVec) getVec(key);
      byte[] bits = vec.chunkForChunkIdx(0)._mem;
      if (bits == null || bits.length == 0) {
        assert false : "encountered empty file during multifile parse? should've been filtered already";
        return; // should not really get here
      }
      final int chunkStartIdx = _fileChunkOffsets.get(key)._val;

      // Parse the file -- for now assume no compression no parallelParse
      try {
        TParseProgressMonitor pmon = new TParseProgressMonitor(_progress);
        _dout = streamParse(vec.openStream(pmon), chunkStartIdx);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    // Reduce: combine errors from across files.
    // Roll-up other meta data
    @Override public void reduce( MultiFileTParseTask mfpt ) {
      assert this != mfpt;
      // Combine parse errors from across files
      if( _parserr == null ) _parserr = mfpt._parserr;
      else if( mfpt._parserr != null ) _parserr += mfpt._parserr;
      // Collect & combine columns across files
      if( _dout == null ) _dout = mfpt._dout;
      else _dout.reduce(mfpt._dout);
    }

    @Override protected void postGlobal() {
      System.out.println("MY NODE IS:"+ H2O.SELF.index()+ "; KEYS IN _docs: "+_docs.size());
    }

    public NonBlockingHashMap<Key,Doc> docs() { return _docs; }

    private Doc docs(Key eKey, int nstr){
      if (_docs == null) {
        _docs = new NonBlockingHashMap<Key, Doc>();
      }
      if(!_docs.containsKey(eKey)){
        Doc docs = new Doc();
        _docs.putIfAbsent(eKey, docs);
      }
      return _docs.get(eKey);
    }

    public void filter() {

    }

    // ------------------------------------------------------------------------
    // Zipped file; no parallel decompression; decompress into local chunks,
    // parse local chunks; distribute chunks later.
    private TParseDataOut streamParse( final InputStream is, int chunkStartIdx) throws IOException {
      // All output into a fresh pile of NewChunks, one per column
      TParseDataOut dout = new TParseDataOut(chunkStartIdx, docs(Key.make(),1));
      TextParser p = new TextParser();
      // assume 2x inflation rate
      try{p.streamParse(is, dout);}catch(IOException e){throw new RuntimeException(e);} catch (Exception e) {
        e.printStackTrace();
      }
      return dout;
    }

    // Find & remove all partially built output chunks & vecs
    private Futures onExceptionCleanup(Futures fs) {
      cancel(true);
      return fs;
    }

//    @Override public AutoBuffer write( AutoBuffer ab ) {
//      super.write(ab);
//      _fileChunkOffsets.write(ab);
//      if( _docs == null ) return ab.put4(0);
//      ab.put4(_docs.size());
//      for( Key k : _docs.keySet() ) {
//        ab.put(k);
//        ab.put4((_docs.get(k)).length);
//        for (Doc d: _docs.get(k)) d.write(ab);
//      }
//      return ab;
//    }
//
//    @Override public MultiFileTParseTask read( AutoBuffer ab ) {
//      super.read(ab);
//      _fileChunkOffsets = new IcedHashMap<Key, IcedInt>();
//      int n = ab.get4();
//      for(int i = 0; i < n; ++i)
//        _fileChunkOffsets.put(ab.<Key>get(), ab.<IcedInt>get());
//
//      if (_docs == null) { _docs = new NonBlockingHashMap<Key, Doc[]>(); }
//      int len = ab.get4();
//      if( len == 0 ) return this;
//      for( int i=0; i<len; i++ ) {
//        Key k = ab.get(Key.class);
//        int ndocs = ab.get4();
//        Doc[] docs = new Doc[ndocs];
//        for (int d = 0; d < ndocs; ++d) {
//          Doc doc = new Doc();
//          int ll = ab.get4();
//          if (ll == 0) {
//            docs[d] = doc;
//            continue;
//          }
//          for (int t = 0; t < ll; ++t) {
//            int l = ab.get2();
//            doc.put(new ValueString(ab.getA1(l)), ab.get8());
//          }
//          docs[d] = ab.get();
//        }
//        _docs.put(k, docs);
//      }
//      return this;
//    }
//    @Override public void copyOver( Freezable dt ) {
//      MultiFileTParseTask that = (MultiFileTParseTask)dt;
//      super.copyOver(that);
//      this._docs = that._docs;
////      this._dout = that._dout;
//    }
  }

  // ------------------------------------------------------------------------
  /** Parse Some Text Data
   */
  static class TParseDataOut extends Iced implements CustomParser.StreamDataOut {
    private final Doc _doc;
    final int _cidx;

    public TParseDataOut(int cidx, Doc doc) {
      _doc = doc;
      _cidx = cidx;
    }

    @Override public TParseDataOut reduce(StreamDataOut sdout){
      TParseDataOut dout = (TParseDataOut)sdout;
      if (dout._doc == null || _doc == null) return this;
      _doc.merge(dout._doc);
      return this;
    }
    @Override public TParseDataOut nextChunk(){ return new TParseDataOut(_cidx+1, _doc); }
    @Override public final void addStrCol(int colIdx, ValueString str) { _doc.addKey(str); }


    /* Empty methods not needed here */
    @Override public TParseDataOut close(){  return null;  }
    @Override public TParseDataOut close(Futures fs){ return null; }
    @Override public void addNumCol(int colIdx, double value) {    }
    @Override public void addInvalidCol(int colIdx) {}
    @Override public void setColumnNames(String [] names){}
    @Override public void newLine() {    }
    @Override public final void rollbackLine() {}
    @Override public void invalidLine(String err) { newLine(); }
    @Override public void invalidValue(int line, int col) {}
    @Override public final boolean isString(int colIdx) { return false; }
    @Override public void addNumCol(int colIdx, long number, int exp) {}
  }

  public static class TParseProgressMonitor extends Iced implements Job.ProgressMonitor {
    final Key _progressKey;
    private long _progress;
    public TParseProgressMonitor(Key pKey){_progressKey = pKey;}
    @Override public void update(long n) {
      TParse.onProgress(n, _progressKey);
      _progress += n;
    }
    public long progress() {
      return _progress;
    }
  }
}
