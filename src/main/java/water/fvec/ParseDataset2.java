package water.fvec;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.*;
import jsr166y.CountedCompleter;
import jsr166y.ForkJoinTask;
import jsr166y.ForkJoinWorkerThread;
import jsr166y.RecursiveAction;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.Vec.VectorGroup;
import water.nbhm.NonBlockingHashMap;
import water.nbhm.NonBlockingSetInt;
import water.parser.*;
import water.parser.CustomParser.DataOut;
import water.parser.CustomParser.ParserSetup;
import water.parser.CustomParser.ParserType;
import water.parser.CustomParser.StreamDataOut;
import water.parser.Enum;
import water.util.FrameUtils;
import water.util.Log;
import water.util.Utils.IcedHashMap;
import water.util.Utils.IcedInt;
import water.util.Utils;

public final class ParseDataset2 extends Job {
  public final Key  _progress;  // Job progress Key
  private MultiFileParseTask _mfpt; // Access to partially built vectors for cleanup after parser crash
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
    return parse(okey,keys,new GuessSetup.GuessSetupTsk(new ParserSetup(),true).invoke(keys)._gSetup._setup,true);
  }

  public static Frame parse(Key okey, Key[] keys, CustomParser.ParserSetup globalSetup, boolean delete_on_done) {
    if( globalSetup._ncols == 0 ) throw new java.lang.IllegalArgumentException(globalSetup.toString());
    return forkParseDataset(okey, keys, globalSetup, delete_on_done).get();
  }
  // Same parse, as a backgroundable Job
  public static ParseDataset2 forkParseDataset(final Key dest, Key[] keys, final CustomParser.ParserSetup setup, boolean delete_on_done) {
    keys = filterEmptyFiles(keys);
    setup.checkDupColumnNames();
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

    ParseDataset2 job = new ParseDataset2(dest, keys);
    new Frame(job.dest(),new String[0],new Vec[0]).delete_and_lock(job.self()); // Lock BEFORE returning
    for( Key k : keys ) Lockable.read_lock(k,job.self()); // Lock BEFORE returning
    ParserFJTask fjt = new ParserFJTask(job, keys, setup, delete_on_done); // Fire off background parse
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
  private ParseDataset2(Key dest, Key[] fkeys) {
    destination_key = dest;
    // Job progress Key
    _progress = Key.make((byte) 0, Key.JOB);
    UKV.put(_progress, ParseProgress.make(fkeys));
  }

  // Simple internal class doing background parsing, with trackable Job status
  public static class ParserFJTask extends H2OCountedCompleter {
    final ParseDataset2 _job;
    Key[] _keys;
    CustomParser.ParserSetup _setup;
    boolean _delete_on_done;

    public ParserFJTask( ParseDataset2 job, Key[] keys, CustomParser.ParserSetup setup, boolean delete_on_done) {
      _job = job;
      _keys = keys;
      _setup = setup;
      _delete_on_done = delete_on_done;
    }
    @Override public void compute2() {
      parse_impl(_job, _keys, _setup, _delete_on_done);
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
        sb.append("at8[" + (k+chk._start) + "] = " + chk.at80(k) + ", chk2 = " + (chk2 != null?chk2.at80(k):"") + "\n");
      k = Math.max(k,j-2);
      sb.append("...\n");
      for(; k < Math.min(chk._len,j+2); ++k)
        sb.append("at8[" + (k+chk._start) + "] = " + chk.at80(k) + ", chk2 = " + (chk2 != null?chk2.at80(k):"") + "\n");
      sb.append("...\n");
      k = Math.max(k,chk._len-5);
      for(; k < chk._len; ++k)
        sb.append("at8[" + (k+chk._start) + "] = " + chk.at80(k) + ", chk2 = " + (chk2 != null?chk2.at80(k):"") + "\n");
      throw new RuntimeException(sb.toString());
    }
  }

  // --------------------------------------------------------------------------
  private static class EnumFetchTask extends MRTask<EnumFetchTask> {
    private final Key _k;
    private final int[] _ecols;
    private final int _homeNode; // node where the computation started, enum from this node MUST be cloned!
    private Enum[] _gEnums;      // global enums per column
    private Enum[][] _lEnums;    // local enums per node per column
    private EnumFetchTask(int homeNode, Key k, int[] ecols){_homeNode = homeNode; _k = k;_ecols = ecols;}
    @Override public void map(Key key) {
      _lEnums = new Enum[H2O.CLOUD.size()][];
      if(MultiFileParseTask._enums.containsKey(_k)){
        _lEnums[H2O.SELF.index()] = _gEnums = MultiFileParseTask._enums.get(_k);
        // if we are the original node (i.e. there will be no sending over
        // wire), we have to clone the enums not to share the same object
        // (causes problems when computing column domain and renumbering maps).
        if( H2O.SELF.index() == _homeNode ) {
          _gEnums = _gEnums.clone();
          for(int i = 0; i < _gEnums.length; ++i)
            _gEnums[i] = _gEnums[i].clone();
        }
        MultiFileParseTask._enums.remove(_k);
      }
    }

    @Override public void reduce(EnumFetchTask etk) {
      if(_gEnums == null) {
        _gEnums = etk._gEnums;
        _lEnums = etk._lEnums;
      } else if (etk._gEnums != null) {
        for( int i : _ecols ) _gEnums[i].merge(etk._gEnums[i]);
        for( int i = 0; i < _lEnums.length; ++i )
          if( _lEnums[i] == null ) _lEnums[i] = etk._lEnums[i];
          else assert etk._lEnums[i] == null;
      }
    }
  }

  // --------------------------------------------------------------------------
  // Run once on all nodes; fill in missing zero chunks
  private static class SVFTask extends MRTask<SVFTask> {
    private final Frame _f;
    private SVFTask( Frame f ) { _f = f; }
    @Override public void map(Key key) {
      Vec v0 = _f.anyVec();
      ArrayList<RecursiveAction> rs = new ArrayList<RecursiveAction>();
      for( int i = 0; i < v0.nChunks(); ++i ) {
        if( !v0.chunkKey(i).home() ) continue;
        final int fi = i;
        rs.add(new RecursiveAction() {
          @Override
          protected void compute() {
            // First find the nrows as the # rows of non-missing chunks; done on
            // locally-homed chunks only - to keep the data distribution.
            int nlines = 0;
            for( Vec vec : _f.vecs() ) {
              Value val = H2O.get(vec.chunkKey(fi)); // Local-get only
              if( val != null ) {
                nlines = ((Chunk)val.get())._len;
                break;
              }
            }
            final int fnlines = nlines;
            // Now fill in appropriate-sized zero chunks
            for(int j = 0; j < _f.numCols(); ++j) {
              Vec vec = _f.vec(j);
              Key k = vec.chunkKey(fi);
              Value val = H2O.get(k);   // Local-get only
              if( val == null )         // Missing?  Fill in w/zero chunk
                H2O.putIfMatch(k, new Value(k, new C0DChunk(0, fnlines)), null);
            }
          }
        });
      }
      ForkJoinTask.invokeAll(rs);
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
  private static void logParseResults(ParseDataset2 job, Frame fr) {
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
  private static void parse_impl(ParseDataset2 job, Key[] fkeys, CustomParser.ParserSetup setup, boolean delete_on_done) {
    assert setup._ncols > 0;
    if( fkeys.length == 0) { job.cancel();  return;  }
    // Remove any previous instance and insert a sentinel (to ensure no one has
    // been writing to the same keys during our parse)!
    Vec v = getVec(fkeys[0]);
    int reserveKeys = setup._pType == ParserType.SVMLight?25000000:setup._ncols;
    VectorGroup vg = v.group();

    int vecIdStart = vg.reserveKeys(reserveKeys);
    MultiFileParseTask mfpt = job._mfpt = new MultiFileParseTask(v.group(),vecIdStart,setup,job._progress);
    mfpt.invoke(fkeys);
    EnumUpdateTask eut = null;
    // Calculate enum domain
    int n = 0;
    AppendableVec [] avs = mfpt.vecs();

    if((avs.length + vecIdStart) < reserveKeys) {
      Future f = vg.tryReturnKeys(vecIdStart + reserveKeys, vecIdStart + avs.length);
      if (f != null) try { f.get(); } catch (InterruptedException e) { } catch (ExecutionException e) {}
    }
    int [] ecols = new int[avs.length];
    for( int i = 0; i < ecols.length; ++i )
      if(avs[i].shouldBeEnum())
        ecols[n++] = i;
    ecols =  Arrays.copyOf(ecols, n);
    if( ecols.length > 0 ) {
      EnumFetchTask eft = new EnumFetchTask(H2O.SELF.index(), mfpt._eKey, ecols).invokeOnAllNodes();
      Enum[] enums = eft._gEnums;
      ValueString[][] ds = new ValueString[ecols.length][];
      int j = 0;
      for( int i : ecols ) avs[i]._domain = ValueString.toString(ds[j++] = enums[i].computeColumnDomain());
      eut = new EnumUpdateTask(ds, eft._lEnums, mfpt._chunk2Enum, mfpt._eKey, ecols);
    }
    final Frame fr = new Frame(job.dest(),setup._columnNames != null?setup._columnNames:genericColumnNames(avs.length),AppendableVec.closeAll(avs));
    // SVMLight is sparse format, there may be missing chunks with all 0s, fill them in
    new SVFTask(fr).invokeOnAllNodes();
    fr.checkCompatible();
    // Update enums to the globally agreed numbering
    if( eut != null ) {
      Vec[] evecs = new Vec[ecols.length];
      for( int i = 0; i < evecs.length; ++i ) evecs[i] = fr.vecs()[ecols[i]];
      eut.doAll(evecs);
    }
    Futures fs = new Futures();
    for(Vec v2:fr.vecs())
      v2.rollupStats(fs);
    fs.blockForPending();
    logParseResults(job, fr);
    // Release the frame for overwriting
    fr.unlock(job.self());
    // Remove CSV files from H2O memory
    if( delete_on_done ) for( Key k : fkeys ) Lockable.delete(k,job.self());
    else for( Key k : fkeys ) {
      Lockable l = UKV.get(k);
      l.unlock(job.self());
    }
    job.remove();
  }

  public static class ParseProgressMonitor extends Iced implements Job.ProgressMonitor {
    final Key _progressKey;
    private long _progress;
    public ParseProgressMonitor(Key pKey){_progressKey = pKey;}
    @Override public void update(long n) {
      ParseDataset2.onProgress(n, _progressKey);
      _progress += n;
    }
    public long progress() {
      return _progress;
    }
  }

  // --------------------------------------------------------------------------
  // We want to do a standard MRTask with a collection of file-keys (so the
  // files are parsed in parallel across the cluster), but we want to throttle
  // the parallelism on each node.
  private static class MultiFileParseTask extends MRTask<MultiFileParseTask> {
    private final CustomParser.ParserSetup _setup; // The expected column layout
    private final VectorGroup _vg;    // vector group of the target dataset
    private final int _vecIdStart;    // Start of available vector keys
    // Shared against all concurrent unrelated parses, a map to the node-local
    // Enum lists for each concurrent parse.
    private static NonBlockingHashMap<Key, Enum[]> _enums = new NonBlockingHashMap<Key, Enum[]>();
    // The Key used to sort out *this* parse's Enum[]
    private final Key _eKey = Key.make();
    private final Key _progress;
    // Mapping from Chunk# to cluster-node-number holding the enum mapping.
    // It is either self for all the non-parallel parses, or the Chunk-home for parallel parses.
    private int[] _chunk2Enum;
    // All column data for this one file
//    Vec _vecs[];M
    // A mapping of Key+ByteVec to rolling total Chunk counts.
    private IcedHashMap<Key,IcedInt> _fileChunkOffsets;

    // OUTPUT fields:
    private FVecDataOut [] _dout;


    public AppendableVec[] vecs(){
      if(_dout.length == 1)
        return _dout[0]._vecs;
      int nCols = 0;
      for(FVecDataOut dout:_dout)
        nCols = Math.max(dout._vecs.length,nCols);
      AppendableVec [] res = new AppendableVec[nCols];
      int nchunks = 0;
      for(FVecDataOut dout:_dout)
        nchunks += dout.nChunks();
      long [] espc = MemoryManager.malloc8(nchunks);
      for(int i = 0; i < res.length; ++i) {
        res[i] = new AppendableVec(_vg.vecKey(_vecIdStart + i), espc, 0);
        res[i]._chunkTypes = MemoryManager.malloc1(nchunks);
      }
      for(int i = 0; i < _dout.length; ++i)
        for(int j = 0; j < _dout[i]._vecs.length; ++j)
          res[j].setSubRange(_dout[i]._vecs[j]);
      return res;
    }
    public String _parserr;              // NULL if parse is OK, else an error string

    MultiFileParseTask(VectorGroup vg,  int vecIdStart, CustomParser.ParserSetup setup, Key progress ) {
      _vg = vg; _setup = setup; _progress = progress;
      _vecIdStart = vecIdStart;
      _runSingleThreaded = true;
    }

    @Override public void init(){
      super.init();
      _dout = new FVecDataOut[_keys.length];
    }
    @Override
    public MultiFileParseTask dfork(Key... keys){
//      init();
      _fileChunkOffsets = new IcedHashMap<Key, IcedInt>();
      int len = 0;
      for( Key k:keys) {
        _fileChunkOffsets.put(k,new IcedInt(len));
        len += getVec(k).nChunks();
      }

      // Mapping from Chunk# to cluster-node-number
      _chunk2Enum = MemoryManager.malloc4(len);
      Arrays.fill(_chunk2Enum, -1);
      return super.dfork(keys);
    }

    private FVecDataOut makeDout(ParserSetup localSetup, int chunkOff, int nchunks) {
      AppendableVec [] avs = new AppendableVec[localSetup._ncols];
      long [] espc = MemoryManager.malloc8(nchunks);
      for(int i = 0; i < avs.length; ++i)
        avs[i] = new AppendableVec(_vg.vecKey(i + _vecIdStart),espc, chunkOff);
      return localSetup._pType == ParserType.SVMLight
        ?new SVMLightFVecDataOut(_vg,chunkOff,avs,_vecIdStart,chunkOff,enums(_eKey,localSetup._ncols))
        :new FVecDataOut(_vg, chunkOff, chunkOff, enums(_eKey,localSetup._ncols), avs);
    }

    // Called once per file
    @Override public void map( Key key ) {
      // Get parser setup info for this chunk
      ByteVec vec = (ByteVec) getVec(key);
      byte [] bits = vec.chunkForChunkIdx(0)._mem;
      if(bits == null || bits.length == 0){
        assert false:"encountered empty file during multifile parse? should've been filtered already";
        return; // should not really get here
      }
      final int chunkOff = _fileChunkOffsets.get(key)._val;
      Compression cpr = Utils.guessCompressionMethod(bits);
      CustomParser.ParserSetup localSetup = GuessSetup.guessSetup(Utils.unzipBytes(bits,cpr), _setup,false)._setup;
      // Local setup: nearly the same as the global all-files setup, but maybe
      // has the header-flag changed.
      if(!localSetup.isCompatible(_setup)) {
        _parserr = "Conflicting file layouts, expecting: " + _setup + " but found " + localSetup;
        return;
      }

      // Allow dup headers, if they are equals-ignoring-case
      boolean has_hdr = _setup._header && localSetup._header;
      if( has_hdr ) {           // Both have headers?
        for( int i = 0; has_hdr && i < localSetup._columnNames.length; ++i )
          has_hdr = localSetup._columnNames[i].equalsIgnoreCase(_setup._columnNames[i]);
        if( !has_hdr )          // Headers not compatible?
          // Then treat as no-headers, i.e., parse it as a normal row
          localSetup = new CustomParser.ParserSetup(ParserType.CSV,localSetup._separator, false);
      }

      // Parse the file
      try {
        switch( cpr ) {
        case NONE:
          if( localSetup._pType.parallelParseSupported ) {
            DParse dp = new DParse(_vg, localSetup, _vecIdStart, chunkOff,this, vec.nChunks());
            addToPendingCount(1);
            dp._removeKey = vec._key;
            dp.exec(new Frame(vec));
            for( int i = 0; i < vec.nChunks(); ++i )
              _chunk2Enum[chunkOff + i] = vec.chunkKey(i).home_node().index();
          } else {
            ParseProgressMonitor pmon = new ParseProgressMonitor(_progress);
            _dout[_lo] = streamParse(vec.openStream(pmon), localSetup, makeDout(localSetup,chunkOff,vec.nChunks()), pmon);
            for(int i = 0; i < vec.nChunks(); ++i)
              _chunk2Enum[chunkOff + i] = H2O.SELF.index();
          }
          break;
        case ZIP: {
          // Zipped file; no parallel decompression;
          ParseProgressMonitor pmon = new ParseProgressMonitor(_progress);
          ZipInputStream zis = new ZipInputStream(vec.openStream(pmon));
          ZipEntry ze = zis.getNextEntry(); // Get the *FIRST* entry
          // SVMLightFVecDataOut(VectorGroup vg, int cidx, AppendableVec [] avs, int vecIdStart, int chunkOff,  Enum [] enums)

          // There is at least one entry in zip file and it is not a directory.
          if( ze != null && !ze.isDirectory() ) 
            _dout[_lo] = streamParse(zis, localSetup, makeDout(localSetup,chunkOff,vec.nChunks()), pmon);
          else zis.close();       // Confused: which zipped file to decompress
          // set this node as the one which rpocessed all the chunks
          for(int i = 0; i < vec.nChunks(); ++i)
            _chunk2Enum[chunkOff + i] = H2O.SELF.index();
          break;
        }
        case GZIP:
          // Zipped file; no parallel decompression;
          ParseProgressMonitor pmon = new ParseProgressMonitor(_progress);
          _dout[_lo] = streamParse(new GZIPInputStream(vec.openStream(pmon)),localSetup,makeDout(localSetup,chunkOff,vec.nChunks()), pmon);
          // set this node as the one which processed all the chunks
          for(int i = 0; i < vec.nChunks(); ++i)
            _chunk2Enum[chunkOff + i] = H2O.SELF.index();
          break;
        }
      } catch( IOException ioe ) {
        throw new RuntimeException(ioe);
      }
    }

    // Reduce: combine errors from across files.
    // Roll-up other meta data
    @Override public void reduce( MultiFileParseTask mfpt ) {
      assert this != mfpt;
      // Combine parse errors from across files
      if( _parserr == null ) _parserr = mfpt._parserr;
      else if( mfpt._parserr != null ) _parserr += mfpt._parserr;
      // Collect & combine columns across files
      if( _dout == null ) _dout = mfpt._dout;
      else if(_dout != mfpt._dout) _dout = Utils.append(_dout,mfpt._dout);
      if( _chunk2Enum == null ) _chunk2Enum = mfpt._chunk2Enum;
      else if(_chunk2Enum != mfpt._chunk2Enum) { // we're sharing global array!
        for( int i = 0; i < _chunk2Enum.length; ++i ) {
          if( _chunk2Enum[i] == -1 ) _chunk2Enum[i] = mfpt._chunk2Enum[i];
          else assert mfpt._chunk2Enum[i] == -1 : Arrays.toString(_chunk2Enum) + " :: " + Arrays.toString(mfpt._chunk2Enum);
        }
      }
    }

    private static Enum [] enums(Key eKey, int ncols){
      if(!_enums.containsKey(eKey)){
        Enum [] enums = new Enum[ncols];
        for(int i = 0; i < enums.length; ++i)enums[i] = new Enum();
        _enums.putIfAbsent(eKey, enums);
      }
      return _enums.get(eKey);
    }

    // ------------------------------------------------------------------------
    // Zipped file; no parallel decompression; decompress into local chunks,
    // parse local chunks; distribute chunks later.
    private FVecDataOut streamParse( final InputStream is, final CustomParser.ParserSetup localSetup, FVecDataOut dout, ParseProgressMonitor pmon) throws IOException {
      // All output into a fresh pile of NewChunks, one per column
      CustomParser p = localSetup.parser();
      // assume 2x inflation rate
      //if( localSetup._pType.parallelParseSupported )
      if( localSetup._pType.parallelParseSupported )
        try{p.streamParse(is, dout, pmon);}catch(IOException e){throw new RuntimeException(e);}
      else
        try{p.streamParse(is, dout);}catch(Exception e){throw new RuntimeException(e);}

      // Parse all internal "chunks", until we drain the zip-stream dry.  Not
      // real chunks, just flipping between 32K buffers.  Fills up the single
      // very large NewChunk.
      dout.close(_fs);
      return dout;
    }

    // ------------------------------------------------------------------------
    private static class DParse extends MRTask2<DParse> {
      private final CustomParser.ParserSetup _setup;
      private final int _vecIdStart;
      private final int _chunkOff; // for multifile parse, offset of the first chunk in the final dataset
      private final VectorGroup _vg;

      private FVecDataOut _dout;
      private final Key _eKey;
      final Key _progress;
      Key _removeKey;
      private transient final MultiFileParseTask _outerMFPT;
      final int _nchunks;
      private transient NonBlockingSetInt _visited;
      private transient long [] _espc;

      DParse(VectorGroup vg, CustomParser.ParserSetup setup, int vecIdstart, int startChunkIdx, MultiFileParseTask mfpt, int nchunks) {
        super(mfpt);
        _vg = vg;
        _setup = setup;
        _vecIdStart = vecIdstart;
        _chunkOff = startChunkIdx;
        _outerMFPT = mfpt;
        _eKey = mfpt._eKey;
        _progress = mfpt._progress;
        _nchunks = nchunks;
      }
      @Override public void setupLocal(){
        super.setupLocal();
        _espc = MemoryManager.malloc8(_nchunks);
        _visited = new NonBlockingSetInt();
      }
      @Override public void map( Chunk in) {
        AppendableVec [] avs = new AppendableVec[_setup._ncols];
        for(int i = 0; i < avs.length; ++i)
          avs[i] = new AppendableVec(_vg.vecKey(_vecIdStart + i), _espc, _chunkOff);
        Enum [] enums = enums(_eKey,_setup._ncols);
        // Break out the input & output vectors before the parse loop
        // The Parser
        FVecDataIn din = new FVecDataIn(in);
        FVecDataOut dout;
        CustomParser p;
        switch(_setup._pType) {
        case CSV:
          p = new CsvParser(_setup);
          dout = new FVecDataOut(_vg,_chunkOff, _chunkOff + in.cidx(),enums,avs);
          break;
        case SVMLight:
          p = new SVMLightParser(_setup);
          dout = new SVMLightFVecDataOut(_vg, _chunkOff + in.cidx(), avs, _vecIdStart, _chunkOff, enums);
          break;
        default:
          throw H2O.unimpl();
        }
        p.parallelParse(in.cidx(),din,dout);
        (_dout = dout).close(_fs);
        onProgress(in._len, _progress); // Record bytes parsed

        // remove parsed data right away (each chunk is used by 2)
        final int cidx = in.cidx();
        if(!_visited.add(cidx)) {
          Value v = H2O.get(in._vec.chunkKey(cidx));
          if(v != null && v.isPersisted()) {
            v.freePOJO();
            v.freeMem();
          }
        }
        if(!_visited.add(cidx+1)) {
          Value v = H2O.get(in._vec.chunkKey(cidx+1));
          if(v != null && v.isPersisted()) {
            v.freePOJO();
            v.freeMem();
          }
        }
      }
      @Override public void reduce(DParse dp) {
        if(_dout == null)_dout = dp._dout;
        else _dout.reduce(dp._dout);
      }
      @Override public void postGlobal() {
        super.postGlobal();
        _outerMFPT._dout[_outerMFPT._lo] = _dout;
        _dout = null;           // Reclaim GC eagerly
        if(_removeKey != null) UKV.remove(_removeKey);
      }
    }

    // Find & remove all partially built output chunks & vecs
    private Futures onExceptionCleanup(Futures fs) {
      int nchunks = _chunk2Enum.length;
      int ncols = _setup._ncols;
      for( int i = 0; i < ncols; ++i ) {
        Key vkey = _vg.vecKey(_vecIdStart + i);
        DKV.remove(vkey,fs);
        for( int c = 0; c < nchunks; ++c )
          DKV.remove(Vec.chunkKey(vkey,c),fs);
      }
      cancel(true);
      return fs;
    }
  }


  // ------------------------------------------------------------------------
  /** Parsed data output specialized for fluid vecs.
   * @author tomasnykodym
   */
  static class FVecDataOut extends Iced implements CustomParser.StreamDataOut {
    protected transient NewChunk [] _nvs;
    public final int _chunkOff;
    protected AppendableVec []_vecs;
    private transient final Enum [] _enums;
    protected byte [] _ctypes;
    long _nLines;
    int _col = -1;
    final int _cidx;
    int _nChunks;
    boolean _closedVecs = false;
    private final VectorGroup _vg;

    static final protected byte UCOL = 0; // unknown col type
    static final protected byte NCOL = 1; // numeric col type
    static final protected byte ECOL = 2; // enum    col type
    static final protected byte TCOL = 3; // time    col typ
    static final protected byte ICOL = 4; // UUID    col typ

    public int nChunks(){return _nChunks;}

//    private static AppendableVec[] newAppendables(int n, VectorGroup vg, int vecIdStart){
//      AppendableVec [] apps = new AppendableVec[n];
//
//      for(int i = 0; i < n; ++i)
//        apps[i] = new AppendableVec(vg.vecKey(vecIdStart + i));
//      return apps;
//    }
//    public FVecDataOut(VectorGroup vg, int cidx, int ncols, int vecIdStart, Enum[] enums){
//      this(vg,cidx,ncols,vecIdStart,enums,newAppendables(ncols,vg,vecIdStart));
//    }

    public FVecDataOut(VectorGroup vg, int chunkOff, int cidx, Enum [] enums, AppendableVec [] appendables){
      assert cidx - chunkOff >= 0:"incompatible cidx/chunkOff " + chunkOff + ", " + cidx;
      _vecs = appendables;
      _chunkOff = chunkOff;
      _enums = enums;
      _cidx = cidx;
      _vg = vg;
      _ctypes = MemoryManager.malloc1(appendables.length);
      _nvs = new NewChunk[appendables.length];
      for(int i = 0; i < appendables.length; ++i)
        _nvs[i] = (NewChunk)_vecs[i].chunkForChunkIdx(_cidx);

    }

    @Override public FVecDataOut reduce(StreamDataOut sdout){
      FVecDataOut dout = (FVecDataOut)sdout;
      _nChunks += dout._nChunks;
      if( dout!=null && _vecs != dout._vecs){
        if(dout._vecs.length > _vecs.length){
          AppendableVec [] v = _vecs;
          _vecs = dout._vecs;
          for(int i = 1; i < _vecs.length; ++i)
            _vecs[i]._espc = _vecs[0]._espc;
          dout._vecs = v;
        }
        for(int i = 0; i < dout._vecs.length; ++i)
          _vecs[i].reduce(dout._vecs[i]);
      }
      return this;
    }
    @Override public FVecDataOut close(){
      Futures fs = new Futures();
      close(fs);
      fs.blockForPending();
      return this;
    }
    public void check(){
      if(_nvs != null) for(NewChunk nv:_nvs)
        assert (nv._len == _nLines):"unexpected number of lines in NewChunk, got " + nv._len + ", but expected " + _nLines;
    }
    @Override public FVecDataOut close(final Futures fs){
      if( _nvs == null ) return this; // Might call close twice
      for(NewChunk nc:_nvs)
        assert nc._len == _nLines:"incompatible lengths after parsing chunk, " + _nLines + " != " + nc._len;
      RecursiveAction [] rs = new RecursiveAction[_nvs.length];
      for(int i = 0; i < _nvs.length; ++i) {
        final int fi = i;
        rs[i] = new RecursiveAction() {
          @Override
          protected void compute() {
            _nvs[fi].close(_cidx, fs);
            _nvs[fi] = null; // Free for GC
          }
        };
      }
      ForkJoinTask.invokeAll(rs);
      _nChunks++;
      _nvs = null;  // Free for GC
      return this;
    }
    @Override public FVecDataOut nextChunk(){
      return  new FVecDataOut(_vg, _chunkOff,_cidx+1,_enums, _vecs);
    }

//    protected Vec [] closeVecs(){
//      Futures fs = new Futures();
//      _closedVecs = true;
//      Vec [] res = new Vec[_vecs.length];
//      for(int i = 0; i < _vecs.length; ++i)
//        res[i] = _vecs[i].close(fs);
//      _vecs = null;  // Free for GC
//      fs.blockForPending();
//      return res;
//    }

    @Override public void newLine() {
      if(_col >= 0){
        ++_nLines;
        for(int i = _col+1; i < _vecs.length; ++i)
          addInvalidCol(i);
      }
      _col = -1;
    }
    @Override public void addNumCol(int colIdx, long number, int exp) {
      if( colIdx < _vecs.length ) {
        _nvs[_col = colIdx].addNum(number, exp);
        if(_ctypes[colIdx] == UCOL ) _ctypes[colIdx] = NCOL;
      }
    }

    @Override public void addInvalidCol(int colIdx) {
      if(colIdx < _vecs.length) _nvs[_col = colIdx].addNA();
    }
    @Override public final boolean isString(int colIdx) { return false; }
    @Override public final void addStrCol(int colIdx, ValueString str) {
      if(colIdx < _nvs.length){
        if(_ctypes[colIdx] == NCOL){ // support enforced types
          addInvalidCol(colIdx);
          return;
        }
        if(_ctypes[colIdx] == UCOL && ParseTime.attemptTimeParse(str) > 0)
          _ctypes[colIdx] = TCOL;
        if( _ctypes[colIdx] == UCOL ) { // Attempt UUID parse
          int old = str.get_off();
          ParseTime.attemptUUIDParse0(str);
          ParseTime.attemptUUIDParse1(str);
          if( str.get_off() != -1 ) _ctypes[colIdx] = ICOL;
          str.setOff(old);
        }

        if( _ctypes[colIdx] == TCOL ) {
          long l = ParseTime.attemptTimeParse(str);
          if( l == Long.MIN_VALUE ) addInvalidCol(colIdx);
          else {
            int time_pat = ParseTime.decodePat(l); // Get time pattern
            l = ParseTime.decodeTime(l);           // Get time
            addNumCol(colIdx, l, 0);               // Record time in msec
            _nvs[_col]._timCnt[time_pat]++; // Count histo of time parse patterns
          }
        } else if( _ctypes[colIdx] == ICOL ) { // UUID column?  Only allow UUID parses
          long lo = ParseTime.attemptUUIDParse0(str);
          long hi = ParseTime.attemptUUIDParse1(str);
          if( str.get_off() == -1 )  { lo = C16Chunk._LO_NA; hi = C16Chunk._HI_NA; }
          if( colIdx < _vecs.length ) _nvs[_col = colIdx].addUUID(lo, hi);
        } else if(!_enums[_col = colIdx].isKilled()) {
          // store enum id into exponent, so that it will be interpreted as NA if compressing as numcol.
          int id = _enums[colIdx].addKey(str);
          if(_ctypes[colIdx] == UCOL && id > 1) _ctypes[colIdx] = ECOL;
          _nvs[colIdx].addEnum(id);
        } else // turn the column into NAs by adding value overflowing Enum.MAX_SIZE
          _nvs[colIdx].addEnum(Integer.MAX_VALUE);
      } //else System.err.println("additional column (" + colIdx + ":" + str + ") on line " + linenum());
    }

    /** Adds double value to the column. */
    @Override public void addNumCol(int colIdx, double value) {
      if (Double.isNaN(value)) {
        addInvalidCol(colIdx);
      } else {
        double d= value;
        int exp = 0;
        long number = (long)d;
        while (number != d) {
          d = d * 10;
          --exp;
          number = (long)d;
        }
        addNumCol(colIdx, number, exp);
      }
    }
    @Override public void setColumnNames(String [] names){}
    @Override public final void rollbackLine() {}
    @Override public void invalidLine(String err) { newLine(); }
    @Override public void invalidValue(int line, int col) {}
  }

  // ------------------------------------------------------------------------
  /** Parser data in taking data from fluid vec chunk.
   *  @author tomasnykodym
   */
  private static class FVecDataIn implements CustomParser.DataIn {
    final Vec _vec;
    Chunk _chk;
    int _idx;
    final long _firstLine;
    public FVecDataIn(Chunk chk){
      _chk = chk;
      _idx = _chk.cidx();
      _firstLine = _chk._start;
      _vec = chk._vec;
    }
    @Override public byte[] getChunkData(int cidx) {
      if(cidx != _idx)
        _chk = cidx < _vec.nChunks()?_vec.chunkForChunkIdx(_idx = cidx):null;
      return (_chk == null)?null:_chk._mem;
    }
    @Override public int  getChunkDataStart(int cidx) { return -1; }
    @Override public void setChunkDataStart(int cidx, int offset) { }
  }
}
