package water.parser;

import java.io.InputStream;
import java.util.*;
import java.util.zip.*;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.api.Inspect;
import water.parser.DParseTask.Pass;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;

/**
 * Helper class to parse an entire ValueArray data, and produce a structured ValueArray result.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
@SuppressWarnings("fallthrough")
public final class ParseDataset extends Job {

  public static int PLIMIT = Integer.MAX_VALUE;
  public static enum Compression { NONE, ZIP, GZIP }

  public final Key  _progress;

  private ParseDataset(Key dest, Key[] keys) {
    super("Parse", dest);
    Value dataset = DKV.get(keys[0]);
    long total = dataset.length() * Pass.values().length;
    for(int i = 1; i < keys.length; ++i){
      dataset = DKV.get(keys[i]);
      total += dataset.length() * Pass.values().length;
    }
    _progress = Key.make(UUID.randomUUID().toString(), (byte) 0, Key.JOB);
    UKV.put(_progress, new Progress(0,total));
  }

  // Guess
  public static Compression guessCompressionMethod(Value dataset) {
    byte[] b = dataset.getFirstBytes(); // First chunk
    AutoBuffer ab = new AutoBuffer(b);

    // Look for ZIP magic
    if( b.length > ZipFile.LOCHDR && ab.get4(0) == ZipFile.LOCSIG )
      return Compression.ZIP;
    if( b.length > 2 && ab.get2(0) == GZIPInputStream.GZIP_MAGIC )
      return Compression.GZIP;
    return Compression.NONE;
  }

  public static void parse(Key okey, Key [] keys){
    forkParseDataset(okey, keys, null).get();
  }

  static DParseTask tryParseXls(Value v,ParseDataset job){
    DParseTask t = DParseTask.createPassOne(v, job, CustomParser.Type.XLS);
    try{t.passOne(null);}catch(Exception e){return null;}
    return t;
  }

  public static void parse(ParseDataset job, Key [] keys, CsvParser.Setup setup){
    int j = 0;
    UKV.remove(job.dest());// remove any previous instance and insert a sentinel (to ensure no one has been writing to the same keys during our parse!
    Key [] nonEmptyKeys = new Key[keys.length];
    for (int i = 0; i < keys.length; ++i) {
      Value v = DKV.get(keys[i]);
      if (v == null || v.length() > 0) // skip nonzeros
        nonEmptyKeys[j++] = keys[i];
    }
    if (j < nonEmptyKeys.length) // remove the nulls
      keys = Arrays.copyOf(nonEmptyKeys, j);
    if (keys.length == 0) {
      job.cancel();
      return;
    }
    CustomParser.Type ptype = CustomParser.Type.CSV;
    Value v = DKV.get(keys[0]);
    DParseTask p1 = tryParseXls(v,job);
    if(p1 != null) {
      if(keys.length == 1){ // shortcut for 1 xls file, we already have pass one done, just do the 2nd pass and we're done
        DParseTask p2 = DParseTask.createPassTwo(p1);
        p2.passTwo();
        p2.normalizeSigma();
        p2.createValueArrayHeader();
        job.remove();
        return;
      }
      ptype = CustomParser.Type.XLS;
    } else if (setup == null || setup._data == null || setup._data[0] == null) {
      setup = Inspect.csvGuessValue(v, (setup != null)?setup._separator:CsvParser.NO_SEPARATOR);
      assert setup._data[0] != null;
    }
    final int ncolumns = setup._data[0].length;
    Compression compression = guessCompressionMethod(v);
    try {
      UnzipAndParseTask tsk = new UnzipAndParseTask(job, compression, setup,ptype);
      tsk.invoke(keys);
      DParseTask [] p2s = new DParseTask[keys.length];
      DParseTask phaseTwo = DParseTask.createPassTwo(tsk._tsk);
      // too keep original order of the keys...
      HashMap<Key, FileInfo> fileInfo = new HashMap<Key, FileInfo>();
      long rowCount = 0;
      for(int i = 0; i < tsk._fileInfo.length; ++i)
        fileInfo.put(tsk._fileInfo[i]._ikey,tsk._fileInfo[i]);
      // run pass 2
      for(int i = 0; i < keys.length; ++i){
        FileInfo finfo = fileInfo.get(keys[i]);
        Key k = finfo._okey;
        long nrows = finfo._nrows[finfo._nrows.length-1];
        for(j = 0; j < finfo._nrows.length; ++j)
          finfo._nrows[j] += rowCount;
        rowCount += nrows;
        p2s[i] = (DParseTask) new DParseTask(phaseTwo, finfo).dfork(k);
      }
      phaseTwo._sigma = new double[ncolumns];
      phaseTwo._invalidValues = new long[ncolumns];
      // now put the results together and create ValueArray header
      for(int i = 0; i < p2s.length; ++i){
        DParseTask t = p2s[i];
        p2s[i].get();
        for (j = 0; j < phaseTwo._ncolumns; ++j) {
          phaseTwo._sigma[j] += t._sigma[j];
          phaseTwo._invalidValues[j] += t._invalidValues[j];
        }
        if ((t._error != null) && !t._error.isEmpty()) {
          System.err.println(phaseTwo._error);
          throw new Exception("The dataset format is not recognized/supported");
        }
        FileInfo finfo = fileInfo.get(keys[i]);
        UKV.remove(finfo._okey);
      }
      phaseTwo.normalizeSigma();
      phaseTwo._colNames = setup._data[0];
      phaseTwo.createValueArrayHeader();
    } catch (Exception e) {
      UKV.put(job.dest(), new Fail(e.getMessage()));
      throw Throwables.propagate(e);
    } finally {
      job.remove();
    }
  }

  public static class ParserFJTask extends H2OCountedCompleter {
    final ParseDataset job;
    Key [] keys;
    CsvParser.Setup setup;

    public ParserFJTask(ParseDataset job, Key [] keys, CsvParser.Setup setup){
      this.job = job;
      this.keys = keys;
      this.setup = setup;
    }
    @Override
    public void compute2() {
      parse(job, keys,setup);
      tryComplete();
    }
  }

  public static Job forkParseDataset(final Key dest, final Key[] keys, final CsvParser.Setup setup) {
    ParseDataset job = new ParseDataset(dest, keys);
    H2OCountedCompleter fjt = new ParserFJTask(job, keys, setup);
    job.start(fjt);
    H2O.submitTask(fjt);
    return job;
  }

  public static class ParseException extends RuntimeException {
    public ParseException(String msg) {
      super(msg);
    }
  }

  public static class FileInfo extends Iced{
    Key _ikey;
    Key _okey;
    long [] _nrows;
    boolean _header;
  }

  public static class UnzipAndParseTask extends DRemoteTask {
    final ParseDataset _job;
    final Compression _comp;
    DParseTask _tsk;
    FileInfo [] _fileInfo;
    final byte _sep;
    final int _ncolumns;
    final CustomParser.Type _pType;
    final String [] _headers;

    public UnzipAndParseTask(ParseDataset job, Compression comp, CsvParser.Setup setup, CustomParser.Type pType) {
      this(job,comp,setup,pType,Integer.MAX_VALUE);
    }
    public UnzipAndParseTask(ParseDataset job, Compression comp, CsvParser.Setup setup, CustomParser.Type pType, int maxParallelism) {
      _job = job;
      _comp = comp;
      _sep = setup._separator;
      _ncolumns = setup._data[0].length;
      _pType = pType;
      _headers = (setup._header)?setup._data[0]:null;
    }
    @Override
    public DRemoteTask dfork( Key... keys ) {
      _keys = keys;
      H2O.submitTask(this);
      return this;
    }
    static private class UnzipProgressMonitor implements ProgressMonitor {
      int _counter = 0;
      Key _progress;

      public UnzipProgressMonitor(Key progress){_progress = progress;}
      @Override
      public void update(long n) {
        n += _counter;
        if(n > (1 << 20)){
          onProgress(n, _progress);
          _counter = 0;
        } else
          _counter = (int)n;
      }
    }
    // actual implementation of unzip and parse, intedned for the FJ computation
    private class UnzipAndParseLocalTask extends H2OCountedCompleter {
      final int _idx;
      public UnzipAndParseLocalTask(int idx){
        _idx = idx;
        setCompleter(UnzipAndParseTask.this);
      }
      protected  DParseTask _p1;
      @Override
      public void compute2() {
        final Key key = _keys[_idx];
        _fileInfo[_idx] = new FileInfo();
        _fileInfo[_idx]._ikey = key;
        _fileInfo[_idx]._okey = key;
        Value v = DKV.get(key);
        assert v != null;
        long csz = v.length();
        if(_comp != Compression.NONE){
          onProgressSizeChange(csz,_job); // additional pass through the data to decompress
          InputStream is = null;
          try {
            switch(_comp){
            case ZIP:
              ZipInputStream zis = new ZipInputStream(v.openStream(new UnzipProgressMonitor(_job._progress)));
              ZipEntry ze = zis.getNextEntry();
              // There is at least one entry in zip file and it is not a directory.
              if (ze == null || ze.isDirectory())
                throw new Exception("Unsupported zip file: " + ((ze == null) ? "No entry found": "Files containing directory are not supported."));
              is = zis;
              break;
            case GZIP:
              is = new GZIPInputStream(v.openStream(new UnzipProgressMonitor(_job._progress)));
              break;
            default:
              throw H2O.unimpl();
            }
            _fileInfo[_idx]._okey = Key.make(new String(key._kb) + "_UNZIPPED");
            ValueArray.readPut(_fileInfo[_idx]._okey, is,_job);
            v = DKV.get(_fileInfo[_idx]._okey);
            onProgressSizeChange(2*(v.length() - csz), _job); // the 2 passes will go over larger file!
            assert v != null;
          } catch (Throwable t) {
            System.err.println("failed decompressing data " + key.toString() + " with compression " + _comp);
            throw new RuntimeException(t);
          } finally {
           Closeables.closeQuietly(is);
          }
        }
        CsvParser.Setup setup = null;
        if(_pType == CustomParser.Type.CSV){
          setup = Inspect.csvGuessValue(v,_sep);
          if(setup._data[0].length != _ncolumns)
            throw new ParseException("Found conflicting number of columns (using separator " + (int)_sep + ") when parsing multiple files. Found " + setup._data[0].length + " columns  in " + key + " , but expected " + _ncolumns);
          _fileInfo[_idx]._header = setup._header;
          if(_fileInfo[_idx]._header && _headers != null) // check if we have the header, it should be the same one as we got from the head
            for(int i = 0; i < setup._data[0].length; ++i)
              _fileInfo[_idx]._header = _fileInfo[_idx]._header && setup._data[0][i].equalsIgnoreCase(_headers[i]);
          setup = new CsvParser.Setup(_sep, _fileInfo[_idx]._header, setup._data, setup._numlines, setup._bits);
          _p1 = DParseTask.createPassOne(v, _job, _pType);
          _p1.setCompleter(this);
          _p1.passOne(setup);
          // DO NOT call tryComplete here, _p1 calls it!
        } else {
         _p1 = tryParseXls(v,_job);
         if(_p1 == null)
           throw new ParseException("Found conflicting types of files. Can not parse xls and not-xls files together");
         tryComplete();
        }
      }

      @Override
      public void onCompletion(CountedCompleter caller){
        _fileInfo[_idx]._nrows = _p1._nrows;
        long numRows = 0;
        for(int i = 0; i < _p1._nrows.length; ++i){
          numRows += _p1._nrows[i];
          _fileInfo[_idx]._nrows[i] = numRows;
        }
        quietlyComplete(); // wake up anyone  who is joining on this task!
      }
    }

    @Override
    public void lcompute() {
      _fileInfo = new FileInfo[_keys.length];
      subTasks = new UnzipAndParseLocalTask[_keys.length];
      setPendingCount(subTasks.length);
      int p = 0;
      int j = 0;
      for(int i = 0; i < _keys.length; ++i){
        if(p == ParseDataset.PLIMIT) subTasks[j++].join(); else ++p;
        H2O.submitTask((subTasks[i] = new UnzipAndParseLocalTask(i)));
      }
      tryComplete();
    }

    transient UnzipAndParseLocalTask [] subTasks;
    @Override
    public final void lonCompletion(CountedCompleter caller){
      _tsk = subTasks[0]._p1;
      for(int i = 1; i < _keys.length; ++i){
        DParseTask tsk = subTasks[i]._p1;
        tsk._nrows = _tsk._nrows;
        _tsk.reduce(tsk);
      }
    }

    @Override
    public void reduce(DRemoteTask drt) {
      UnzipAndParseTask tsk = (UnzipAndParseTask)drt;
      if(_tsk == null && _fileInfo == null){
        _fileInfo = tsk._fileInfo;
        _tsk = tsk._tsk;
      } else {
        final int n = _fileInfo.length;
        _fileInfo = Arrays.copyOf(_fileInfo, n + tsk._fileInfo.length);
        System.arraycopy(tsk._fileInfo, 0, _fileInfo, n, tsk._fileInfo.length);
        // we do not want to merge nrows from different files, apart from that, we want to use standard reduce!
        tsk._tsk._nrows = _tsk._nrows;
        _tsk.reduce(tsk._tsk);
      }
    }
  }

  // True if the array is all NaNs
  static boolean allNaNs(double ds[]) {
    for( double d : ds )
      if( !Double.isNaN(d) )
        return false;
    return true;
  }

  // Progress (TODO count chunks in VA, unify with models?)

  static class Progress extends Iced {
    long _total;
    long _value;
    Progress(long val, long total){_value = val; _total = total;}
  }

  @Override
  public float progress() {
    Progress progress = UKV.get(_progress);
    if(progress == null || progress._total == 0) return 0;
    return progress._value / (float) progress._total;
  }
  @Override public void remove() {
    DKV.remove(_progress);
    super.remove();
  }
  static final void onProgress(final Key chunk, final Key progress) {
    assert progress != null;
    Value val = DKV.get(chunk);
    if (val == null)
      return;
    final long len = val.length();
    onProgress(len, progress);
  }
  static final void onProgress(final long len, final Key progress) {
    new TAtomic<Progress>() {
      @Override
      public Progress atomic(Progress old) {
        if (old == null)
          return null;
        old._value += len;
        return old;
      }
    }.fork(progress);
  }
  static final void onProgressSizeChange(final long len, final ParseDataset job) {
    new TAtomic<Progress>() {
      @Override
      public Progress atomic(Progress old) {
        if (old == null)
          return null;
        old._total += len;
        return old;
      }
    }.fork(job._progress);
  }
}
