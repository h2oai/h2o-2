package water.parser;

import java.io.EOFException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.*;

import org.apache.hadoop.thirdparty.guava.common.base.Objects;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.api.Inspect;
import water.parser.CustomParser.ParserSetup;
import water.parser.CustomParser.ParserType;
import water.parser.DParseTask.Pass;
import water.util.Log;
import water.util.RIStream;

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

  public static ParserSetup guessSetup(Value v){
    return guessSetup(v,ParserType.AUTO,CsvParser.NO_SEPARATOR);
  }
  public static ParserSetup guessSetup(byte [] bits){
    return guessSetup(bits,ParserType.AUTO,CsvParser.NO_SEPARATOR);
  }

  public static ParserSetup guessSetup(Value v, ParserType pType){
    return guessSetup(v, pType, CsvParser.NO_SEPARATOR);
  }
  public static ParserSetup guessSetup(Value v, ParserType pType, byte sep){
    return guessSetup(Inspect.getFirstBytes(v),pType,sep);
  }
  public static ParserSetup guessSetup(byte [] bits, ParserType pType, byte sep){
    ParserSetup res = null;
    switch(pType){
      case CSV:
        return CsvParser.guessSetup(bits,sep);
      case SVMLight:
        return SVMLightParser.guessSetup(bits);
      case XLS:
        return XlsParser.guessSetup(bits);
      case AUTO:
        if((res = XlsParser.guessSetup(bits)) != null)
          return res;
        if((res = SVMLightParser.guessSetup(bits)) != null)
          return res;
        return CsvParser.guessSetup(bits,sep);
      default:
        throw H2O.unimpl();
    }
  }

  public static void parse(Key okey, Key [] keys){
    forkParseDataset(okey, keys, null).get();
  }

  static DParseTask tryParseXls(Value v,ParseDataset job){
    DParseTask t =  new DParseTask().createPassOne(v, job, new XlsParser(null));
    try{t.passOne();} catch(Exception e) {return null;}
    return t;
  }

  public static void parse(ParseDataset job, Key [] keys, CustomParser.ParserSetup setup){
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
    Value v = DKV.get(keys[0]);
    DParseTask p1 = tryParseXls(v,job);
    if(p1 != null) {
      if(keys.length == 1){ // shortcut for 1 xls file, we already have pass one done, just do the 2nd pass and we're done
        DParseTask p2 = p1.createPassTwo();
        p2.passTwo();
        p2.createValueArrayHeader();
        job.remove();
        return;
      } else
        throw H2O.unimpl();
    }
    if(setup == null || setup._pType == CustomParser.ParserType.AUTO)
      setup = ParseDataset.guessSetup(v);
    Compression compression = guessCompressionMethod(v);
    try {
      UnzipAndParseTask tsk = new UnzipAndParseTask(job, compression, setup);
      tsk.invoke(keys);
      DParseTask [] p2s = new DParseTask[keys.length];
      DParseTask phaseTwo = tsk._tsk.createPassTwo();
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
        p2s[i] = phaseTwo.makePhase2Clone(finfo).dfork(k);
      }
      phaseTwo._sigma = new double[phaseTwo._ncolumns];
      phaseTwo._invalidValues = new long[phaseTwo._ncolumns];
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
    } catch (Throwable e) {
      UKV.put(job.dest(), new Fail(e.getMessage()));
      throw Throwables.propagate(e);
    } finally {
      job.remove();
    }
  }

  public static class ParserFJTask extends H2OCountedCompleter {
    final ParseDataset job;
    Key [] keys;
    CustomParser.ParserSetup setup;

    public ParserFJTask(ParseDataset job, Key [] keys, CustomParser.ParserSetup setup){
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
  public static Job forkParseDataset(final Key dest, final Key[] keys, final CustomParser.ParserSetup setup) {
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
    final CustomParser.ParserSetup _parserSetup;

    public UnzipAndParseTask(ParseDataset job, Compression comp, CustomParser.ParserSetup parserSetup) {
      this(job,comp,parserSetup, Integer.MAX_VALUE);
    }
    public UnzipAndParseTask(ParseDataset job, Compression comp, CustomParser.ParserSetup parserSetup, int maxParallelism) {
      _job = job;
      _comp = comp;
      _parserSetup = parserSetup;
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
        Value v = DKV.get(key);
        assert v != null;
        ParserSetup localSetup = ParseDataset.guessSetup(v, _parserSetup._pType, _parserSetup._separator);
        localSetup._header &= _parserSetup._header;
        if(!_parserSetup.isCompatible(localSetup))throw new ParseException("Parsing incompatible files. " + _parserSetup.toString() + " is not compatible with " + localSetup.toString());
        _fileInfo[_idx] = new FileInfo();
        _fileInfo[_idx]._ikey = key;
        _fileInfo[_idx]._okey = key;
        if(localSetup._header)
          for(int i = 0; i < _parserSetup._ncols; ++i)
            localSetup._header &= _parserSetup._columnNames[i].equalsIgnoreCase(localSetup._columnNames[i]);
        _fileInfo[_idx]._header = localSetup._header;
        CustomParser parser = null;
        DParseTask dpt = null;
        switch(localSetup._pType){
          case CSV:
            parser = new CsvParser(localSetup, false);
            dpt = new DParseTask();
            break;
          case SVMLight:
            parser = new SVMLightParser(localSetup);
            dpt = new SVMLightDParseTask();
            break;
          default:
            throw H2O.unimpl();
        }
        long csz = v.length();
        if(_comp != Compression.NONE){
          onProgressSizeChange(csz,_job); // additional pass through the data to decompress
          InputStream is = null;
          InputStream ris = null;
          try {
            ris = v.openStream(new UnzipProgressMonitor(_job._progress));
            switch(_comp){
            case ZIP:
              ZipInputStream zis = new ZipInputStream(ris);
              ZipEntry ze = zis.getNextEntry();
              // There is at least one entry in zip file and it is not a directory.
              if (ze == null || ze.isDirectory())
                throw new Exception("Unsupported zip file: " + ((ze == null) ? "No entry found": "Files containing directory are not supported."));
              is = zis;
              break;
            case GZIP:
              is = new GZIPInputStream(ris);
              break;
            default:
              Log.info("Can't understand compression: _comp: "+_comp+" csz: "+csz+" key: "+key+" ris: "+ris);
              throw H2O.unimpl();
            }
            _fileInfo[_idx]._okey = Key.make(new String(key._kb) + "_UNZIPPED");
            ValueArray.readPut(_fileInfo[_idx]._okey, is,_job);
            v = DKV.get(_fileInfo[_idx]._okey);
            onProgressSizeChange(2*(v.length() - csz), _job); // the 2 passes will go over larger file!
            assert v != null;
          }catch (EOFException e){
            if(ris != null && ris instanceof RIStream){
              RIStream r = (RIStream)ris;
              System.err.println("Unexpected eof after reading " + r.off() + "bytes, expeted size = " + r.expectedSz());
            }
            System.err.println("failed decompressing data " + key.toString() + " with compression " + _comp);
            throw new RuntimeException(e);
          } catch (Throwable t) {
            System.err.println("failed decompressing data " + key.toString() + " with compression " + _comp);
            throw new RuntimeException(t);
          } finally {
           Closeables.closeQuietly(is);
          }
        }
        _p1 = dpt.createPassOne(v, _job, parser);
        _p1.setCompleter(this);
        _p1.passOne();
//        if(_parser instanceof CsvParser){
//          CustomParser p2 = null; // gues parser hereInspect.csvGuessValue(v);
//          if(setup._data[0].length != _ncolumns)
//            throw new ParseException("Found conflicting number of columns (using separator " + (int)_sep + ") when parsing multiple files. Found " + setup._data[0].length + " columns  in " + key + " , but expected " + _ncolumns);
//          _fileInfo[_idx]._header = setup._header;
//          if(_fileInfo[_idx]._header && _headers != null) // check if we have the header, it should be the same one as we got from the head
//            for(int i = 0; i < setup._data[0].length; ++i)
//              _fileInfo[_idx]._header = _fileInfo[_idx]._header && setup._data[0][i].equalsIgnoreCase(_headers[i]);
//          setup = new CsvParser.Setup(_sep, _fileInfo[_idx]._header, setup._data, setup._numlines, setup._bits);
//          _p1 = DParseTask.createPassOne(v, _job, _pType);
//          _p1.setCompleter(this);
//          _p1.passOne(setup);
          // DO NOT call tryComplete here, _p1 calls it!
//        } else {
//         _p1 = tryParseXls(v,_job);
//         if(_p1 == null)
//           throw new ParseException("Found conflicting types of files. Can not parse xls and not-xls files together");
//         tryComplete();
//        }
      }

      @Override
      public void onCompletion(CountedCompleter caller){
        try{
          _fileInfo[_idx]._nrows = _p1._nrows;
          long numRows = 0;
          for(int i = 0; i < _p1._nrows.length; ++i){
            numRows += _p1._nrows[i];
            _fileInfo[_idx]._nrows[i] = numRows;
          }
        }catch(Throwable t){t.printStackTrace();}
        quietlyComplete(); // wake up anyone  who is joining on this task!
      }
    }

    @Override
    public void lcompute() {
      try{
        _fileInfo = new FileInfo[_keys.length];
        subTasks = new UnzipAndParseLocalTask[_keys.length];
        setPendingCount(subTasks.length);
        int p = 0;
        int j = 0;
        for(int i = 0; i < _keys.length; ++i){
          if(p == ParseDataset.PLIMIT) subTasks[j++].join(); else ++p;
          H2O.submitTask((subTasks[i] = new UnzipAndParseLocalTask(i)));
        }
      }catch(Throwable t){t.printStackTrace();}
      tryComplete();
    }

    transient UnzipAndParseLocalTask [] subTasks;
    @Override
    public final void lonCompletion(CountedCompleter caller){
      try{
        _tsk = subTasks[0]._p1;
        for(int i = 1; i < _keys.length; ++i){
          DParseTask tsk = subTasks[i]._p1;
          tsk._nrows = _tsk._nrows;
          _tsk.reduce(tsk);
        }
      }catch(Throwable t){t.printStackTrace();}
    }

    @Override
    public void reduce(DRemoteTask drt) {
      try{
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
      }catch(Throwable t){t.printStackTrace();}
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
