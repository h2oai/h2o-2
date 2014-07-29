package water.parser;

import java.io.EOFException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.*;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.Frame;
import water.parser.CustomParser.PSetupGuess;
import water.parser.CustomParser.ParserSetup;
import water.parser.CustomParser.ParserType;
import water.parser.DParseTask.Pass;
import water.util.*;
import water.util.Utils.IcedArrayList;

import com.google.common.io.Closeables;

/**
 * Helper class to parse an entire ValueArray data, and produce a structured ValueArray result.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
@SuppressWarnings("fallthrough")
public final class ParseDataset extends Job {
  public static enum Compression { NONE, ZIP, GZIP }

  public final Key  _progress;

  private ParseDataset(Key dest, Key[] keys) {
    destination_key = dest;
    Value dataset = DKV.get(keys[0]);
    long total = dataset.length() * Pass.values().length;
    for(int i = 1; i < keys.length; ++i) {
      dataset = DKV.get(keys[i]);
      total += dataset.length() * Pass.values().length;
    }
    _progress = Key.make((byte) 0, Key.JOB);
    UKV.put(_progress, new Progress(0,total));
  }

  public static PSetupGuess guessSetup(byte [] bits){
    return guessSetup(bits,new ParserSetup(),true);
  }


  public static class GuessSetupTsk extends MRTask<GuessSetupTsk> {
    final CustomParser.ParserSetup _userSetup;
    final boolean _checkHeader;
    boolean _empty = true;
    public PSetupGuess _gSetup;
    IcedArrayList<Key> _failedSetup;
    IcedArrayList<Key> _conflicts;

    public GuessSetupTsk(CustomParser.ParserSetup userSetup, boolean checkHeader){
      _userSetup = userSetup;
      assert _userSetup != null;
      _checkHeader = checkHeader;
      assert !_userSetup._header || !checkHeader;
    }
    public static final int MAX_ERRORS = 64;
    @Override public void map(Key key) {
      byte [] bits = Utils.getFirstUnzipedBytes(key);
      if(bits != null && bits.length > 0) {
        _empty = false;
        _failedSetup = new IcedArrayList<Key>();
        _conflicts = new IcedArrayList<Key>();
        _gSetup = ParseDataset.guessSetup(bits, _userSetup, _checkHeader);
        if (_gSetup == null || !_gSetup._isValid)
          _failedSetup.add(key);
        else {
          _gSetup._setupFromFile = key;
          if (_checkHeader && _gSetup._setup._header)
            _gSetup._hdrFromFile = key;
        }
      }
    }

    @Override public void reduce(GuessSetupTsk drt) {
      if(drt._empty)return;
      if(_gSetup == null || !_gSetup._isValid){
        _empty = false;
        _gSetup = drt._gSetup;
        _gSetup._hdrFromFile = drt._gSetup._hdrFromFile;
        _gSetup._setupFromFile = drt._gSetup._setupFromFile;
      } else if(drt._gSetup._isValid && !_gSetup._setup.isCompatible(drt._gSetup._setup) ){
        if(_conflicts.contains(_gSetup._setupFromFile) && !drt._conflicts.contains(drt._gSetup._setupFromFile)){
          _gSetup = drt._gSetup; // setups are not compatible, select random setup to send up (thus, the most common setup should make it to the top)
          _gSetup._setupFromFile = drt._gSetup._setupFromFile;
          _gSetup._hdrFromFile = drt._gSetup._hdrFromFile;
        } else if(!drt._conflicts.contains(drt._gSetup._setupFromFile)) {
          _conflicts.add(_gSetup._setupFromFile);
          _conflicts.add(drt._gSetup._setupFromFile);
        }
      } else if(drt._gSetup._isValid){ // merge the two setups
        if(!_gSetup._setup._header && drt._gSetup._setup._header){
          _gSetup._setup._header = true;
          _gSetup._hdrFromFile = drt._gSetup._hdrFromFile;
          _gSetup._setup._columnNames = drt._gSetup._setup._columnNames;
        }
        if(_gSetup._data.length < CustomParser.MAX_PREVIEW_LINES){
          int n = _gSetup._data.length;
          int m = Math.min(CustomParser.MAX_PREVIEW_LINES, n + drt._gSetup._data.length-1);
          _gSetup._data = Arrays.copyOf(_gSetup._data, m);
          for(int i = n; i < m; ++i){
            _gSetup._data[i] = drt._gSetup._data[i-n+1];
          }
        }
      }
      // merge failures
      if(_failedSetup == null){
        _failedSetup = drt._failedSetup;
        _conflicts = drt._conflicts;
      } else {
        _failedSetup.addAll(drt._failedSetup);
        _conflicts.addAll(drt._conflicts);
      }
    }
  }

  public static class ParseSetupGuessException extends RuntimeException {
    public final PSetupGuess _gSetup;
    public final Key [] _failed;

    public ParseSetupGuessException(String msg,PSetupGuess gSetup, Key [] failed){
      super(msg + (gSetup != null?", found setup: " + gSetup.toString():""));
      _gSetup = gSetup;
      _failed = failed;
    }
    public ParseSetupGuessException(PSetupGuess gSetup, Key [] failed){
      super(gSetup != null?gSetup.toString():"Failed to guess parser setup.");
      _gSetup = gSetup;
      _failed = failed;
    }
  }

  public static CustomParser.PSetupGuess guessSetup(ArrayList<Key> keys,Key headerKey, CustomParser.ParserSetup setup, boolean checkHeader)  {
    String [] colNames = null;
    CustomParser.PSetupGuess gSetup = null;
    boolean headerKeyPartOfParse = false;
    if(headerKey != null ){
      if(keys.contains(headerKey)){
        headerKeyPartOfParse = true;
        keys.remove(headerKey); // process the header key separately
      }
    }
    if(keys.size() > 1){
      GuessSetupTsk t = new GuessSetupTsk(setup,checkHeader);
      Key [] ks = new Key[keys.size()];
      keys.toArray(ks);
      t.invoke(ks);
      gSetup = t._gSetup;

      if(gSetup._isValid && (!t._failedSetup.isEmpty() || !t._conflicts.isEmpty())){
        // run guess setup once more, this time knowing the global setup to get rid of conflicts (turns them into failures) and bogus failures (i.e. single line files with unexpected separator)
        GuessSetupTsk t2 = new GuessSetupTsk(gSetup._setup, !gSetup._setup._header);
        HashSet<Key> keySet = new HashSet<Key>(t._conflicts);
        keySet.addAll(t._failedSetup);
        Key [] keys2 = new Key[keySet.size()];
        t2.invoke(keySet.toArray(keys2));
        t._failedSetup = t2._failedSetup;
        t._conflicts = t2._conflicts;
        if(!gSetup._setup._header && t2._gSetup._setup._header){
          gSetup._setup._header = true;
          gSetup._setup._columnNames = t2._gSetup._setup._columnNames;
          t._gSetup._hdrFromFile = t2._gSetup._hdrFromFile;
        }
      }
      assert t._conflicts.isEmpty(); // we should not have any conflicts here, either we failed to find any valid global setup, or conflicts should've been converted into failures in the second pass
      if(!t._failedSetup.isEmpty()){
        Key [] fks = new Key[t._failedSetup.size()];
        throw new ParseSetupGuessException("Can not parse: Got incompatible files.", gSetup, t._failedSetup.toArray(fks));
      }
    } else if(!keys.isEmpty())
      gSetup = ParseDataset.guessSetup(Utils.getFirstUnzipedBytes(keys.get(0)),setup,checkHeader);
    if( gSetup == null || !gSetup._isValid){
      throw new ParseSetupGuessException(gSetup,null);
    }
    if(headerKey != null){ // separate headerKey
      Value v = DKV.get(headerKey);
      if(!v.isRawData()){ // either ValueArray or a Frame, just extract the headers
        if(v.isArray()){
          ValueArray ary = v.get();
          colNames = ary.colNames();
        } else if(v.isFrame()){
          Frame fr = v.get();
          colNames = fr._names;
        } else
          throw new ParseSetupGuessException("Headers can only come from unparsed data, ValueArray or a frame. Got " + v.className(),gSetup,null);
      } else { // check the hdr setup by parsing first bytes
        CustomParser.ParserSetup lSetup = gSetup._setup.clone();
        lSetup._header = true;
        PSetupGuess hSetup = ParseDataset.guessSetup(Utils.getFirstUnzipedBytes(headerKey),lSetup,false);
        if(hSetup == null || !hSetup._isValid) { // no match with global setup, try once more with general setup (e.g. header file can have different separator than the rest)
          ParserSetup stp = new ParserSetup();
          stp._header = true;
          hSetup = ParseDataset.guessSetup(Utils.getFirstUnzipedBytes(headerKey),stp,false);
        }
        if(!hSetup._isValid || hSetup._setup._columnNames == null)
          throw new ParseSetupGuessException("Invalid header file. I did not find any column names.",gSetup,null);
        if(hSetup._setup._ncols != gSetup._setup._ncols)
          throw new ParseSetupGuessException("Header file has different number of columns than the rest!, expected " + gSetup._setup._ncols + " columns, got " + hSetup._setup._ncols + ", header: " + Arrays.toString(hSetup._setup._columnNames),gSetup,null);
        if(hSetup._data != null && hSetup._data.length > 1){// the hdr file had both hdr and data, it better be part of the parse and represent the global parser setup
          if(!headerKeyPartOfParse) throw new ParseSetupGuessException(headerKey + " can not be used as a header file. Please either parse it separately first or include the file in the parse. Raw (unparsed) files can only be used as headers if they are included in the parse or they contain ONLY the header and NO DATA.",gSetup,null);
          else if(gSetup._setup.isCompatible(hSetup._setup)){
            gSetup = hSetup;
            keys.add(headerKey); // put the key back so the file is parsed!
          }else
            throw new ParseSetupGuessException("Header file is not compatible with the other files.",gSetup, null);
        } else if(hSetup != null && hSetup._setup._columnNames != null)
          colNames = hSetup._setup._columnNames;
        else
          throw new ParseSetupGuessException("Invalid header file. I did not find any column names.",gSetup,null);
      }
    }
    // now set the header info in the final setup
    if(colNames != null){
      gSetup._setup._header = true;
      gSetup._setup._columnNames = colNames;
      gSetup._hdrFromFile = headerKey;
    }
    return gSetup;
  }



  public static PSetupGuess guessSetup(byte [] bits, ParserSetup setup, boolean checkHeader){
    if(bits == null)return new PSetupGuess(new ParserSetup(), 0, 0, null,false, null);
    ArrayList<PSetupGuess> guesses = new ArrayList<CustomParser.PSetupGuess>();
    PSetupGuess res = null;
    if(setup == null)setup = new ParserSetup();
    switch(setup._pType){
      case CSV:
        return CsvParser.guessSetup(bits,setup,checkHeader);
      case SVMLight:
        return SVMLightParser.guessSetup(bits);
      case XLS:
        return XlsParser.guessSetup(bits);
      case AUTO:
        try{
          if((res = XlsParser.guessSetup(bits)) != null && res._isValid)
            if(!res.hasErrors())return res;
            else guesses.add(res);
        }catch(Exception e){}
        try{
          if((res = SVMLightParser.guessSetup(bits)) != null && res._isValid)
            if(!res.hasErrors())return res;
            else guesses.add(res);
        }catch(Exception e){}
        try{
          if((res = CsvParser.guessSetup(bits,setup,checkHeader)) != null && res._isValid)
            if(!res.hasErrors())return res;
            else guesses.add(res);
        }catch(Exception e){e.printStackTrace();}
        if(res == null || !res._isValid && !guesses.isEmpty()){
          for(PSetupGuess pg:guesses)
            if(res == null || pg._validLines > res._validLines)
              res = pg;
        }
        assert res != null;
        return res;
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

  public static void parse(ParseDataset job, Key [] keys, CustomParser.ParserSetup setup) {
    if(setup == null){
      ArrayList<Key> ks = new ArrayList<Key>(keys.length);
      for (Key k:keys)ks.add(k);
      PSetupGuess guess = guessSetup(ks, null, new ParserSetup(), true);
      if(!guess._isValid)throw new RuntimeException("can not parse this dataset, did not find working setup");
      setup = guess._setup;
    }
    setup.checkDupColumnNames();
    int j = 0;
    // remove any previous instance and insert a sentinel (to ensure no one has
    // been writing to the same keys during our parse!
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
    if(setup == null || setup._pType == ParserType.XLS){
      DParseTask p1 = tryParseXls(DKV.get(keys[0]),job);
      if(p1 != null) {
        if(keys.length == 1){ // shortcut for 1 xls file, we already have pass one done, just do the 2nd pass and we're done
          DParseTask p2 = p1.createPassTwo();
          p2.passTwo();
          p2.createValueArrayHeader();
          Lockable.delete(keys[0],job.self());
          job.remove();
          return;
        } else
          throw H2O.unimpl();
      }
    }
    UnzipAndParseTask tsk = new UnzipAndParseTask(job, setup);
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
      try{
        p2s[i].get();
      }catch(Exception e){throw new RuntimeException(e);}
      Utils.add(phaseTwo._sigma,t._sigma);
      Utils.add(phaseTwo._invalidValues,t._invalidValues);
      if ((t._error != null) && !t._error.isEmpty()) {
        System.err.println(phaseTwo._error);
        throw new RuntimeException("The dataset format is not recognized/supported");
      }
      // Delete source files after pass 2
      FileInfo finfo = fileInfo.get(keys[i]);
      Lockable.delete(finfo._okey,job.self());
    }
    phaseTwo.normalizeSigma();
    phaseTwo._colNames = setup._columnNames;
    if(setup._header)
      phaseTwo.setColumnNames(setup._columnNames);
    phaseTwo.createValueArrayHeader();
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

    @Override public void onCompletion(CountedCompleter cmp){job.remove();}

    @Override
    public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
      UKV.remove(job._progress);
      if(!(ex instanceof JobCancelledException))
        job.cancel("Got Exception " + ex.getClass().getSimpleName() + ", with msg " + ex.getMessage());
      return super.onExceptionalCompletion(ex, caller);
    }
  }
  public static Job forkParseDataset(final Key dest, final Key[] keys, final CustomParser.ParserSetup setup) {
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
    if( sum > memsz*2 )
      throw new IllegalArgumentException("Total input file size of "+PrettyPrint.bytes(sum)+" is much larger than total cluster memory of "+PrettyPrint.bytes(memsz)+", please use either a larger cluster or smaller data.");

    ParseDataset job = new ParseDataset(dest, keys);
    new ValueArray(job.dest(),0).delete_and_lock(job.self()); // Lock BEFORE returning
    for( Key k : keys ) Lockable.read_lock(k,job.self());
    ParserFJTask fjt = new ParserFJTask(job, keys, setup);    // Fire off background parse
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
    DParseTask _tsk;
    FileInfo [] _fileInfo;
    CustomParser.ParserSetup _parserSetup;

    public UnzipAndParseTask(ParseDataset job, CustomParser.ParserSetup parserSetup) {
      this(job,parserSetup, Integer.MAX_VALUE);
    }
    public UnzipAndParseTask(ParseDataset job, CustomParser.ParserSetup parserSetup, int maxParallelism) {
      _job = job;
      _parserSetup = parserSetup;
    }
    @Override
    public DRemoteTask dfork( Key... keys ) {
      _keys = keys;
      if(_parserSetup == null)
        _parserSetup = ParseDataset.guessSetup(Utils.getFirstUnzipedBytes(keys[0]))._setup;
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
    // actual implementation of unzip and parse, intended for the FJ computation
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
        assert v != null : "Did not find "+key;
        ParserSetup localSetup = ParseDataset.guessSetup(Utils.getFirstUnzipedBytes(v), _parserSetup,false)._setup;
        if(!_parserSetup.isCompatible(localSetup))throw new ParseException("Parsing incompatible files. " + _parserSetup.toString() + " is not compatible with " + localSetup.toString());
        _fileInfo[_idx] = new FileInfo();
        _fileInfo[_idx]._ikey = key;
        _fileInfo[_idx]._okey = key;
        if(localSetup._header &= _parserSetup._header) {
          assert localSetup._columnNames != null:"parsing " + key;
          assert _parserSetup._columnNames != null:"parsing " + key;
          for(int i = 0; i < _parserSetup._ncols; ++i)
            localSetup._header &= _parserSetup._columnNames[i].equalsIgnoreCase(localSetup._columnNames[i]);
        }
        _fileInfo[_idx]._header = localSetup._header;
        CustomParser parser = null;
        DParseTask dpt = null;
        switch(localSetup._pType){
          case CSV:
            parser = new CsvParser(localSetup);
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
        Compression comp = Utils.guessCompressionMethod(DKV.get(key).getFirstBytes());
        if(comp != Compression.NONE){
          onProgressSizeChange(csz,_job); // additional pass through the data to decompress
          InputStream is = null;
          InputStream ris = null;
          try {
            ris = v.openStream(new UnzipProgressMonitor(_job._progress));
            switch(comp){
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
              Log.info("Can't understand compression: _comp: "+ comp+" csz: "+csz+" key: "+key+" ris: "+ris);
              throw H2O.unimpl();
            }
            _fileInfo[_idx]._okey = Key.make(new String(key._kb) + "_UNZIPPED");
            ValueArray.readPut(_fileInfo[_idx]._okey, is,_job);
            Lockable.read_lock(_fileInfo[_idx]._okey,_job.self());
            Lockable.delete(_fileInfo[_idx]._ikey,_job.self()); // Delete zip after unzipping
            v = DKV.get(_fileInfo[_idx]._okey);
            onProgressSizeChange(2*(v.length() - csz), _job); // the 2 passes will go over larger file!
            assert v != null;
          }catch (EOFException e){
            if(ris != null && ris instanceof RIStream){
              RIStream r = (RIStream)ris;
              System.err.println("Unexpected eof after reading " + r.off() + "bytes, expected size = " + r.expectedSz());
            }
            System.err.println("failed decompressing data " + key.toString() + " with compression " + comp);
            throw new RuntimeException(e);
          } catch (Throwable t) {
            System.err.println("failed decompressing data " + key.toString() + " with compression " + comp);
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
          if(p == H2O.OPT_ARGS.pparse_limit) subTasks[j++].join(); else ++p;
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
