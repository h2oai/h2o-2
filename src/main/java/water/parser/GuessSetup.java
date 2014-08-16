package water.parser;

import java.util.*;
import water.*;
import water.fvec.Frame;
import water.fvec.ParseDataset2;
import water.parser.CustomParser.PSetupGuess;
import water.parser.CustomParser.ParserSetup;
import water.parser.CustomParser.ParserType;
import water.util.*;
import water.util.Utils.IcedArrayList;

abstract public class GuessSetup {

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
//      ks = ParseDataset2.filterEmptyFiles(ks);
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
      gSetup = guessSetup(Utils.getFirstUnzipedBytes(keys.get(0)),setup,checkHeader);
    if( gSetup == null || !gSetup._isValid){
      throw new ParseSetupGuessException(gSetup,null);
    }
    if(headerKey != null){ // separate headerKey
      Value v = DKV.get(headerKey);
      if(!v.isRawData()){ // either ValueArray or a Frame, just extract the headers
        if(v.isFrame()){
          Frame fr = v.get();
          colNames = fr._names;
        } else
          throw new ParseSetupGuessException("Headers can only come from unparsed data, ValueArray or a frame. Got " + v.className(),gSetup,null);
      } else { // check the hdr setup by parsing first bytes
        CustomParser.ParserSetup lSetup = gSetup._setup.clone();
        lSetup._header = true;
        PSetupGuess hSetup = guessSetup(Utils.getFirstUnzipedBytes(headerKey),lSetup,false);
        if(hSetup == null || !hSetup._isValid) { // no match with global setup, try once more with general setup (e.g. header file can have different separator than the rest)
          ParserSetup stp = new ParserSetup();
          stp._header = true;
          hSetup = guessSetup(Utils.getFirstUnzipedBytes(headerKey),stp,false);
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


  public static class GuessSetupTsk extends MRTask<GuessSetupTsk> {
    final CustomParser.ParserSetup _userSetup;
    final boolean _checkHeader;
    boolean _empty = true;
    public PSetupGuess _gSetup;
    IcedArrayList<Key> _failedSetup;
    IcedArrayList<Key> _conflicts;

    public GuessSetupTsk(CustomParser.ParserSetup userSetup, boolean checkHeader) {
      _userSetup = userSetup;
      assert _userSetup != null;
      _checkHeader = checkHeader;
      assert !_userSetup._header || !checkHeader;
    }

    public static final int MAX_ERRORS = 64;

    @Override public void map(Key key) {
      byte [] bits = Utils.getFirstUnzipedBytes(key);
      if(bits.length > 0) {
        _empty = false;
        _failedSetup = new IcedArrayList<Key>();
        _conflicts = new IcedArrayList<Key>();
        _gSetup = GuessSetup.guessSetup(bits, _userSetup, _checkHeader);
        if (_gSetup == null || !_gSetup._isValid)
          _failedSetup.add(key);
        else {
          _gSetup._setupFromFile = key;
          if (_checkHeader && _gSetup._setup._header)
            _gSetup._hdrFromFile = key;
        }
      }
    }

    @Override
    public void reduce(GuessSetupTsk drt) {
      if (drt._empty) return;
      if (_gSetup == null || !_gSetup._isValid) {
        _empty = false;
        _gSetup = drt._gSetup;
        if (_gSetup == null)
          System.out.println("haha");
//        if(_gSetup != null) {
        try {
          _gSetup._hdrFromFile = drt._gSetup._hdrFromFile;
          _gSetup._setupFromFile = drt._gSetup._setupFromFile;
//        }
        } catch (Throwable t) {
          t.printStackTrace();
        }
      } else if (drt._gSetup._isValid && !_gSetup._setup.isCompatible(drt._gSetup._setup)) {
        if (_conflicts.contains(_gSetup._setupFromFile) && !drt._conflicts.contains(drt._gSetup._setupFromFile)) {
          _gSetup = drt._gSetup; // setups are not compatible, select random setup to send up (thus, the most common setup should make it to the top)
          _gSetup._setupFromFile = drt._gSetup._setupFromFile;
          _gSetup._hdrFromFile = drt._gSetup._hdrFromFile;
        } else if (!drt._conflicts.contains(drt._gSetup._setupFromFile)) {
          _conflicts.add(_gSetup._setupFromFile);
          _conflicts.add(drt._gSetup._setupFromFile);
        }
      } else if (drt._gSetup._isValid) { // merge the two setups
        if (!_gSetup._setup._header && drt._gSetup._setup._header) {
          _gSetup._setup._header = true;
          _gSetup._hdrFromFile = drt._gSetup._hdrFromFile;
          _gSetup._setup._columnNames = drt._gSetup._setup._columnNames;
        }
        if (_gSetup._data.length < CustomParser.MAX_PREVIEW_LINES) {
          int n = _gSetup._data.length;
          int m = Math.min(CustomParser.MAX_PREVIEW_LINES, n + drt._gSetup._data.length - 1);
          _gSetup._data = Arrays.copyOf(_gSetup._data, m);
          for (int i = n; i < m; ++i) {
            _gSetup._data[i] = drt._gSetup._data[i - n + 1];
          }
        }
      }
      // merge failures
      if (_failedSetup == null) {
        _failedSetup = drt._failedSetup;
        _conflicts = drt._conflicts;
      } else {
        _failedSetup.addAll(drt._failedSetup);
        _conflicts.addAll(drt._conflicts);
      }
    }
  }

    public static PSetupGuess guessSetup(byte [] bits){
    return guessSetup(bits,new ParserSetup(),true);
  }

  public static PSetupGuess guessSetup(byte [] bits, ParserSetup setup, boolean checkHeader ) {
    if(bits == null) return new PSetupGuess(new ParserSetup(), 0, 0, null,false, null);
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

}
