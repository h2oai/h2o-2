package water.parser;

import water.*;
import water.api.Inspect;
import water.util.Log;

public class CheckParseSetup extends MRTask {

  boolean _res = true;
  final byte _separator;
  final int _ncols;


  public CheckParseSetup(ParseDataset job, CsvParser.Setup s){
    _job = job;
    _separator = s._separator; _ncols = s._data[0].length;
  }

  final ParseDataset _job;

  @Override
  public void map(Key key) {
    if(!_job.cancelled()){
      try{
        CsvParser.Setup setup = Inspect.csvGuessValue(DKV.get(key));
        _res = (setup._separator == _separator) && (setup._data[0].length == _ncols);
        if(!_res){
          _job.cancel();
          UKV.put(_job.dest(), new Job.Fail("Attempting to parse incompatible files!"));
        }
      } catch(Throwable t){
        Log.err(t);
        _res = false;
      }
    }
  }

  @Override
  public void reduce(DRemoteTask drt) {
    _res &= ((CheckParseSetup)drt)._res;
  }

}
