package water.util;

import java.io.*;

import water.*;

public class LogCollectorTask extends DRemoteTask {

  public static final int MAX_SIZE = 1 << 16;

  public byte[][] _result;

  public LogCollectorTask() {}

  @Override public void lcompute() {
    _result = new byte[H2O.CLOUD._memary.length][];

    int  idx       = H2O.SELF.index();
    File logFile   = PersistIce.logFile;
    InputStream is = null;
    int length     = (int) Math.min(MAX_SIZE, logFile.length());
    try {
      _result[idx] = new byte[length];
      is = new FileInputStream(logFile);
      is.skip(logFile.length() - length);
      int off = 0;
      while (length > 0) {
        int l = is.read(_result[idx], off, length);
        off    += l;
        length -= l;
      }
    } catch (IOException e) {
      H2O.ignore(e);
    } finally {
      try { is.close(); } catch (Exception _) { }
      tryComplete();
    }
  }

  @Override public void reduce(DRemoteTask drt) {
    LogCollectorTask another = (LogCollectorTask) drt;
    if( _result == null ) _result = another._result;
    else for (int i=0; i<_result.length; ++i)
      if (_result[i] == null)
        _result[i] = another._result[i];
  }

  @Override public byte priority() { return H2O.GUI_PRIORITY; }
}
