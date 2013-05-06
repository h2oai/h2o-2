package water.util;

import java.io.IOException;
import java.io.InputStream;
import water.Job.ProgressMonitor;
import water.Key;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.base.Throwables;

public abstract class RIStream extends InputStream {
  InputStream _is;
  ProgressMonitor _pmon;
  public final int _retries = 3;
  String [] _bk;
  private long _off;

  protected RIStream( long off, ProgressMonitor pmon){
    _off = off;
  }

  public final void open(){
    assert _is == null;
    _is = open(_off);
  }

  protected abstract InputStream open(long offset);

  private void try2Recover(int attempt, IOException e) {
    System.out.println("[H2OS3InputStream] Attempt("+attempt + ") to recover from " + e.getMessage() + "), off = " + _off);
    e.printStackTrace();
    if(attempt == _retries) Throwables.propagate(e);
    try{_is.close();}catch(IOException ex){}
    _is = null;
    if(attempt > 0) try {Thread.sleep(256 << attempt);}catch(InterruptedException ex){}
    open(_off);
    return;
  }
  @Override
  public boolean markSupported(){
    return false;
  }
  @Override
  public void mark(int readLimit){throw new UnsupportedOperationException();}
  @Override
  public void reset(){throw new UnsupportedOperationException();}

  @Override
  public final int available() throws IOException {
    int attempts = 0;
    while(true){
      try {
        return _is.available();
      } catch (IOException e) {
        try2Recover(attempts++,e);
      }
    }
  }

  @Override
  public int read() throws IOException {
    int attempts = 0;
    while(true){
      try{
        int res = _is.read();
        if(res != -1){
          _off += 1;
          if(_pmon != null)_pmon.update(1);
        }
        return res;
      }catch (IOException e){
        try2Recover(attempts++,e);
      }
    }
  }

  @Override
  public int read(byte [] b) throws IOException {
    int attempts = 0;
    while(true){
      try {
        int res =  _is.read(b);
        if(res > 0){
          _off += res;
          if(_pmon != null)_pmon.update(res);
        }
        return res;
      } catch(IOException e) {
        try2Recover(attempts++,e);
      }
    }
  }

  @Override
  public int read(byte [] b, int off, int len) throws IOException {
    int attempts = 0;
    while(true){
      try {
        int res = _is.read(b,off,len);;
        if(res > 0){
          _off += res;
          if(_pmon != null)_pmon.update(res);
        }
        return res;
      } catch(IOException e) {
        try2Recover(attempts++,e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if(_is != null){
      _is.close();
      _is = null;
    }
  }

  @Override
  public long skip(long n) throws IOException {
    int attempts = 0;
    while(true){
      try{
        long res = _is.skip(n);
        if(res > 0){
          _off += res;
          if(_pmon != null)_pmon.update(res);
        }
        return res;
      } catch (IOException e) {
        try2Recover(attempts++,e);
      }
    }
  }
}
