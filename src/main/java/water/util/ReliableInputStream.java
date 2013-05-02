package water.util;

import java.io.*;
import java.util.Arrays;

import water.Job.ProgressMonitor;
import water.Key;

import com.google.common.base.Throwables;

public abstract class ReliableInputStream extends InputStream {
  Key _k;
  long _off;
  long _mark;
  InputStream _is;
  ProgressMonitor _pmon;
  public final int _retries = 3;
  String [] _bk;

  protected ReliableInputStream(InputStream is) throws IOException{_is = is;}
  protected abstract InputStream open(long off) throws IOException;
  private void try2Recover(int attempt, IOException e) {
    System.out.println("[H2OS3InputStream] Attempt("+attempt + ") to recover from " + e.getMessage() + ")");
    e.printStackTrace();
    while(attempt < _retries) {
      try{close();}catch(IOException ex){}
      if(attempt > 0) try {Thread.sleep(256 << attempt);}catch(InterruptedException ex){}
      try {
        _is = open(_off);
        return;
      } catch(IOException ex){++attempt;}
    }
    Throwables.propagate(e);
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
        if(res != -1)_off += 1;
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
        if(res > 0)_off += res;
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
        if(res > 0)_off += res;
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
      _off = 0;
    }
  }

  @Override
  public long skip(long n) throws IOException {
    int attempts = 0;
    while(true){
      try{
        long res = _is.skip(n);
        if(res > 0)_off += res;
        return res;
      } catch (IOException e) {
        try2Recover(attempts++,e);
      }
    }
  }
  public final long bytesRead(){return _off;}
  static class MyInputStream extends ReliableInputStream {
    final byte [] _bs;
    public MyInputStream(byte [] bs) throws IOException {super(new ByteArrayInputStream(bs));_bs = bs;}
    @Override
    protected InputStream open(long off) throws IOException {
      InputStream is = new ByteArrayInputStream(_bs);
      is.skip(off);
      return is;
    }
  }

  public static void main(String [] args) throws Exception {
    MyInputStream is = new MyInputStream(new byte[]{'a','b','c','d','e','f','g','h'});
    System.out.println(is.bytesRead());
    System.out.println((char)is.read() + ", " + is.bytesRead());
    byte [] bf = new byte[4];
    System.out.println(is.read(bf) + ", " + is.bytesRead() + ", " + Arrays.toString(bf));
    System.out.println(is.read(bf,2,2) + ", " + is.bytesRead() + ", " + Arrays.toString(bf));
    System.out.println(is.available());
    is.skip(is.available());
    System.out.println(is.available());
    System.out.println(is.bytesRead());
  }
}

