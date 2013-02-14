package water.util;

import java.io.File;
import java.util.ArrayList;

import water.*;

public class FileIntegrityChecker extends DRemoteTask {
  String[] _files;
  long[] _sizes;
  int[] _ok;

  @Override public void compute() {
    _ok = new int[_files.length];
    for (int i = 0; i < _files.length; ++i) {
      File f = new File(_files[i]);
      if (f.exists() && (f.length()==_sizes[i]))
        _ok[i] = 1;
    }
    tryComplete();
  }

  @Override public void reduce(DRemoteTask drt) {
    FileIntegrityChecker o = (FileIntegrityChecker) drt;
    if( _ok == null ) _ok = o._ok;
    else for ( int i = 0; i < _ok.length; ++i ) _ok[i] += o._ok[i];
  }

  private void addFolder(File folder, ArrayList<File> filesInProgress ) {
    if (folder.isDirectory()) {
      for (File f: folder.listFiles()) {
        if (f.isDirectory())
          addFolder(f,filesInProgress);
        else
          filesInProgress.add(f);
      }
    } else {
      filesInProgress.add(folder);
    }
  }

  public static FileIntegrityChecker check(File r) {
    FileIntegrityChecker checker = new FileIntegrityChecker(r);
    checker.invokeOnAllNodes();
    return checker;
  }

  public FileIntegrityChecker(File root) {
    ArrayList<File> filesInProgress = new ArrayList();
    addFolder(root,filesInProgress);
    _files = new String[filesInProgress.size()];
    _sizes = new long[filesInProgress.size()];
    for (int i = 0; i < _files.length; ++i) {
      File f = filesInProgress.get(i);
      _files[i] = f.getAbsolutePath();
      _sizes[i] = f.length();
    }
  }

  public int size() { return _files.length; }
  public String getFileName(int i) { return _files[i]; }

  public Key importFile(int i, Futures fs) {
    if( _ok[i] < H2O.CLOUD.size() ) return null;
    File f = new File(_files[i]);
    Key k = PersistNFS.decodeFile(f);
    long size = f.length();
    Value val = (size < 2*ValueArray.CHUNK_SZ)
      ? new Value(k,(int)size,Value.NFS)
      : new ValueArray(k,size,Value.NFS).value();
    val.setdsk();
    if(fs == null) UKV.put(k, val);
    else UKV.put(k, val, fs);
    return k;
  }
}