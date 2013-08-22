package water.util;

import java.io.File;
import java.util.ArrayList;

import water.*;
import water.fvec.*;
import water.persist.PersistNFS;

public class FileIntegrityChecker extends DRemoteTask<FileIntegrityChecker> {
  final String[] _files;        // File names found locally
  final long  [] _sizes;        // File sizes found locally
  final boolean  _newApi;       // Produce NFSFileVec instead of ValueArray
  int[] _ok;                    // OUTPUT: files which are globally compatible


  @Override public void lcompute() {
    _ok = new int[_files.length];
    for (int i = 0; i < _files.length; ++i) {
      File f = new File(_files[i]);
      if (f.exists() && (f.length()==_sizes[i]))
        _ok[i] = 1;
    }
    tryComplete();
  }

  @Override public void reduce(FileIntegrityChecker o) {
    if( _ok == null ) _ok = o._ok;
    else Utils.add(_ok,o._ok);
  }

  @Override public byte priority() { return H2O.GUI_PRIORITY; }

  private void addFolder(File folder, ArrayList<File> filesInProgress ) {
    if( !folder.canRead() ) return;
    if (folder.isDirectory()) {
      for (File f: folder.listFiles()) {
        if( !f.canRead() ) continue; // Ignore unreadable files
        if( f.isHidden() && !folder.isHidden() )
          continue;             // Do not dive into hidden dirs unless asked
        if (f.isDirectory())
          addFolder(f,filesInProgress);
        else
          filesInProgress.add(f);
      }
    } else {
      filesInProgress.add(folder);
    }
  }

  public static FileIntegrityChecker check(File r, boolean newApi) {
    return new FileIntegrityChecker(r,newApi).invokeOnAllNodes();
  }

  public FileIntegrityChecker(File root, boolean newApi) {
    _newApi = newApi;
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
    Key k;
    if(_newApi) {
      k = PersistNFS.decodeFile(f);
      NFSFileVec nfs = DKV.get(NFSFileVec.make(f, fs)).get();
      UKV.put(k, new Frame(new String[] { "0" }, new Vec[] { nfs }), fs);
    } else {
      k = PersistNFS.decodeFile(f);
      long size = f.length();
      Value val = (size < 2*ValueArray.CHUNK_SZ)
        ? new Value(k,(int)size,Value.NFS)
        : new Value(k,new ValueArray(k,size),Value.NFS);
      val.setdsk();
      UKV.put(k, val, fs);
    }
    return k;
  }
}
