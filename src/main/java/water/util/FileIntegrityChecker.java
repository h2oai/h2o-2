package water.util;

import java.io.File;
import java.util.ArrayList;

import water.*;
import water.fvec.*;
import water.persist.PersistNFS;

public class FileIntegrityChecker extends DRemoteTask<FileIntegrityChecker> {
  final String   _root;         // Root of directory
  final String[] _files;        // File names found locally
  final long  [] _sizes;        // File sizes found locally
  int[][] _ok;                    // OUTPUT: files which are globally compatible


  @Override public void lcompute() {
    _ok = new int[_files.length][H2O.CLOUD.size()];
    for (int i = 0; i < _files.length; ++i) {
      File f = new File(_files[i]);
      if (f.exists() && (f.length()==_sizes[i]))
        _ok[i][H2O.SELF.index()] = 1;
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

  public static FileIntegrityChecker check(File r) {
    return new FileIntegrityChecker(r).invokeOnAllNodes();
  }

  public FileIntegrityChecker(File root) {
    _root = PersistNFS.decodeFile(new File(root.getAbsolutePath())).toString();
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

  // Sync this directory with H2O.  Record all files that appear to be visible
  // to the entire cloud, and give their Keys.  List also all files which appear
  // on this H2O instance but are not consistent around the cluster, and Keys
  // which match the directory name but are not on disk.
  public Key syncDirectory(ArrayList<String> files,
                           ArrayList<String> keys,
                           ArrayList<String> fails,
                           ArrayList<String> dels) {

    Futures fs = new Futures();
    Key k = null;
    // Find all Keys which match ...
    for( int i = 0; i < _files.length; ++i ) {
      boolean failed = false;
      for (int j = 0; j < H2O.CLOUD.size(); ++j) {
        if (_ok[i][j] == 0) {
          failed = true;
          fails.add("missing file " + _files[i] + " at node " + H2O.CLOUD._memary[j]);
        }
      }
      if(!failed){
        File f = new File(_files[i]);
        k = PersistNFS.decodeFile(f);
        if( files != null ) files.add(_files[i]);
        if( keys  != null ) keys .add(k.toString());
        if(DKV.get(k) != null)dels.add(k.toString());
        new Frame(k).delete_and_lock(null);
        NFSFileVec nfs = DKV.get(NFSFileVec.make(f, fs)).get();
        Frame fr = new Frame(k,new String[] { "0" }, new Vec[] { nfs });
        fr.update(null);
        fr.unlock(null);
      }
    }
    fs.blockForPending();
    return k;
  }
}
