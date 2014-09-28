package water.util;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import water.DRemoteTask;
import water.H2O;

public class LogCollectorTask extends DRemoteTask {
  final int MB = 1 << 20;
  final int MAX_SIZE = 25 * MB;
  public byte[][] _result;

  public LogCollectorTask() {}

  private transient ByteArrayOutputStream baos = null;

  @Override public void lcompute() {
    _result = new byte[H2O.CLOUD._memary.length][];
    int idx = H2O.SELF.index();
    baos = new ByteArrayOutputStream();
    ZipOutputStream zos = new ZipOutputStream(baos);

    try {
      zipDir(Log.LOG_DIR, zos);
    }
    catch (IOException e) {
      H2O.ignore(e);
    }
    finally {
      try {
        zos.close();
        baos.close();
      } catch (Exception xe) {
        // do nothing
      }

      byte[] arr = baos.toByteArray();
      _result[idx] = arr;

      tryComplete();
    }
  }

  //here is the code for the method
  private void zipDir(String dir2zip, ZipOutputStream zos) throws IOException
  {
    try
    {
      //create a new File object based on the directory we have to zip.
      File zipDir = new File(dir2zip);
      //get a listing of the directory content
      String[] dirList = zipDir.list();
      byte[] readBuffer = new byte[4096];
      int bytesIn = 0;
      //loop through dirList, and zip the files
      for(int i=0; i<dirList.length; i++)
      {
        File f = new File(zipDir, dirList[i]);
        if(f.isDirectory())
        {
          //if the File object is a directory, call this
          //function again to add its content recursively
          String filePath = f.getPath();
          zipDir(filePath, zos);
          //loop again
          continue;
        }
        //if we reached here, the File object f was not a directory
        //create a FileInputStream on top of f
        FileInputStream fis = new FileInputStream(f);
        // create a new zip entry
        ZipEntry anEntry = new ZipEntry(f.getPath());
        anEntry.setTime(f.lastModified());
        //place the zip entry in the ZipOutputStream object
        zos.putNextEntry(anEntry);
        //now write the content of the file to the ZipOutputStream

        boolean stopEarlyBecauseTooMuchData = false;
        while((bytesIn = fis.read(readBuffer)) != -1)
        {
          zos.write(readBuffer, 0, bytesIn);
          if (baos.size() > MAX_SIZE) {
            stopEarlyBecauseTooMuchData = true;
            break;
          }
        }
        //close the Stream
        fis.close();
        zos.closeEntry();

        if (stopEarlyBecauseTooMuchData) {
          Log.warn("LogCollectorTask stopEarlyBecauseTooMuchData");
          break;
        }
      }
    }
    catch(Exception e)
    {
      //handle exception
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
