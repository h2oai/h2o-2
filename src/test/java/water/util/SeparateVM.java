package water.util;

import H2OInit.Boot;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

import water.Log;

/**
 * Executes code in a separate VM.
 */
public class SeparateVM implements Separate {
  private final Process _process;

  public SeparateVM(String prefix, String[] args) throws IOException {
    ArrayList<String> list = new ArrayList<String>();
    list.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
    list.add("-cp");
    list.add(System.getProperty("java.class.path"));
    // list.add("-agentlib:jdwp=transport=dt_socket,address=8000,server=y,suspend=y");
    list.add(getClass().getName());
    if( args != null )
      list.addAll(Arrays.asList(args));
    ProcessBuilder builder = new ProcessBuilder(list);
    builder.directory(new File(System.getProperty("user.dir")));
    builder.redirectErrorStream(true);
    Process process = builder.start();
    inheritIO(process, null, false);
    _process = process;
  }

  public static void main(String[] args) throws Exception {
    exitWithParent();
    Boot.main(args);
  }

  @Override
  public void close() {
    _process.destroy();
  }

  public int exitValue() {
    return _process.exitValue();
  }

  @Override
  public void waitForEnd() {
    try {
      _process.waitFor();
    } catch( InterruptedException e ) {
      Log.write(e);
    }
  }

  static void exitWithParent() {
    Thread thread = new Thread() {
      @Override
      public void run() {
        for( ;; ) {
          int b;
          try {
            b = System.in.read();
          } catch( Exception e ) {
            b = -1;
          }
          if( b < 0 ) {
            Log.write("Assuming parent done, exit(0)");
            System.exit(0);
          }
        }
      }
    };
    thread.setDaemon(true);
    thread.start();
  }

  static void inheritIO(Process process, final String description, final boolean addLogHeader) {
    final BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
    Thread thread = new Thread() {
      @Override
      public void run() {
        if( description != null ) {
          if( addLogHeader )
            Log.write(description);
          else
            System.out.println(description);
        }

        try {
          for( ;; ) {
            String line = input.readLine();

            if( line == null )
              break;

            if( addLogHeader )
              Log.write(line);
            else
              System.out.println(line);
          }
        } catch( IOException e ) {
          // Ignore, process probably done
        }
      }
    };
    thread.start();
  }
}