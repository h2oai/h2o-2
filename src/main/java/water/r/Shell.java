package water.r;

import r.Console;
import r.ifc.Interop;
import water.r.commands.Shutdown;

public class Shell extends Thread {
  public static void go() {
    Shell shell = new Shell();
    shell.start();
  }

  public void run() {
    Interop.register(new Shutdown());
    Console.main(new String[]{});
  }
}
