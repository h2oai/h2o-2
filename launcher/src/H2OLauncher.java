import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: tomk
 * Date: 8/15/13
 * Time: 12:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class H2OLauncher implements ActionListener {
  // Stuff managed by the GUI builder BEGIN.
  private JTextField xmxField;
  private JButton hexdataButton;
  private JButton startStopButton;
  private JPanel panel;
  // Stuff managed by the GUI builder END.

  private static H2OLauncher h2oLauncher;

  private static Thread procManagerThread;
  private static Process proc;

  private final static String XMX_COMMAND = "xmx";
  private final static String START_COMMAND = "start";
  private final static String STOP_COMMAND = "stop";

  private enum H2oStatus {
    RUNNING,
    NOT_RUNNING
  }

  private static H2oStatus status;

  public H2OLauncher() {
    startStopButton.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent actionEvent) {
        //To change body of implemented methods use File | Settings | File Templates.
        if (status == H2oStatus.RUNNING) {
          stopProcess();
        }
        else {
          startProcess();
        }
        recalcInBackground();
      }
    });
    xmxField.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent actionEvent) {
        //To change body of implemented methods use File | Settings | File Templates.
        String s = xmxField.getText();
        String s2 = s.trim();
        if (s2.matches("^[1-9][0-9]*$")) {
          updateResult ("Xmx set successfully");
        }
        else {
          xmxField.setText("1");
          updateResult("Xmx must be of the form nnnm or nnng (e.g. 1024m, 2g)");
        }
        recalcInBackground();
      }
    });
    hexdataButton.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent actionEvent) {
        try {
          URI uri = new URI("http://docs.0xdata.com");
          Desktop.getDesktop().browse(uri);
        } catch (Exception e) {
        }
      }
    });
  }

  private boolean isPortTaken (int port) {
    boolean portTaken = false;

    ServerSocket socket = null;
    try {
      socket = new ServerSocket(port);
    }
    catch (IOException e) {
      portTaken = true;
    }
    finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
					/* e.printStackTrace(); */
        }
      }
    }

    return portTaken;
  }

  private class ProcManagerThread extends Thread {
    private H2OLauncher _launcher;
    private Process _p;

    ProcManagerThread (H2OLauncher launcher, Process p) {
      _launcher = launcher;
      _p = p;
    }

    public void run() {
      BufferedReader r = new BufferedReader (new InputStreamReader(_p.getInputStream()));
      while (true) {
        try {
          String s = r.readLine();
          if (s == null) {
            _p.waitFor();
            _launcher.notifyProcessExit();
            break;
          }

          notifyProcessOutput (s + "\n");

          if (s.contains("Listening for HTTP and REST traffic")) {
            String s2 = s.replaceFirst ("^.*http", "http");
            _launcher.notifyHttp (s2);
          }
        }
        catch (Exception e) {
          _p.destroy();
          _launcher.notifyProcessExit();
          break;
        }
      }
    }
  }

  private void recalcInBackground() {
    javax.swing.SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        recalc();
      }
    });
  }

  public void notifyProcessOutput(final String s) {
    javax.swing.SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        // textArea.append(s);
      }
    });
  }

  public void notifyHttp(final String s) {
    javax.swing.SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        // browserField.setText(s);
        if ((s != null) && (s.length() > 0)) {
          try {
            URI uri = new URI(s);
            Desktop.getDesktop().browse(uri);
          } catch (Exception e) {
          }
        }
      }
    });
  }

  public void notifyProcessExit() {
    synchronized (this) {
      procManagerThread = null;
      proc = null;
      status = H2oStatus.NOT_RUNNING;
    }

    notifyProcessOutput ("\nH2O PROCESS EXITED\n\n");
    notifyHttp("");
    recalcInBackground();
  }

  private void startProcess() {
    synchronized (this) {
      if (procManagerThread != null) {
        updateResult ("Error: procManagerThread already exists");
        return;
      }
    }

    try {
      String cloudName;
      {
        Random r = new Random();
        Integer randValue = new Integer(r.nextInt());
        if (randValue < 0) {
          randValue = -randValue;
        }
        cloudName = "h2o_cluster_" + (randValue.toString());
      }

      File currentDirectory = new File(new File(".").getAbsolutePath());
      String javaHome = System.getProperty("java.home");
      String javaBinary = javaHome + File.separator + "bin" + File.separator + "java";
      String h2oJar = currentDirectory.getAbsolutePath().toString() + File.separator + "h2o.jar";
      // String h2oJar = "/Users/tomk/0xdata/ws/h2o/target/h2o.jar";
      ProcessBuilder pb = new ProcessBuilder(
              javaBinary,
              "-Xmx" + xmxField.getText().trim() + "g",
              "-jar", h2oJar,
              "-name", cloudName,
              "-ip", "127.0.0.1",
              "-port", "54321"
      );
      pb.redirectErrorStream(true);

      Process p = pb.start();
      synchronized (this) {
        proc = p;
        ProcManagerThread t;
        t = new ProcManagerThread (this, proc);
        t.start();
        procManagerThread = t;
        status = H2oStatus.RUNNING;
      }

      updateResult("H2O started");
    }
    catch (Exception e) {
      updateResult ("Failed to start H2O (process start failed)");
      String s = e.getMessage() + "\n" + e.getStackTrace();
      // textArea.setText(s);
    }
  }

  private void stopProcess() {
    synchronized (this) {
      //if (procManagerThread == null) {
      //  updateResult ("Error: procManagerThread does not");
      //}

      if (proc == null) {
        updateResult ("Error: proc does not exist, cleaning up as best as possible");
        procManagerThread = null;
        status = H2oStatus.NOT_RUNNING;
        return;
      }

      proc.destroy();

      updateResult("H2O stopped");
      notifyHttp("");
    }
  }

  // Called when this JVM is exiting.
  public void destroyProcess() {
    synchronized (this) {
      if (proc == null) {
        return;
      }

      proc.destroy();
    }
  }

  private void updateResult (String s) {
    // resultField.setText(s);
  }

  public void actionPerformed(ActionEvent evt) {
    String actionCommand = evt.getActionCommand();

    if (actionCommand.equals (XMX_COMMAND)) {
      String s = xmxField.getText();
      String s2 = s.trim();
      if (s2.matches("^[1-9][0-9]*$")) {
        updateResult ("Xmx set successfully");
      }
      else {
        updateResult ("Xmx must be of the form nnnm or nnng (e.g. 1024m, 2g)");
      }
    }
    else if (actionCommand.equals (START_COMMAND)) {
      startProcess();
    }
    else if (actionCommand.equals (STOP_COMMAND)) {
      stopProcess();
    }
    else {
      System.err.println ("Unknown actionCommand");
    }

    recalcInBackground();
  }

  private void recalc() {
    if (status == H2oStatus.RUNNING) {
      // h2oStatusLabel.setText(H2O_STATUS_RUNNING);
      startStopButton.setText("Stop H2O");
    }
    else if (status == H2oStatus.NOT_RUNNING) {
      // h2oStatusLabel.setText(H2O_STATUS_NOT_RUNNING);
      startStopButton.setText("Start H2O");
    }

    startStopButton.requestFocusInWindow();
  }

  public static void main(String[] args) {
    JFrame frame = new JFrame("H2O");
    h2oLauncher = new H2OLauncher();
    frame.setContentPane(h2oLauncher.panel);
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    frame.pack();
    frame.setVisible(true);

    status = H2oStatus.NOT_RUNNING;
    h2oLauncher.recalcInBackground();

    final Thread mainThread = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          h2oLauncher.destroyProcess();
          mainThread.join();
        }
        catch (Exception e) {
        }
      }
    });
  }
}
