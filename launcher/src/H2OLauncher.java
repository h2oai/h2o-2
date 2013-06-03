import java.util.Random;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.io.IOException;

public class H2OLauncher extends JPanel implements ActionListener {
	private static H2OLauncher h2oLauncher;
	
	protected JTextField cloudField;
	protected JLabel h2oStatusLabel;
	protected JTextField portField;
	protected JTextField xmxField;
	protected JButton startButton;
	protected JButton stopButton;
	protected JTextField browserField;
	protected JTextField resultField;
	protected JTextArea textArea;

	private Thread procManagerThread;
	private Process proc;
	
	private final static String CLOUD_COMMAND = "cloud";
	private final static String PORT_COMMAND = "port";
	private final static String XMX_COMMAND = "xmx";
	private final static String START_COMMAND = "start";
	private final static String STOP_COMMAND = "stop";

	private enum H2oStatus {
		RUNNING,
		NOT_RUNNING
	}

	private final static String H2O_STATUS_RUNNING = "H2O status:  Running";
	private final static String H2O_STATUS_NOT_RUNNING = "H2O status:  Not running";

	private H2oStatus status;

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
			BufferedReader r = new BufferedReader (new InputStreamReader (_p.getInputStream()));
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
				textArea.append(s);
			}
		});
	}
	
	public void notifyHttp(final String s) {
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				browserField.setText(s);
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
			ProcessBuilder pb =
                            new ProcessBuilder("/Users/tomk/0xdata/H2OInstallerJRE/macosx/Contents/Resources/jre1.7.0_21-osx/java-osx/bin/java",
						"-Xmx" + xmxField.getText().trim(),
						"-jar", "/Users/tomk/0xdata/ws/h2o/target/h2o.jar",
						"-name", cloudField.getText().trim(),
						"-port", portField.getText().trim()
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
			textArea.setText(s);
		}
	}
	
	private void stopProcess() {
		synchronized (this) {
			if (procManagerThread == null) {
				updateResult ("Error: procManagerThread does not");
			}
			
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
	
	private GridBagConstraints makeConstraints() {
		GridBagConstraints c = new GridBagConstraints();    	
		c.insets = new Insets(0, 10, 10, 10);
		return c;
	}

	private GridBagConstraints makeLabelConstraints() {
		GridBagConstraints c = makeConstraints();
		c.anchor = GridBagConstraints.EAST;
		c.gridx = 0;
		return c;
	}    

	private GridBagConstraints makeFieldConstraints() {
		GridBagConstraints c = makeConstraints();
		c.anchor = GridBagConstraints.WEST;
		c.gridx = 1;
		return c;
	}    

	private GridBagConstraints makeButtonConstraints() {
		GridBagConstraints c = new GridBagConstraints();
		c.anchor = GridBagConstraints.CENTER;
		c.gridx = 2;
		return c;
	}

	private void myAdd(Component a, Object b) {
		add (a, b);
	}

	private void addPadding() {
		{
			JLabel padding = new JLabel (" ");
			GridBagConstraints c = makeLabelConstraints();
			c.gridwidth = GridBagConstraints.REMAINDER;
			c.fill = GridBagConstraints.HORIZONTAL;
			myAdd(padding, c);
		}
	}

	public H2OLauncher() {
		super(new GridBagLayout());

		status = H2oStatus.NOT_RUNNING;

		Color colorNotEditable = new Color (230, 230, 230);

		addPadding();

		JLabel cloudLabel;
		cloudLabel = new JLabel ("Cloud name");
		cloudField = new JTextField (20);
		cloudField.setToolTipText("H2O nodes must have the same cloud name in order to form a cloud.  By default, a random name is chosen to prevent you from accidentally joining an existing cloud.");
		cloudField.addActionListener(this);
		cloudField.setActionCommand(CLOUD_COMMAND);
		{
			Random r = new Random();
			Integer randValue = new Integer(r.nextInt());
			if (randValue < 0) {
				randValue = -randValue;
			}
			String s = "cloud_" + (randValue.toString());
			cloudField.setText (s);

			myAdd(cloudLabel, makeLabelConstraints());
			myAdd(cloudField, makeFieldConstraints());
		}

		JLabel portLabel;
		portLabel = new JLabel ("Browser port");
		portField = new JTextField (6);
		portField.setToolTipText("The embedded web server inside the H2O node will listen on this port.");
		portField.setText ("54321");
		portField.addActionListener(this);
		portField.setActionCommand(PORT_COMMAND);
		{
			myAdd(portLabel, makeLabelConstraints());
			myAdd(portField, makeFieldConstraints());
		}

		JLabel xmxLabel = new JLabel ("H2O Java heap size (Xmx)");
		xmxField = new JTextField (6);
		xmxField.setToolTipText("For best performance, this value should be at least roughly four times the size of your data set.");
		xmxField.setText("2g");
		xmxField.addActionListener(this);
		xmxField.setActionCommand(XMX_COMMAND);
		{
			myAdd(xmxLabel, makeLabelConstraints());
			myAdd(xmxField, makeFieldConstraints());
		}

		// Buttons and status.
		{
			startButton = new JButton ("Start H2O");
			startButton.setToolTipText("Start a new Java process running H2O.");
			startButton.addActionListener(this);
			startButton.setActionCommand (START_COMMAND);
			int y = 1;
			{
				GridBagConstraints c = makeButtonConstraints();
				c.gridy = y++;
				myAdd(startButton, c);        	
			}
			
			stopButton = new JButton ("Stop H2O");
			stopButton.setToolTipText("Stop the currently running H2O Java process.");
			stopButton.addActionListener(this);
			stopButton.setActionCommand (STOP_COMMAND);
			{
				GridBagConstraints c = makeButtonConstraints();
				c.gridy = y++;
				myAdd(stopButton, c);        	
			}

			h2oStatusLabel = new JLabel ("Blah blah");
			{
				GridBagConstraints c = makeButtonConstraints();
				c.gridy = y++;
				myAdd(h2oStatusLabel, c);
			}
		}

		addPadding();

		final int textAreaWidthInCharacters = 60;
		final int wideFieldDiffInCharacters = 0;

		JLabel browserLabel = new JLabel("H2O browser URL");
		browserField = new JTextField(textAreaWidthInCharacters-wideFieldDiffInCharacters);
		browserField.setToolTipText("After starting H2O, point your browser to this URL to interact with it.");
		browserField.setEditable(false);
		browserField.setBackground(colorNotEditable);
		{
			myAdd(browserLabel, makeLabelConstraints());

			GridBagConstraints c = makeFieldConstraints();
			c.gridwidth = GridBagConstraints.REMAINDER;
			c.fill = GridBagConstraints.BOTH;
			myAdd(browserField,c);
		}

		JLabel resultLabel = new JLabel("Last operation result");
		resultField = new JTextField(textAreaWidthInCharacters-wideFieldDiffInCharacters);
		resultField.setToolTipText("Information about whether the last thing you did succeeded or not");
		resultField.setEditable(false);
		resultField.setBackground(colorNotEditable);
		{
			myAdd(resultLabel, makeLabelConstraints());

			GridBagConstraints c = makeFieldConstraints();
			c.gridwidth = GridBagConstraints.REMAINDER;
			c.fill = GridBagConstraints.BOTH;
			myAdd(resultField, c);
		}

		addPadding();

		{
			JLabel outputLabel = new JLabel("(H2O stdout and stderr output)");
			myAdd(outputLabel, makeLabelConstraints());
		}

		textArea = new JTextArea(30, textAreaWidthInCharacters);
		textArea.setEditable(false);
		JScrollPane scrollPane = new JScrollPane(textArea);
		scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		{
			GridBagConstraints c = makeLabelConstraints();
			c.gridwidth = GridBagConstraints.REMAINDER;
			c.fill = GridBagConstraints.BOTH;
			myAdd(scrollPane, c);
		}

		validatePort();
		recalcInBackground();
	}

	private void updateResult (String s) {
		resultField.setText(s);
	}
	
	private boolean validatePort() {
		int port = Integer.parseInt(portField.getText().trim());
		if (isPortTaken (port)) {
			updateResult ("Port " + portField.getText().trim() + " is already taken by an existing process.  Is H2O already running?");
			return false;
		}
		
		return true;
	}
	
	public void actionPerformed(ActionEvent evt) {
		String actionCommand = evt.getActionCommand();

		if (actionCommand.equals (CLOUD_COMMAND)) {
			String s = cloudField.getText();
			String s2 = s.trim();
			if (s2.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
				updateResult ("Cloud name set successfully");
			}
			else {
				updateResult ("Cloud name must start with a letter and contain only letters, numbers and underscores");
			}
		}
		else if (actionCommand.equals (PORT_COMMAND)) {
			String s = portField.getText();
			String s2 = s.trim();
			Integer i = Integer.parseInt (s);
			String s3 = i.toString();
			
			if (! s2.equals (s3)) {
				updateResult ("Illegal characters in port");
			}
			else if (i < 1) {
				updateResult ("Port must be greater than 0");
			}
			else if (i >= 65535) {
				updateResult ("Port must be less than 65535");
			}
			else if (! validatePort()) {
				// no nothing, validatePort calls updateResult directly.
			}
			else {
				updateResult ("Port set successfully");
			}
		}
		else if (actionCommand.equals (XMX_COMMAND)) {
			String s = xmxField.getText();
			String s2 = s.trim();
			if (s2.matches("^[1-9][0-9]*[MGmg]$")) {
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
			h2oStatusLabel.setText(H2O_STATUS_RUNNING);
			startButton.setEnabled(false);
			stopButton.setEnabled(true);
			stopButton.requestFocusInWindow();
		}
		else if (status == H2oStatus.NOT_RUNNING) {
			h2oStatusLabel.setText(H2O_STATUS_NOT_RUNNING);
			startButton.setEnabled(true);
			stopButton.setEnabled(false);
			startButton.requestFocusInWindow();
		}		
	}

	// Call this from UI thread.
	private static void createAndShowGUI() {
		//Create and set up the window.
		JFrame frame = new JFrame("H2O Launcher");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		//Add contents to the window.
		h2oLauncher = new H2OLauncher();
		frame.add(h2oLauncher);

		//Display the window.
		frame.pack();
		frame.setVisible(true);
	}

	public static void main(String[] args) {
		//Schedule a job for the event dispatch thread:
		//creating and showing this application's GUI.
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				createAndShowGUI();
			}
		});
		
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
