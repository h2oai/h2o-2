package water.deploy;

import java.io.IOException;

import water.deploy.VM.SSH;

/**
 * Creates a node on a host.
 */
public class NodeHost implements Node {
  private final SSH _ssh;

  public NodeHost(Host host, String[] args) {
    _ssh = new SSH(host, VM.javaArgs(NodeVM.class.getName()), args);
  }

  public Host host() {
    return _ssh.host();
  }

  @Override public void inheritIO() {
    _ssh.inheritIO();
  }

  @Override public void persistIO(String outFile, String errFile) throws IOException {
    _ssh.persistIO(outFile, errFile);
  }

  @Override public void start() {
    _ssh.startThread();
  }

  @Override public int waitFor() {
    try {
      _ssh._thread.join();
    } catch( InterruptedException e ) {
    }
    return 0;
  }

  @Override public void kill() {
    try {
      _ssh.kill();
    } catch( Exception xe ) {
    }
  }
}
