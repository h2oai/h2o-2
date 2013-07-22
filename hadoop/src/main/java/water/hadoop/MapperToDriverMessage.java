package water.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Simple class to help serialize messages from the Mapper to the Driver.
 */
class MapperToDriverMessage {
  public static final char TYPE_UNKNOWN = 0;
  public static final char TYPE_EOF_NO_MESSAGE = 1;
  public static final char TYPE_EMBEDDED_WEB_SERVER_IP_PORT = 2;
  public static final char TYPE_CLOUD_SIZE = 3;
  public static final char TYPE_EXIT = 4;


  private String _driverCallbackIp = null;
  private int _driverCallbackPort = -1;
  private char _type = TYPE_UNKNOWN;

  private String _embeddedWebServerIp = "";
  private int _embeddedWebServerPort = -1;
  private int _cloudSize = -1;
  private int _exitStatus = -1;

  public MapperToDriverMessage() {
  }

  // Readers
  // -------
  public char getType() { return _type; }
  public String getEmbeddedWebServerIp() { return _embeddedWebServerIp; }
  public int getEmbeddedWebServerPort() { return _embeddedWebServerPort; }
  public int getCloudSize() { return _cloudSize; }
  public int getExitStatus() { return _exitStatus; }

  public void readMessage(Socket s) throws Exception {
    _type = readType(s);

    if (_type == TYPE_EMBEDDED_WEB_SERVER_IP_PORT) {
      _embeddedWebServerIp = readString(s);
      _embeddedWebServerPort = readInt(s);
    }
    else if (_type == TYPE_CLOUD_SIZE) {
      _embeddedWebServerIp = readString(s);
      _embeddedWebServerPort = readInt(s);
      _cloudSize = readInt(s);
    }
    else if (_type == TYPE_EXIT) {
      _embeddedWebServerIp = readString(s);
      _embeddedWebServerPort = readInt(s);
      _exitStatus = readInt(s);
    }
    else {
      // Ignore unknown types.
    }
  }

  // Writers
  // -------

  public void setDriverCallbackIpPort(String ip, int port) {
    _driverCallbackIp = ip;
    _driverCallbackPort = port;
  }

  public String getDriverCallbackIp() { return _driverCallbackIp; }
  public int getDriverCallbackPort() { return _driverCallbackPort; }

  public void setMessageEmbeddedWebServerIpPort(String ip, int port) {
    _type = TYPE_EMBEDDED_WEB_SERVER_IP_PORT;
    _embeddedWebServerIp = ip;
    _embeddedWebServerPort = port;
  }

  public void setMessageCloudSize(String ip, int port, int cloudSize) {
    _type = TYPE_CLOUD_SIZE;
    _embeddedWebServerIp = ip;
    _embeddedWebServerPort = port;
    _cloudSize = cloudSize;
  }

  public void setMessageExit(String ip, int port, int exitStatus) {
    _type = TYPE_EXIT;
    _embeddedWebServerIp = ip;
    _embeddedWebServerPort = port;
    _exitStatus = exitStatus;
  }

  public void write(Socket s) throws Exception {
    if (_type == TYPE_EMBEDDED_WEB_SERVER_IP_PORT) {
      writeMessageEmbeddedWebServerIpPort(s);
    }
    else if (_type == TYPE_CLOUD_SIZE) {
      writeMessageCloudSize(s);
    }
    else if (_type == TYPE_EXIT) {
      writeMessageExit(s);
    }
    else {
      throw new Exception("MapperToDriverMessage: write: Unknown type");
    }
  }

  //-----------------------------------------------------------------
  // Private below this line.
  //-----------------------------------------------------------------

  // Readers
  // -------
  private int readBytes(Socket s, byte[] b) throws Exception {
    int bytesRead = 0;
    int bytesExpected = b.length;
    InputStream is = s.getInputStream();
    while (bytesRead < bytesExpected) {
      int n = is.read(b, bytesRead, bytesExpected - bytesRead);
      if (n < 0) {
        return n;
      }
      bytesRead += n;
    }

    return bytesRead;
  }

  private char readType(Socket s) throws Exception {
    byte b[] = new byte[1];
    int n = readBytes(s, b);
    if (n < 0) {
      return TYPE_EOF_NO_MESSAGE;
    }
    // System.out.println("readType: " + b[0]);
    return (char) b[0];
  }

  private int readInt(Socket s) throws Exception {
    byte b[] = new byte[4];
    int n = readBytes(s, b);
    if (n < 0) {
      throw new IOException("MapperToDriverMessage: readBytes failed");
    }

    int i =
            (
                    ((((int) b[0]) << (8*0)) & 0x000000ff) |
                    ((((int) b[1]) << (8*1)) & 0x0000ff00) |
                    ((((int) b[2]) << (8*2)) & 0x00ff0000) |
                    ((((int) b[3]) << (8*3)) & 0xff000000)
            );
    // System.out.println("readInt: " + i);
    // System.out.println("readInt: " + b[0] + " " + b[1] + " " + b[2] + " " + b[3]);

    return i;
  }

  private String readString(Socket s) throws Exception {
    int length = readInt(s);

    byte b[] = new byte[length];
    int n = readBytes(s, b);
    if (n < 0) {
      throw new IOException("MapperToDriverMessage: readBytes failed");
    }

    String str = new String(b, "UTF-8");
    // System.out.println("readString: " + str);
    return str;
  }

  // Writers
  // -------
  private void writeBytes(Socket s, byte[] b) throws Exception {
    OutputStream os = s.getOutputStream();
    os.write(b);
  }

  private void writeType(Socket s, int type) throws Exception {
    byte b[] = new byte[1];
    b[0] = (byte)(char)type;
    writeBytes(s, b);
  }

  private void writeInt(Socket s, int i) throws Exception {
    byte b[] = new byte[4];
    b[0] = (byte)((i >> (8*0)) & 0xff);
    b[1] = (byte)((i >> (8*1)) & 0xff);
    b[2] = (byte)((i >> (8*2)) & 0xff);
    b[3] = (byte)((i >> (8*3)) & 0xff);
    // System.out.println("writeInt: " + b[0] + " " + b[1] + " " + b[2] + " " + b[3]);
    writeBytes(s, b);
  }

  private void writeString(Socket s, String str) throws Exception {
    byte b[] = str.getBytes("UTF-8");
    writeInt(s, b.length);
    writeBytes(s, b);
  }

  private void writeMessageEmbeddedWebServerIpPort(Socket s) throws Exception {
    writeType(s, TYPE_EMBEDDED_WEB_SERVER_IP_PORT);
    writeString(s, _embeddedWebServerIp);
    writeInt(s, _embeddedWebServerPort);
    s.getOutputStream().flush();
    s.close();
  }

  private void writeMessageCloudSize(Socket s) throws Exception {
    writeType(s, TYPE_CLOUD_SIZE);
    writeString(s, _embeddedWebServerIp);
    writeInt(s, _embeddedWebServerPort);
    writeInt(s, _cloudSize);
    s.getOutputStream().flush();
    s.close();
  }

  private void writeMessageExit(Socket s) throws Exception {
    writeType(s, TYPE_EXIT);
    writeString(s, _embeddedWebServerIp);
    writeInt(s, _embeddedWebServerPort);
    writeInt(s, _exitStatus);
    s.getOutputStream().flush();
    s.close();
  }
}
