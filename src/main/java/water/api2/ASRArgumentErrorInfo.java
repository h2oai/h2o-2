package water.api2;

public class ASRArgumentErrorInfo {
  public int error_code;
  public String error_message;
  public String error_parameter;

  public ASRArgumentErrorInfo(int c, String m, String p) {
    error_code = c;
    error_message = m;
    error_parameter = p;
  }
}