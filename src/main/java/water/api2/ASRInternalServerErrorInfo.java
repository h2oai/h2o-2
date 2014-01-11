package water.api2;

public class ASRInternalServerErrorInfo {
  public String error_message;
  public String stack_trace;

  public ASRInternalServerErrorInfo(String m, String t) {
    error_message = m;
    stack_trace = t;
  }
}
