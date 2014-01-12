package water.api2;

/**
 * A specific Exception to carry argument error information.
 */
public class ASRIllegalArgumentException extends IllegalArgumentException {
  private ASRErrorInfo _ei;

  ASRIllegalArgumentException(ASRErrorInfo ei) {
    _ei = ei;
  }

  public ASRErrorInfo getErrorInfo() { return _ei; }
}
