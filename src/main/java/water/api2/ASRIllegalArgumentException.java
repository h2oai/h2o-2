package water.api2;

/**
 * A specific Exception to carry argument error information.
 */
public class ASRIllegalArgumentException extends IllegalArgumentException {
  private ASRArgumentErrorInfo _ei;

  ASRIllegalArgumentException(ASRArgumentErrorInfo ei) {
    _ei = ei;
  }

  public ASRArgumentErrorInfo getErrorInfo() { return _ei; }
}
