
package water.exec;

import water.exec.RLikeParser.Token;

/**
 *
 * @author peta
 */
public class ParserException extends PositionedException {
  
  public ParserException(int pos, String msg) { super(pos, msg); }
  
  public ParserException(int pos, Token.Type expected, Token.Type found) { super(pos, "Expected token "+expected.toString()+", but "+found.toString()+" found"); }
  
  public ParserException(int pos, String expected, Token.Type found) { super(pos, "Expected "+expected+", but "+found.toString()+" found"); }
  
}
