package water.exec;

import water.AutoBuffer;
import water.Key;

/**
 *
 * @author peta
 */
public class RLikeParser {

  // ---------------------------------------------------------------------------
  // Lexer part
  //
  // A simple lexer that can support a LL(1) grammar for the time being.
  // We might get more complicated here in the future. But for now, I can't care
  // less.
  // ---------------------------------------------------------------------------
  public static class Token {

    public enum Type {

      ttFloat, // any floating point number
      ttInteger, // integer number
      ttIdent, // any identifier
      ttString,
      ttOpAssign, // assignment, = or <-
      ttOpRightAssign, // -> assignment in R
      ttOpDollar, // $
      ttOpAdd, // +
      ttOpSub, // -
      ttOpMul, // *
      ttOpDiv, // /
      ttOpMod, // %
      ttOpParOpen, // (
      ttOpParClose, // )
      ttOpBracketOpen, // [
      ttOpBracketClose, // ]
      ttOpLess, // <
      ttOpGreater, // >
      ttOpEq, // ==
      ttOpNeq, // !=
      ttOpLessOrEq, // <=
      ttOpGreaterOrEq, //>=
      ttOpAnd, // &&
      ttOpOr, // ||
      ttOpNot, // !
      ttOpComma, // ,
      ttOpIif, // ?
      ttOpColon, // :
      ttEOF,
      ttUnknown,;

      public String toString() {
        switch( this ) {
          case ttFloat:
            return "float";
          case ttInteger:
            return "integer";
          case ttIdent:
            return "identifier";
          case ttString:
            return "string literal";
          case ttOpAssign:
            return "assignment";
          case ttOpRightAssign:
            return "assignment to the right";
          case ttOpDollar:
            return "membership $";
          case ttOpAdd:
            return "operator +";
          case ttOpSub:
            return "operator -";
          case ttOpMul:
            return "operator *";
          case ttOpDiv:
            return "operator /";
          case ttOpMod:
            return "operator %";
          case ttOpLess:
            return "operator <";
          case ttOpGreater:
            return "operator >";
          case ttOpLessOrEq:
            return "operator <=";
          case ttOpGreaterOrEq:
            return "operator >=";
          case ttOpEq:
            return "operator ==";
          case ttOpNeq:
            return "operator !=";
          case ttOpAnd:
            return "operator &&";
          case ttOpOr:
            return "operator ||";
          case ttOpNot:
            return "operator !";
          case ttOpParOpen:
            return "opening parenthesis";
          case ttOpParClose:
            return "closing parenthesis";
          case ttOpBracketOpen:
            return "opening bracket";
          case ttOpBracketClose:
            return "closing bracket";
          case ttOpComma:
            return "comma ','";
          case ttOpIif:
            return "inline if '?'";
          case ttOpColon:
            return "colon ':'";
          case ttEOF:
            return "end of input";
          default:
            return "unknown token";
        }
      }
    }
    public final Type _type;
    public final double _value;
    public final int _valueInt;
    public final String _id;
    public final int _pos;

    public Token(int pos, Type type) {
      _pos = pos;
      this._type = type;
      _value = 0;
      _valueInt = 0;
      _id = "";
    }

    public Token(int pos, double d) {
      _pos = pos;
      this._type = Type.ttFloat;
      _value = d;
      _valueInt = (int) d;
      _id = "";
    }

    public Token(int pos, int i) {
      _pos = pos;
      this._type = Type.ttInteger;
      _value = i;
      _valueInt = i;
      _id = "";
    }

    public Token(int pos, String s) {
      this(pos,s,Type.ttIdent);
    }

    public Token(int pos, String s, Type type) {
      _pos = pos;
      this._type = type;
      _value = 0;
      _valueInt = 0;
      _id = s;
    }

  }
  private AutoBuffer _s;
  private Token _top;

  protected Token top() {
    return _top;
  }

  protected Token pop() throws ParserException {
    Token x = _top;
    _top = parseNextToken();
    return x;
  }

  protected Token pop(Token.Type type) throws ParserException {
    if( top()._type != type )
      throw new ParserException(top()._pos, type, top()._type);
    return pop();
  }

  protected boolean condPop(Token.Type type) throws ParserException {
    if (top()._type == type) {
      pop();
      return true;
    } else {
      return false;
    }
  }

  private void skipWhitespace() {
    char c;
    while( true ) {
      if( _s.eof() )
        break;
      c = (char) _s.peek1();
      if( (c == ' ') || (c == '\t') || (c == '\n') ) {
        _s.get1();
        continue;
      }
      break;
    }
  }

  private boolean isCharacter(char c) {
    if( (c >= 'a') && (c <= 'z') )
      return true;
    if( (c >= 'A') && (c <= 'Z') )
      return true;
    return c == '_';
  }

  private boolean isDigit(char c) {
    return (c >= '0') && (c <= '9');
  }

  private Token parseNextToken() throws ParserException {
    skipWhitespace();
    int pos = _s.position();
    if( _s.eof() )
      return new Token(pos, Token.Type.ttEOF);
    char c = (char) _s.get1();
    switch( c ) {
      case '=':
        if (_s.peek1() == '=') {
          _s.get1();
          return new Token(pos, Token.Type.ttOpEq);
        } else {
          return new Token(pos, Token.Type.ttOpAssign);
        }
      case '&':
        if (_s.peek1() == '&') {
          _s.get1();
          return new Token(pos, Token.Type.ttOpAnd);
        } else {
          throw new ParserException(pos,"&& expected");
        }
      case '?': return new Token(pos, Token.Type.ttOpIif);
      case ':': return new Token(pos, Token.Type.ttOpColon);
      case '$': return new Token(pos, Token.Type.ttOpDollar);
      case '%': return new Token(pos, Token.Type.ttOpMod);
      case '+': return new Token(pos, Token.Type.ttOpAdd);
      case '-':
        if( _s.peek1() == '>' ) {
          _s.get1();
          return new Token(pos, Token.Type.ttOpRightAssign);
        } else {
          return new Token(pos, Token.Type.ttOpSub);
        }
      case '*': return new Token(pos, Token.Type.ttOpMul);
      case '/': return new Token(pos, Token.Type.ttOpDiv);
      case '(': return new Token(pos, Token.Type.ttOpParOpen);
      case ')': return new Token(pos, Token.Type.ttOpParClose);
      case '[': return new Token(pos, Token.Type.ttOpBracketOpen);
      case ']': return new Token(pos, Token.Type.ttOpBracketClose);
      case '<':
        if( _s.peek1() == '-' ) {
          _s.get1();
          return new Token(pos, Token.Type.ttOpAssign);
        } else if (_s.peek1() == '=') {
          _s.get1();
          return new Token(pos, Token.Type.ttOpLessOrEq);
        } else {
          return new Token(pos, Token.Type.ttOpLess);
        }
      case '>':
        if (_s.peek1() == '=') {
          _s.get1();
          return new Token(pos, Token.Type.ttOpGreaterOrEq);
        } else {
          return new Token(pos, Token.Type.ttOpGreater);
        }
      case ',': return new Token(pos,Token.Type.ttOpComma);
      case '!':
        if (_s.get1() != '=')
          throw new ParserException(pos," != operator expected");
        return new Token(pos,Token.Type.ttOpNeq);
      case '"':
      case '\'':
        return parseString();
      case '|':
        if (_s.get1() =='|') {
          return new Token(pos, Token.Type.ttOpOr);
        }
        // back up for both pipes
        _s.position(_s.position() - 2);
        return parseIdent();
      default:
        _s.position(_s.position() - 1);
        if( isCharacter(c) ) return parseIdent();
        if( isDigit(c)     ) return parseNumber();
    }
    return new Token(pos, Token.Type.ttUnknown);
  }

  private Token parseString() throws ParserException {
    int start = _s.position();
    char end = (char) _s.get1() == '"' ? '"' : '\'';
    StringBuilder sb = new StringBuilder();
    while (true) {
      if( _s.eof() )
        throw new ParserException(start, "String does not finish before the end of input");
      if( _s.peek1() == end ) {
        _s.get1();
        break; // end of the string
      } else if( _s.peek1() == '\\' ) {
        _s.get1();
        if( _s.eof() )
          throw new ParserException(start, "String does not finish before the end of input");
        char add;
        switch( _s.peek1() ) {
          case '"':
          case '\\':
          case '\'':
            add = (char) _s.peek1();
            break;
          case 'n':
            add = '\n';
            break;
          case 't':
            add = '\t';
            break;
          default:
            throw new ParserException(start, "Quotes slashes and \\n and \\t are allowed to be slashed in strings.");
        }
        _s.get1();
        sb.append(add);
      } else {
        sb.append((char) _s.get1());
      }
    }
    return new Token(start,sb.toString(),Token.Type.ttString);
  }

  private Token parseIdent() throws ParserException {
    int start = _s.position();
    if( _s.peek1() == '|' ) { // escaped string
      _s.get1();
      StringBuilder sb = new StringBuilder();
      while( true ) {
        if( _s.eof() )
          throw new ParserException(start, "String does not finish before the end of input");
        if( _s.peek1() == '|' ) {
          _s.get1();
          break; // end of the string
        } else if( _s.peek1() == '\\' ) {
          _s.get1();
          if( _s.eof() )
            throw new ParserException(start, "String does not finish before the end of input");
          switch( _s.peek1() ) {
            case '|':
            case '\\':
              break;
            default:
              throw new ParserException(start, "Only pipe and backslash can be escaped in idents.");
          }
        }
        sb.append((char) _s.get1());
      }
      return new Token(start, sb.toString());
    } else {
      while( true ) {
        if( _s.eof() )
          break;
        char c = (char) _s.peek1();
        if( isCharacter(c) || isDigit(c) || (c == '.') ) {
          _s.get1();
          continue;
        }
        break;
      }
    }
    return new Token(start, _s.getStr(start, _s.position()-start));
  }

  private Token parseNumber() throws ParserException {
    int start = _s.position();
    boolean dot = false;
    boolean e = false;
    while( true ) {
      if( _s.eof() )
        break;
      char c = (char) _s.peek1();
      if( isDigit(c) ) {
        _s.get1();
        continue;
      }
      if( c == '.' ) {
        if( dot != false )
          throw new ParserException(_s.position(), "Only one dot can be present in number.");
        dot = true;
        _s.get1();
        continue;
      }
      if( (c == 'e') || (c == 'E') ) {
        if( e != false )
          throw new ParserException(_s.position(), "Only one exponent can be present in number.");
        e = true;
        _s.get1();
        continue;
      }
      break;
    }
    if( (dot == false) && (e == false) )
      return new Token(start, Integer.parseInt(_s.getStr(start, _s.position()-start)));
    else
      return new Token(start, Double.parseDouble(_s.getStr(start, _s.position()-start)));
  }

  // ---------------------------------------------------------------------------
  // Parser part
  //
  // A simple LL(1) recursive descent guy. With the following grammar:
  public Expr parse(String x) throws ParserException {
    return parse(new AutoBuffer(x.getBytes()));
  }

  public Expr parse(AutoBuffer x) throws ParserException {
    _s = x;
    pop(); // load the first token in the stream
    Expr result = parse_S();
    pop(Token.Type.ttEOF); // make sure we have parsed everything
    return result;
  }

  /**
   * Parses the expression in R.
   *
   * S -> e | E [ -> ident ]
   *
   */
  private Expr parse_S() throws ParserException {
    if( top()._type == Token.Type.ttEOF )
      return null;
    Expr result = parse_E();
    if( top()._type == Token.Type.ttOpRightAssign ) {
      int pos = pop()._pos;
      result = new AssignmentOperator(pos, Key.make(pop()._id), result);
    }
    return result;
  }

  /** E -> E1 [ ? E : E ]
   *
   * @return
   * @throws ParserException
   */

  private Expr parse_E() throws ParserException {
    int pos = top()._pos;
    Expr result = parse_E1();
    if (top()._type == Token.Type.ttOpIif) {
      pop();
      Expr ifTrue = parse_E();
      pop(Token.Type.ttOpColon);
      Expr ifFalse = parse_E();
      result = new Iif(pos, result, ifTrue, ifFalse);
    }
    return result;
  }

  /*
   *
   *
   * E1 -> E2 ( && | '||' ) E2 }
   *
   * @return
   */
  private Expr parse_E1() throws ParserException {
    Expr result = parse_E2();
    while( (top()._type == Token.Type.ttOpAnd) || (top()._type == Token.Type.ttOpOr) ) {
      Token t = pop();
      result = new BinaryOperator(t._pos, t._type, result, parse_E2());
    }
    return result;
  }
  /*
   *
   *
   * E2 -> E3 ( == | != ) E3 }
   *
   * @return
   */
  private Expr parse_E2() throws ParserException {
    Expr result = parse_E3();
    while( (top()._type == Token.Type.ttOpEq) || (top()._type == Token.Type.ttOpNeq) ) {
      Token t = pop();
      result = new BinaryOperator(t._pos, t._type, result, parse_E3());
    }
    return result;
  }


  /*
   *
   *
   * E3 -> E4 { ( < | > | <= | >= ) E4 }
   *
   * @return
   */
  private Expr parse_E3() throws ParserException {
    Expr result = parse_E4();
    while( (top()._type == Token.Type.ttOpLess)
            || (top()._type == Token.Type.ttOpLessOrEq)
            || (top()._type == Token.Type.ttOpGreater)
            || (top()._type == Token.Type.ttOpGreaterOrEq) ) {
      Token t = pop();
      result = new BinaryOperator(t._pos, t._type, result, parse_E4());
    }
    return result;
  }

  /*
   *
   *
   * E4 -> T { + T | - T }
   *
   * @return
   */
  private Expr parse_E4() throws ParserException {
    Expr result = parse_T();
    while( (top()._type == Token.Type.ttOpAdd) || (top()._type == Token.Type.ttOpSub) ) {
      Token t = pop();
      result = new BinaryOperator(t._pos, t._type, result, parse_T());
    }
    return result;
  }

  /*
   * T -> F { * F | / F | % F }
   */
  private Expr parse_T() throws ParserException {
    Expr result = parse_F();
    while( (top()._type == Token.Type.ttOpMul) || (top()._type == Token.Type.ttOpDiv) || (top()._type == Token.Type.ttOpMod)) {
      Token t = pop();
      result = new BinaryOperator(t._pos, t._type, result, parse_T());
    }
    return result;
  }



  /*
   * F -> F2 [ $ ident | '[' number ']' ]
   */
  private Expr parse_F() throws ParserException {
    Expr f = parse_F2();
    switch (top()._type) {
      case ttOpDollar:
        int pos = pop()._pos;
        return new StringColumnSelector(pos, f, pop(Token.Type.ttIdent)._id);
      case ttOpBracketOpen:
        pos = pop()._pos;
        int idx = pop(Token.Type.ttInteger)._valueInt;
        pop(Token.Type.ttOpBracketClose);
        return new ColumnSelector(pos, f, idx);
      default:
        return f;
    }
  }


  /*
   * This is silly grammar for now, I need to understand R more to make it
   *
   * F2 -> - STRING | number | FUNCTION | ident ( = S | $ ident | [ number ] ) | ( S )
   */
  private Expr parse_F2() throws ParserException {
    int pos = top()._pos;
    switch( top()._type ) {
      case ttOpSub:
        return new UnaryOperator(pos, pop()._type, parse_F());
      case ttFloat:
        return new FloatLiteral(pos, pop()._value);
      case ttInteger:
        return new FloatLiteral(pos, pop()._value);
      case ttString:
        return new StringLiteral(pos, pop()._id);
      case ttIdent: {
        Token t = pop();
        if( top()._type == Token.Type.ttOpAssign ) {
          pos = pop()._pos;
          Expr rhs = parse_S();
          return new AssignmentOperator(pos, Key.make(t._id), rhs);
        } else if (top()._type == Token.Type.ttOpParOpen) {
          return parse_Function(t);
        } else {
          return new KeyLiteral(t._pos, t._id);
        }
      }
      case ttOpParOpen: {
        pop();
        Expr e = parse_S();
        pop(Token.Type.ttOpParClose);
        return e;
      }
      default:
        throw new ParserException(top()._pos, "Number or parenthesis", top()._type);
    }
  }

  /*
   * FUNCTION -> fName '(' [ FARG {, FARG } ] ')'
   *
   * where fName is already parsed
   */

  private Expr parse_Function(Token fName) throws ParserException {
    FunctionCall result = new FunctionCall(fName._pos,fName._id);
    pop(Token.Type.ttOpParOpen);
    int argIndex = 0;
    boolean haveSeenNamedArg = false;
    if (top()._type != Token.Type.ttOpParClose) {
      haveSeenNamedArg = parse_FunctionArgument(result,argIndex,haveSeenNamedArg);
      ++argIndex;
      while (condPop(Token.Type.ttOpComma)) {
        haveSeenNamedArg = parse_FunctionArgument(result,argIndex,haveSeenNamedArg);
        ++argIndex;
      }
    }
    pop(Token.Type.ttOpParClose);
    result.staticArgumentVerification();
    return result;
  }

  /*
   * FARG -> argname = S | S
   */

  private boolean parse_FunctionArgument(FunctionCall f, int argIndex, boolean seenNamedArgsBefore) throws ParserException {
    Token t = top();
    if (argIndex >= f._function.numArgs())
      throw new ParserException(t._pos,"Too many arguments to function");
    if ((t._type == Token.Type.ttIdent) && (f._function.argIndex(t._id)!=-1)) { // it is an argument and it does exist in the function
      pop();
      if (condPop(Token.Type.ttOpAssign)) {
        Expr e = parse_S();
        f.addArgument(e, t._id);
        return true;
      }
      _s.position(t._pos);
    }
    if (seenNamedArgsBefore)
      throw new ParserException(t._pos,"After a named argument is used, only named arguments can follow");
    Expr e = parse_S();
    f.addArgument(e, argIndex);
    return false;
  }
}


