package water.exec3;


import org.junit.Test;
import water.*;
import com.google.gson.*;
import water.util.Log;


public class AST2IRTest extends TestUtil {

  @Test public static void test1() {
    Log.info("Checking the instructions produced by the JSON AST representing the R expression `hex + 5`");
    JsonObject ast = test1_json();
    printInstructions(ast);
  }

  @Test public static void test2() {
    Log.info("Checking the instructions produced by the JSON AST representing the R expression `hex + 5 + 10`");
    JsonObject ast = test2_json();
    printInstructions(ast);
  }

  @Test public static void test3() {
    Log.info("Checking the expression `hex + 5 - 1*hex + 15*(23/hex)`");
    JsonObject ast = test3_json();
    printInstructions(ast);
  }


  static void printInstructions(JsonObject ast) {
    AST2IR main = new AST2IR(ast);
    main.make();
    Log.info(main._toString());
  }

  static JsonObject test1_json() {
    JsonParser parser = new JsonParser();
    String s = "{\"astop\":{\"type\":\"BinaryOperator\",\"operator\":\"+\",\"infix\":true,\"node_type\":\"ASTOp\",\"operands\":{\"left\":{\"type\":\"Frame\",\"value\":\"Last.value.0\",\"node_type\":\"ASTFrame\"},\"right\":{\"type\":\"Numeric\",\"value\":5,\"node_type\":\"ASTNumeric\"}}}}";
    return (JsonObject)parser.parse(s);
  }

  static JsonObject test2_json() {
    JsonParser parser = new JsonParser();
    String s= "{\"astop\":{\"type\":\"BinaryOperator\",\"operator\":\"+\",\"infix\":true,\"node_type\":\"ASTOp\",\"operands\":{\"left\":{\"astop\":{\"type\":\"BinaryOperator\",\"operator\":\"+\",\"infix\":true,\"node_type\":\"ASTOp\",\"operands\":{\"left\":{\"type\":\"Frame\",\"value\":\"Last.value.0\",\"node_type\":\"ASTFrame\"},\"right\":{\"type\":\"Numeric\",\"value\":5,\"node_type\":\"ASTNumeric\"}}}},\"right\":{\"type\":\"numeric\",\"value\":10,\"node_type\":\"ASTNumeric\"}}}}";
    return (JsonObject)parser.parse(s);
  }

  static JsonObject test3_json() {
    JsonParser parser = new JsonParser();
    String s = "{\"astop\":{\"type\":\"BinaryOperator\",\"operator\":\"+\",\"infix\":true,\"node_type\":\"ASTOp\",\"operands\":{\"left\":{\"astop\":{\"type\":\"BinaryOperator\",\"operator\":\"-\",\"infix\":true,\"node_type\":\"ASTOp\",\"operands\":{\"left\":{\"astop\":{\"type\":\"BinaryOperator\",\"operator\":\"+\",\"infix\":true,\"node_type\":\"ASTOp\",\"operands\":{\"left\":{\"type\":\"Frame\",\"value\":\"Last.value.0\",\"node_type\":\"ASTFrame\"},\"right\":{\"type\":\"Numeric\",\"value\":5,\"node_type\":\"ASTNumeric\"}}}},\"right\":{\"astop\":{\"type\":\"BinaryOperator\",\"operator\":\"*\",\"infix\":true,\"node_type\":\"ASTOp\",\"operands\":{\"left\":{\"type\":\"numeric\",\"value\":1,\"node_type\":\"ASTNumeric\"},\"right\":{\"type\":\"Frame\",\"value\":\"Last.value.0\",\"node_type\":\"ASTFrame\"}}}}}}},\"right\":{\"astop\":{\"type\":\"BinaryOperator\",\"operator\":\"*\",\"infix\":true,\"node_type\":\"ASTOp\",\"operands\":{\"left\":{\"type\":\"numeric\",\"value\":15,\"node_type\":\"ASTNumeric\"},\"right\":{\"astop\":{\"type\":\"BinaryOperator\",\"operator\":\"/\",\"infix\":true,\"node_type\":\"ASTOp\",\"operands\":{\"left\":{\"type\":\"numeric\",\"value\":23,\"node_type\":\"ASTNumeric\"},\"right\":{\"type\":\"Frame\",\"value\":\"Last.value.0\",\"node_type\":\"ASTFrame\"}}}}}}}}}}";
    return (JsonObject)parser.parse(s);
  }

  public static void main(String[] args) {
    test1();
    test2();
    test3();
  }
}
