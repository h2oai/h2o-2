package water.api;

import java.io.*;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import water.Arguments;
import water.H2O;
import water.util.IndentingAppender;

public class DocGen {
  public static class OptArgs extends Arguments.Opt {
    public String type = "python";
    public String destination = "docs/gen/";
  }

  public static void main(String[] args) throws IOException {
    H2O.main(new String[0]);
    OptArgs options = new OptArgs();
    new Arguments(args).extract(options);

    if(options.type.equalsIgnoreCase("wiki")) {
      generateWiki(options);
    } else if(options.type.equalsIgnoreCase("python")) {
      genereratePython(options);
    } else {
      System.err.println("Unknown documentation type: " + options.type);
    }

    System.exit(0);
  }

  private static void genereratePython(OptArgs options) throws IOException {
    File f = new File(options.destination);
    if( !options.destination.endsWith(".py") ) {
      f = new File(f, "h2o_base.py");
    }
    IndentingAppender ia = new IndentingAppender(Files.newWriter(f, Charsets.UTF_8));
    ia.appendln("##################################");
    ia.appendln("### GENERATED CODE DO NOT EDIT ###");
    ia.appendln("##################################");
    ia.appendln("import requests");
    ia.appendln("");
    ia.appendln("class H2OBase(object):");
    ia.incrementIndent();
    ia.appendln("'''");
    ia.appendln("A generated base class with arguments and documentation");
    ia.appendln("for accessing H2O's REST API from python");
    ia.appendln("'''");

    ia.appendln("").appendln("def __url(self, loc, port=None):");
    ia.incrementIndent();
    ia.appendln("raise Exception('__url should be implemented by a base class')");
    ia.decrementIndent();

    ia.appendln("").appendln("def __check_request(self, r, extraComment=None):");
    ia.incrementIndent();
    ia.appendln("raise Exception('__check_requests should be implemented by a base class')");
    ia.decrementIndent();

    Map<String, Request> requests = RequestServer._requests;
    for( Request r : requests.values() ) {
      r.buildPython(ia.appendln(""));
    }

    ia.close();
  }

  private static void generateWiki(OptArgs options) {
    System.err.println("Generate WIKI unimplemented.");
  }

}

