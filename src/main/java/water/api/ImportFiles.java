package water.api;

import java.util.ArrayList;
import water.Futures;
import water.Key;
import water.util.FileIntegrityChecker;

public class ImportFiles extends Request {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = 
    "  Map a file from the local host filesystem into H2O memory.  Data is "+
    "loaded lazily, when the Key is read (usually in a Parse command).  "+
    "(Warning: Every host in the cluster must have this file visible locally!)";

  // HTTP REQUEST PARAMETERS
  static final String pathHelp = "File or directory to import.";
  protected final ExistingFile path = new ExistingFile();


  // JSON OUTPUT FIELDS
  static final String filesHelp="Files imported.  Imported files are merely Keys mapped over the existing files.  No data is loaded until the Key is used (usually in a Parse command)";
  String[] files;

  static final String keysHelp="Keys of imported files, Keys map 1-to-1 with imported files.";
  String[] keys;

  static final String failsHelp="File names that failed the integrity check, can be empty.";
  String[] fails;

  static final String DOC_4 = 
    "Error JSON elements\n"+
    "-------------------\n"+
    "\n"+
    "   error [string]\n"+
    "\n"+
    "      Required argument not specified.\n"+
    "\n"+
    "      File not found.\n"+
    "\n"+
    "HTTP response codes\n"+
    "-------------------\n"+
    "   \n"+
    "   200 OK\n"+
    "\n"+
    "      Success and error responses are identical.\n"+
    "\n"+
    "Success Example\n"+
    "---------------\n"+
    "\n"+
    ".. literalinclude:: ImportFiles_success_1.rest\n"+
    "\n"+
    "Error Example\n"+
    "-------------\n"+
    "\n"+
    ".. literalinclude:: ImportFiles_error_1.rest\n";

  @Override protected Response serve() {
    FileIntegrityChecker c = FileIntegrityChecker.check(path.value());
    ArrayList<String> afails = new ArrayList();
    ArrayList<String> afiles = new ArrayList();
    ArrayList<String> akeys  = new ArrayList();
    Futures fs = new Futures();
    for( int i = 0; i < c.size(); ++i ) {
      Key k = c.importFile(i, fs);
      if( k == null ) {
        afails.add(c.getFileName(i));
      } else {
        afiles.add(c.getFileName(i));
        akeys .add(k.toString());
      }
    }
    fs.blockForPending();
    fails = afails.toArray(new String[0]);
    files = afiles.toArray(new String[0]);
    keys  = akeys .toArray(new String[0]);

    return new Response(Response.Status.done, this);
  }

  // HTML builder
  @Override public StringBuilder toHTML( StringBuilder sb ) {
    if( files.length > 1 )
      sb.append("<div class='alert'>")
        .append(Parse.link("*"+path.value()+"*", "Parse all into hex format"))
        .append(" </div>");

    DocGen.HTML.title(sb,"files");
    DocGen.HTML.arrayHead(sb);
    for( int i=0; i<files.length; i++ )
      sb.append("<tr><td><a href='Parse.html?source_key=").append(keys[i]).
        append("'>").append(files[i]).append("</a></td></tr>");
    DocGen.HTML.arrayTail(sb);

    if( fails.length > 0 )
      DocGen.HTML.array(DocGen.HTML.title(sb,"fails"),fails);

    return sb;
  }
}
