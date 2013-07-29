package water.api;

import java.io.File;
import java.util.ArrayList;

import water.Futures;
import water.Key;
import water.Weaver.Weave;
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
  @Weave(help="File or directory to import.")
  protected final ExistingFile path = new ExistingFile("path");

  // JSON OUTPUT FIELDS
  @Weave(help="Files imported.  Imported files are merely Keys mapped over the existing files.  No data is loaded until the Key is used (usually in a Parse command).")
  String[] files;

  @Weave(help="Keys of imported files, Keys map 1-to-1 with imported files.")
  String[] keys;

  @Weave(help="File names that failed the integrity check, can be empty.")
  String[] fails;

  @Override public String[] DocExampleSucc() { return new String[]{"path","smalldata/airlines"}; }
  @Override public String[] DocExampleFail() { return new String[]{}; }

  FileIntegrityChecker load(File path) {
    return FileIntegrityChecker.check(path,false);
  }

  @Override protected Response serve() {
    FileIntegrityChecker c = load(path.value());
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
    return new Response(Response.Status.done, this, -1, -1, null);
  }

  // Auto-link to Parse
  String parse() { return "Parse.html"; }

  // HTML builder
  @Override public boolean toHTML( StringBuilder sb ) {
    if( files.length > 1 )
      sb.append("<div class='alert'>")
        .append(Parse.link("*"+path.value()+"*", "Parse all into hex format"))
        .append(" </div>");

    DocGen.HTML.title(sb,"files");
    DocGen.HTML.arrayHead(sb);
    for( int i=0; i<files.length; i++ )
      sb.append("<tr><td><a href='"+parse()+"?source_key=").append(keys[i]).
        append("'>").append(files[i]).append("</a></td></tr>");
    DocGen.HTML.arrayTail(sb);

    if( fails.length > 0 )
      DocGen.HTML.array(DocGen.HTML.title(sb,"fails"),fails);

    return true;
  }
}
