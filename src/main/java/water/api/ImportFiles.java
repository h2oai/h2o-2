package water.api;

import java.io.File;
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
    "Map a file from the local host filesystem into H2O memory.  Data is "+
    "loaded lazily, when the Key is read (usually in a Parse command, to build " +
    "a Hex key).  (Warning: Every host in the cluster must have this file visible locally!)";

  // HTTP REQUEST PARAMETERS
  @API(help="File or directory to import.")
  protected final GeneralFile path = new GeneralFile("path");

  // JSON OUTPUT FIELDS
  @API(help="Files imported.  Imported files are merely Keys mapped over the existing files.  No data is loaded until the Key is used (usually in a Parse command).")
  String[] files;

  @API(help="Keys of imported files, Keys map 1-to-1 with imported files.")
  String[] keys;

  @API(help="File names that failed the integrity check, can be empty.")
  String[] fails;

  @API(help="Prior Keys that matched a prefix of the imported path, and were removed prior to (re)importing")
  String[] dels;


  @Override public String[] DocExampleSucc() { return new String[]{"path","smalldata/airlines"}; }
  @Override public String[] DocExampleFail() { return new String[]{}; }

  @Override protected Response serve() {
    ArrayList<String> afiles = new ArrayList();
    ArrayList<String> akeys  = new ArrayList();
    ArrayList<String> afails = new ArrayList();
    ArrayList<String> adels  = new ArrayList();
    FileIntegrityChecker.check(new File(path.value()),false).syncDirectory(afiles,akeys,afails,adels);
    files = afiles.toArray(new String[0]);
    keys  = akeys .toArray(new String[0]);
    fails = afails.toArray(new String[0]);
    dels  = adels .toArray(new String[0]);
    return Response.done(this);
  }

  protected String parseLink(String k, String txt) { return Parse.link(k, txt); }
  // Auto-link to Parse
  String parse() { return "Parse.query"; }


  // HTML builder
  @Override public boolean toHTML( StringBuilder sb ) {
    if( files.length > 1 )
      sb.append("<div class='alert'>")
        .append(parseLink("*"+path.value()+"*", "Parse all into hex format"))
        .append(" </div>");

    DocGen.HTML.title(sb,"files");
    DocGen.HTML.arrayHead(sb);
    for( int i=0; i<files.length; i++ )
      sb.append("<tr><td><a href='"+parse()+"?source_key=").append(keys[i]).
        append("'>").append(files[i]).append("</a></td></tr>");
    DocGen.HTML.arrayTail(sb);

    if( fails.length > 0 )
      DocGen.HTML.array(DocGen.HTML.title(sb,"fails"),fails);
    if( dels.length > 0 )
      DocGen.HTML.array(DocGen.HTML.title(sb,"Keys deleted before importing"),dels);
    return true;
  }
}
