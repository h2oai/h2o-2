package water.api;


import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

import water.Boot;

/**
 * Basic page introducing tutorial for Random Forest on Iris
 *
 * @author michal
 *
 */
abstract public class TutorialWorkflow extends HTMLOnlyRequest {

  protected final Int _step              = new Int(STEP, 1);

  /** Returns a workflow to show */
  protected abstract TutorWorkflow getWorkflow();

  @Override
  protected String build(Response response) {
    StringBuilder sb = new StringBuilder();
    sb.append("<script type='text/javascript' src='tutorials/js/basic.js'></script>");
    decorateWorkflow(getWorkflow(), sb, _step.value());

    return sb.toString();
   }

  /** Shows the active workflow step */
  protected void decorateActiveStep(final TutorStep step, StringBuilder sb) {
    sb.append("<h4>").append(step.summary()).append("</h4>");
    sb.append(step.content());
  }

  protected void decorateWorkflow(final TutorWorkflow twf, StringBuilder sb, int activeStepNum) {
    int len = twf.length();
    TutorStep activeStep = twf.getStep(activeStepNum);

    // Format tutorial header
    sb.append("<div class='container' style='margin: 0px auto'>");
    sb.append("<h2>").append(twf.title()).append("</h2>");
    sb.append("<blockquote><p>").append(activeStep.summary()).append("</p>");
    sb.append("<small>Step ").append(activeStepNum).append(" of ").append(len).append("</small>");
    sb.append("</blockquote>");

    // Container for left, right columns
    sb.append("<div class='row'>" );
    // Append left column with list of tutorial steps
    sb.append("<div class='span3'>");
    sb.append("<table class='table table-stripped'>");
    for (TutorStep ts : twf) {
      sb.append("<tr>");
      sb.append("<td><span class='label ").append(activeStepNum == ts.ord ? "label-info" : "" ).append("'>").append(ts.title()).append("</span></td>");
      sb.append("<td>");
      if (activeStepNum == ts.ord) {
        sb.append("<strong>").append(ts.summary()).append("</strong>");
      } else {
        sb.append("<a href='").append(getStepUrl(ts.ord)).append("'>");
        sb.append(ts.summary());
        sb.append("</a>");
      }
      sb.append("</td>");
      sb.append("</tr>");
    }
    sb.append("</table>");
    sb.append("</div>"); // Close container for left column

    // Append right column with tutorial step description
    sb.append("<div class='span7 hero-unit'>");
    decorateActiveStep(activeStep, sb);
    sb.append("</div>"); // Close container for right column
    sb.append("</div>"); // Close top-level row for left and right columns

    // Pager in the bottom of left/right column
    sb.append("<div class='row'><div class='span3'>&nbsp;</div><div class='span7'>");
    sb.append("<ul class='pager'>");
    String next = getStepUrl(activeStepNum+1);
    String prev = getStepUrl(activeStepNum-1);
    sb.append(activeStepNum > 1 ? "<li><a href='"+prev+"'>Previous</a></li>" : "<li class='disabled'><a href='#'>Previous</a></li>");
    sb.append(activeStepNum < len ? "<li><a href='"+next+"'>Next</a></li>" : "<li class='disabled'><a href='#'>Next</a></li>");
    sb.append("</ul>");
    sb.append("</div>");

    // Close top-level container
    sb.append("</div>");
  }

  String getStepUrl(int step) {
    return this.getClass().getSimpleName() + ".html?step=" + step;
  }

  /** A simple tutorial workflow representation */
  protected class TutorWorkflow implements Iterable<TutorStep> {
    private final ArrayList<TutorStep> _steps = new ArrayList<TutorialWorkflow.TutorStep>();
    private final String               _title;

    public TutorWorkflow(String title) {
      _title = title;
    }

    /** Add a new step into tutorial workflow */
    public void addStep(int num, TutorStep step) { _steps.add(step); step.ord = num; }
    /** Get tutorial step. Step parameter is 1-based. */
    public TutorStep getStep(int step) { return _steps.get(step-1); }

    @Override
    public Iterator<TutorStep> iterator() {
      return _steps.iterator();
    }

    public final int    length() { return _steps.size(); }
    public final String title()  { return _title; }
  }

  /** Simple tutorial step defined by its title, summary, and content. */
  protected abstract class TutorStep {
    int ord;
    /* Array storing step name, title, and content */
    protected final String[] _info;

    public final String title()   { return _info[0]; }
    public final String summary() { return _info[1]; }
    /* Override this method to provide the content */
    public abstract String content();

    public TutorStep(final String[] info) {
      assert info.length >= 2;
      _info = info;
    }
  }

  /** Tutorial step stored in file */
  protected class FileTutorStep extends TutorStep {
    private String _content;

    public FileTutorStep(String[] info) { super(info); assert info.length == 3; }

    @Override synchronized public final String content() {
      if (true || _content == null) _content = loadContent(_info[2]);
      return _content;
    }

    private String loadContent(String fromFile) {
      BufferedReader reader = null;
      StringBuilder sb = new StringBuilder();

      try {
        InputStream is = Boot._init.getResource2(fromFile);
        assert is != null : "Bundled resource " + fromFile + " does not exist!";

        reader = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while( (line = reader.readLine())!=null) sb.append(line).append('\n');

      } catch (IOException e){ /* Silently ignoring */
        e.printStackTrace();
      } finally {
        if (reader!=null) try { reader.close(); } catch( IOException e ) { assert false : "IOException during reader close."; }
      }

      return sb.toString();
    }
  }
}
