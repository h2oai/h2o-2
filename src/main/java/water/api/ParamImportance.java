package water.api;
/** Parameter importance category */
public enum ParamImportance {
  UNIMPORTANT("unimportant", "The parameter does not affect model quality."),
  CRITICAL   ("critical",    "The parameter is critical for model quality."),
  SECONDARY  ("secondary",   "The parameter is important for model quality."),
  EXPERT     ("expert",      "Expert parameter.");
  /** Printable name */
  public final String title;
  public final String help;
  private ParamImportance(String title, String help) {
    this.title = title; this.help = help;
  }
}
