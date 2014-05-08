package water;

abstract public class AbstractBuildVersion {
    abstract public String branchName();
    abstract public String lastCommitHash();
    abstract public String describe();
    abstract public String projectVersion();
    abstract public String compiledOn();
    abstract public String compiledBy();

    public String buildNumber() {
      String buildNumber = "(unknown)";
      try {
        String projectVersion = projectVersion();
        buildNumber = projectVersion.split("\\.")[3];
      }
      catch (Exception xe) {}
      return buildNumber;
    }

    @Override public String toString() {
    return "H2O v"+projectVersion()+ " ("+branchName()+" - "+lastCommitHash()+")";
    }

    /** Dummy version of H2O. */
    public static AbstractBuildVersion UNKNOWN_VERSION = new AbstractBuildVersion() {
      @Override public String projectVersion() { return "(unknown)"; }
      @Override public String lastCommitHash() { return "(unknown)"; }
      @Override public String describe()   { return "(unknown)"; }
      @Override public String compiledOn() { return "(unknown)"; }
      @Override public String compiledBy() { return "(unknown)"; }
      @Override public String branchName() { return "(unknown)"; }
    };
}
