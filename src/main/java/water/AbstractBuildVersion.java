package water;

abstract public class AbstractBuildVersion {
    abstract public String branchName();
    abstract public String lastCommitHash();
    abstract public String describe();
    abstract public String compiledOn();
    abstract public String compiledBy();
}
