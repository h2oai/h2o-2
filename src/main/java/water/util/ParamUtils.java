package water.util;

import java.util.HashSet;
import java.util.Set;

import water.api.DocGen.FieldDoc;
import water.api.ParamImportance;

/** A helper class proving queries over Iced objects parameters. */
public class ParamUtils {

  /**
   * Names of the model parameters which will always be shown in a short description of
   * the model (e.g., for a tree model it would include ntrees and depth).
   */
  public static Set<String> getCriticalParamNames(FieldDoc[] doc) {
    return getParamNames(doc, ParamImportance.CRITICAL);
  }

  /**
   * Names of the model parameters which will also be shown in a longer description of
   * the model (e.g., learning rate).
   */
  public static Set<String> getSecondaryParamNames(FieldDoc[] doc) {
    return getParamNames(doc, ParamImportance.SECONDARY);
  }

  /**
   * Names of the model parameters which will be shown only in an expert view of
   * the model (e.g., for Deep Learning it would include initial_weight_scale).
   */
  public static Set<String> getExpertParamNames(FieldDoc[] doc) {
    return getParamNames(doc, ParamImportance.EXPERT);
  }

  public static Set<String> getParamNames(FieldDoc[] doc, ParamImportance filter) {
    HashSet<String> r = new HashSet<String>();
    for (FieldDoc d : doc) if (d.importance()==filter) r.add(d.name());
    return r;
  }
}
