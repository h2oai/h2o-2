package cookbook.groupedaverage;

import dontweave.gson.JsonObject;
import water.Key;
import water.Request2;
import water.UKV;
import water.api.DocGen;
import water.fvec.Frame;
import water.fvec.Vec;

public class GroupedAverage extends Request2 {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    @API(help = "Destination key", filter = Default.class, json = true, validator = DestKeyValidator.class)
    public Key destination_key; // Key holding final value after job is removed
    static class DestKeyValidator extends Validator.NOPValidator<Key> {
        @Override public void validateRaw(String value) {
            String pattern = "[a-zA-Z0-9._]+";
            if (! value.matches(pattern)) {
                throw new IllegalArgumentException("Invalid key (valid pattern regex is: '" + pattern + "')");
            }
        }
    }

    @API(help = "Source frame", required = true, filter = Default.class)
    public Frame source;

    @API(help="Column to use as group id", required=true, filter=groupFilter.class)
    public Vec group;
    class groupFilter extends VecClassSelect { groupFilter() { super("source"); } }

    @API(help="Column to generate an average for", required=true, filter=valueFilter.class)
    public Vec value;
    class valueFilter extends VecClassSelect { valueFilter() { super("source"); } }

    @Override public Response serve() {
        JsonObject json = new JsonObject();

        Frame fr = source.deepSlice(null, null);
        UKV.put(destination_key, fr);

        return Response.done(json);
    }
}
