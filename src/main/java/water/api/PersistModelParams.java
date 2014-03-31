package water.api;

import com.google.gson.*;
import water.Job;
import water.Key;
import water.Request2;
import water.util.RString;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PersistModelParams extends Request2 {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    static final public String PERSISTENCE_DIR = "/tmp";

    // TODO: why is this showing up twice in the _Arguments list of Request?

    @API(help = "JSON blob containing the model creation parameters.", required = false, filter = Default.class, validator = JsonValidator.class, gridable = false)
    String jsonStr = null;

    @API(help = "Model class for the desired model params.", required = false, filter = Default.class, validator = Validator.NOPValidator.class, gridable = false)
    String model = null;

    @API(help = "Name of saved the parameter set JSON blob containing the model creation parameters whose JSON we should GET.", required = false, filter = Default.class, validator = Validator.NOPValidator.class, gridable = false)
    String paramsName = null;

    static class JsonValidator extends Validator.NOPValidator<Map> {
        @Override public void validateRaw(String value) {
            // TODO: throw exception if parse fails.
            if (null == value || "".equals(value))
                throw new IllegalArgumentException("Body is empty; expected JSON string.");
        }
    }

    public static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static String modelClassForParams(JsonObject params) {
        String description = params.get("description").getAsString();
        if ("Distributed RF".equals(description))
            return "DRF";

        return "unknown";
    }


    public static String modelDirForClass(String modelClass) {
        return PERSISTENCE_DIR + "/" + modelClass + "/";
    }


    public static List<String> savedModelParamsForClass(String modelClass) {
        File dir = new File(modelDirForClass(modelClass));
        List<String> names = new ArrayList<String>();

        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    names.add(file.getName());
                }
            }

        }
        return names;
    }

    @Override
    protected Response serve() {
        String params = (String)this._parms.get("paramsName");

        if (null != paramsName&& !"".equals(paramsName)) {
            // GET: return the JSON for the file
//            String dir = modelDirForClass(modelClassForParams(json_object));
            String dir = modelDirForClass("DRF");
            String filename = dir + params;
            StringBuffer json = new StringBuffer();

            BufferedReader br = null;
            try {
                br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF-8"));
                String line = null;
                while ((line = br.readLine()) != null) {
                    json.append(line);
                }
            } catch (IOException e) {
                return Response.error("IO error reading model params JSON from file: " + filename + ".  Exception: " + e);
            } finally {
                try {
                    if (br != null) br.close();
                } catch (IOException e) {
                    return Response.error("IO error reading model params JSON from file: " + filename + ".  Exception: " + e);
                }
            }

            JsonObject response = new JsonParser().parse(json.toString()).getAsJsonObject();

/*
            JsonObject fake = new JsonObject();
            fake.addProperty("classification", "false");
            fake.addProperty("ntrees", 500);
            fake.addProperty("max_depth", 9999);
            fake.addProperty("min_rows", 1);
            fake.addProperty("nbins", 200);
            fake.addProperty("score_each_iteration", "true");

            JsonObject response = fake;
*/
            return Response.done(response);

        } else {
            // POST: save te JSON in the file
            String json_str = (String)this._parms.get("jsonStr");
            JsonObject json_object = gson.fromJson(json_str, JsonElement.class).getAsJsonObject();

            // JsonObject json_object = new JsonParser().parse(json_str).getAsJsonObject();
            String destination_key = json_object.get("destination_key").getAsString();
            String dir = modelDirForClass(modelClassForParams(json_object));
            String filename = dir + destination_key + ".json";

            boolean error = false;
            try {
                (new File(dir)).mkdirs();

                PrintWriter pw = new PrintWriter(filename);
                pw.println(gson.toJson(json_object));
                pw.flush();
                error = pw.checkError();
                pw.close();
            } catch (FileNotFoundException fnfe) {
                error = true;
            }

            JsonObject response = json_object;
            if (error) {
                return Response.error("IO error writing model params JSON to file: " + filename);
            } else {
                return Response.done(response);
            }
        }
    }

    @Override
    public RequestServer.API_VERSION[] supportedVersions() {
        return SUPPORTS_V1_V2;
    }
}
