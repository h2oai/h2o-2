package sample.simple.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;

/**
 * Created by paragsanghavi on 2/3/15.
 *
 * Parse csv files with a specified separator
 * Select columns and specify label column
 * Set a model (GBM) with certain parameters
 * Train the model
 * Test the model on another data set
 * Calculate AUC score.
 * Dump model java file
 */
@Service
public class h2oService {

    static final String H2O_HOST_URL = "http://localhost:54321";
    static final String H2O_IMPORT_URL = "/2/ImportFiles2.json?path=" ;
    static final String H2O_PARSE_URL = "/2/Parse2.json?source_key="; //http://localhost:54321/2/Parse2.query?source_key=nfs://tmp/etsy_images/image_deep_features_csv
    static final String H2O_PROGRESS_URL = "/2/Progress2.json?"; //job_key=%240301ac10022e32d4ffffffff%24_9c2f4bf32b3bd2471dec44fc936d4363&destination_key=image_deep_features_csv.hex
    static final String H2O_GBM_MODEL_URL = "/2/GBM.json?";
    static final String H2O_GBM_MODEL_STATUS_URL = "/2/GBMProgressPage.json?";
    static final String H2O_GBM_MODEL_PREDICT_URL = "/2/Predict.json?";
    static final String H2O_GBM_MODEL_PREDICT_STATUS_URL = "/2/Inspect2.json?";
    static final String H2O_GBM_MODEL_AUC_URL = "/2/AUC.json?";
    static final String H2O_GBM_MODEL_POJO_URL = "/2/GBMModelView.java?_modelKey=" ;

    private final Logger log = LoggerFactory.getLogger(h2oService.class);

    public String ImportCSVFile (String path){

        //example - url http://localhost:54321/2/ImportFiles2.json?path=%2Ftmp%2Fetsy_images%2Fimage_deep_features_csv#
        String key;
        String h2oUrlImportEndPoint = H2O_HOST_URL + H2O_IMPORT_URL + path;
        log.debug("@@@ Calling endpoint {}", h2oUrlImportEndPoint);

        RestTemplate restTemplate = new RestTemplate();
        String result = restTemplate.getForObject(h2oUrlImportEndPoint, String.class);

        //{"Request2":0,"response_info":{"h2o":"paragsanghavi","node":"/172.16.2.45:54321","time":1,
        // "status":"done","redirect_url":null},"prefix":"nfs://tmp/etsy_images/deep_features_csv",
        // "files":["/tmp/etsy_images/deep_features_csv"],"keys":["nfs://tmp/etsy_images/deep_features_csv"],
        // "fails":[],"dels":["nfs://tmp/etsy_images/deep_features_csv"]}

        log.debug("@@@ Response json from h2o {}", result);
        JSONObject jsonobject = new JSONObject(result);
        JSONObject response_info = (JSONObject)jsonobject.get("response_info");
        String status = (String)response_info.get("status");
        log.debug("!!!!!! Import Status  {}", status);

        if (status.equalsIgnoreCase("DONE")) {
            JSONArray jsonarray = (JSONArray) jsonobject.get("keys");
             key = (String) jsonarray.get(0);
            System.out.println("!!!!!! Import key : " +  key);
            log.debug("!!!!!! Import key  {}", key);
            return key;
        }
        else{
            return "error";
        }
    }

    public String ParseCSVFile (String key, String framename ) throws org.json.JSONException{

        //http://localhost:54321/2/Parse2.query?source_key=nfs://tmp/etsy_images/image_deep_features_csv
        String h2oUrlImportEndPoint = H2O_HOST_URL + H2O_PARSE_URL+key ;
        log.debug("@@@ Calling endpoint {}", h2oUrlImportEndPoint);

        MultiValueMap<String, String> parameters = new LinkedMultiValueMap<String, String>();

        /*parser_type:CSV
        separator:44
        header:1
        header_with_hash:0
        single_quotes:0
        header_from_file:
        exclude:
        source_key:nfs://tmp/etsy_images/deep_features_csv
        destination_key:deep_features_csv.hex
        preview:
        delete_on_done:1*/

        parameters.add("parser_type", "CSV");
        parameters.add("separator", "44");
        parameters.add("header", "1");
        parameters.add("singleQuotes", "0");
        parameters.add("source_key", key)  ;
        parameters.add("destination_key", framename);
        parameters.add("delete_on_done", "true");

        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.add("Accept", MediaType.APPLICATION_JSON_VALUE);
        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<MultiValueMap<String, String>>(parameters, headers);
        ResponseEntity<String> response = restTemplate.exchange(h2oUrlImportEndPoint, HttpMethod.GET, request, String.class);
        String responseBody = response.getBody();

        /*

        {"Request2":0,
        "response_info":{"h2o":"paragsanghavi","node":"/172.16.2.46:54321","time":25,"status":"redirect",
        "redirect_url":"/2/Progress2.json?job_key=%240301ac10022e32d4ffffffff%24_96ec9394a616c1e5d4ff8a63f17b428e&destination_key=image_deep_features_csv.hex"},
        "job_key":"$0301ac10022e32d4ffffffff$_96ec9394a616c1e5d4ff8a63f17b428e",
        "destination_key":"image_deep_features_csv.hex"}
         */
        log.debug("@@@ Response json from h2o {}", responseBody);
        JSONObject jsonobject = new JSONObject(responseBody);
        String job_key = (String)jsonobject.get("job_key");
        log.debug("!!!!!! Job name {}", job_key);

        String destination_key= (String)jsonobject.get("destination_key");
        String job_status = JobStatus(job_key, destination_key);

        if(job_status !=null) {
            return destination_key;
        }
        return null;
    }
    public String JobStatus( String job_key, String destination_key )  {
        String status ;
        String h2oUrlJobStatusEndPoint = H2O_HOST_URL + H2O_PROGRESS_URL + "job_key=" + job_key + "&destination_key=" + destination_key;
        System.out.println(h2oUrlJobStatusEndPoint);
        log.debug("@@@ Calling endpoint {}", h2oUrlJobStatusEndPoint);
        RestTemplate restTemplate = new RestTemplate();
        try {
            while (true) {
                String responseBody = restTemplate.getForObject(h2oUrlJobStatusEndPoint, String.class);
                JSONObject jsonobject = new JSONObject(responseBody);
                JSONObject response_info = (JSONObject)jsonobject.get("response_info");
                status = (String)response_info.get("status");
                log.debug("!!!!!! JOB Status  {}", status);
                if (status.equalsIgnoreCase("redirect")) {
                    break;
                }
                Thread.sleep(2000L); //Should use futures here
            }
        }catch(Exception ex){
            log.debug("!!!!!! Error Occured while getting job status  {}", ex);
            return null;
        }
        return status;
    }



    public String BuildGBMModel(String destination_key,String source_key) {

        /*http://localhost:54321/2/GBM.html?destination_key=gbm&source=prostate_csv2.hex&response=CAPSULE&ignored_cols=0&
    classification=1&validation=&n_folds=0&holdout_fraction=.1&keep_cross_validation_splits=0&ntrees=50&max_depth=5&min_rows=10&nbins=20&score_each_iteration=0&importance=1&balance_classes=0
    &class_sampling_factors=&max_after_balance_size=Infinity&checkpoint=&overwrite_checkpoint=1&family=AUTO&learn_rate=0.1&grid_parallelism=1&seed=-1&group_split=1
     */

        String h2oUrlGBMEndPoint =  H2O_HOST_URL + H2O_GBM_MODEL_URL +
                "destination_key={destination_key}&source={source}&response={response}&ignored_cols={ignored_cols}" +
                "&classification={classification}&validation={validation}" +
                "&ntrees={ntrees}&max_depth={max_depth}&min_rows={min_rows}&nbins={nbins}&score_each_iteration={score_each_iteration}" +
                "&importance={importance}&balance_classes={balance_classes}" +
                "&class_sampling_factors={class_sampling_factors}&max_after_balance_size={max_after_balance_size}&checkpoint={checkpoint}" +
                "&overwrite_checkpoint={overwrite_checkpoint}&family={family}&learn_rate={learn_rate}&grid_parallelism={grid_parallelism}&seed={seed}&group_split={group_split}";


        System.out.println("@@@ h2oUrlGBMEndPoint : " + h2oUrlGBMEndPoint);

        final HashMap<String, String> parameters = new HashMap<String, String>();

        //MultiValueMap<String, String> parameters = new LinkedMultiValueMap<String, String>();

        parameters.put("destination_key", destination_key);
        parameters.put("source", source_key);
        parameters.put("response", "CAPSULE");
        parameters.put("ignored_cols", "0");
        parameters.put("classification", "1"); // this means regression
        parameters.put("validation", "");
        parameters.put("n_folds", "10");
        //parameters.put("holdout_fraction", "0.1");
        parameters.put("keep_cross_validation_splits", "0");
        parameters.put("ntrees", "50");
        parameters.put("max_depth", "5");
        parameters.put("min_rows", "10");
        parameters.put("nbins", "20");
        parameters.put("score_each_iteration", "0");
        parameters.put("importance", "1");
        parameters.put("balance_classes", "0");
        parameters.put("class_sampling_factors","");
        parameters.put("max_after_balance_size","Infinity");
        parameters.put("checkpoint","");
        parameters.put("overwrite_checkpoint","");
        parameters.put("family", "AUTO");
        parameters.put("learn_rate", "0.1");
        parameters.put("grid_parallelism", "1");
        parameters.put("seed", "-1");
        parameters.put("group_split", "1");

        try {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.add("Accept", MediaType.APPLICATION_JSON_VALUE);
            //HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<MultiValueMap<String, String>>(parameters, headers);
            //ResponseEntity<String> response = restTemplate.exchange(h2oUrlGBMEndPoint, HttpMethod.GET, request, String.class);

            ResponseEntity<String> responseEntity = restTemplate.getForEntity(h2oUrlGBMEndPoint, String.class, parameters);
            String responseBody = responseEntity.getBody();

            JSONObject jsonobject = new JSONObject(responseBody);
            String job_key = (String)jsonobject.get("job_key");
            String ret_destination_key = (String)jsonobject.get("destination_key");
            System.out.println("!!!!!! GBM Job Key  : " + job_key);
            System.out.println("!!!!!! GBM Destination Key  : " + ret_destination_key);

            String gbm_status = GBMJobStatus(job_key,ret_destination_key);
            if(gbm_status!=null){
                System.out.println("gbm_status : " + gbm_status);
            }
            return ret_destination_key;

        } catch (HttpClientErrorException e) {
            log.debug("Error occured in building model {}", e.getResponseBodyAsString());
            log.debug("Root cause in GBM {}", e.getRootCause().getMessage());
            e.printStackTrace();
            return null;
        } catch (Exception ex) {
            log.debug("!!!!!Error occured in deep learning {}", ex.getMessage());
            ex.printStackTrace();
            return null;
        }

    }

    public String GBMJobStatus( String job_key, String destination_key )  {

        String status ;
        String h2oUrlJobStatusEndPoint = H2O_HOST_URL + H2O_GBM_MODEL_STATUS_URL + "job_key=" + job_key + "&destination_key=" + destination_key;
        System.out.println(h2oUrlJobStatusEndPoint);
        log.debug("@@@ Calling endpoint {}", h2oUrlJobStatusEndPoint);
        RestTemplate restTemplate = new RestTemplate();
        try {
            while (true) {
                String responseBody = restTemplate.getForObject(h2oUrlJobStatusEndPoint, String.class);
                JSONObject jsonobject = new JSONObject(responseBody);
                JSONObject response_info = (JSONObject)jsonobject.get("response_info");
                status = (String)response_info.get("status");
                log.debug("!!!!!! JOB Status  {}", status);
                if (status.equalsIgnoreCase("redirect")) {
                    break;
                }
                Thread.sleep(2000L); //Should use futures here
            }
        }catch(Exception ex){
            log.debug("!!!!!! Error Occured while getting job status  {}", ex);
            return null;
        }
        return status;
    }

    public String PredictGBM( String model, String new_data_key )  {

        //http://localhost:54321/2/Predict.html?model=gbmmodelDestinationKey&data=prostate_csv.hex&prediction=predict_1
        String status ;
        String inspect_status;
        String prediction_name = "Predict_GBM";
        String h2oUrlPredictEndPoint = H2O_HOST_URL + H2O_GBM_MODEL_PREDICT_URL + "model=" + model + "&data=" + new_data_key + "&prediction=" + prediction_name;
        System.out.println(h2oUrlPredictEndPoint);
        log.debug("@@@ Calling endpoint {}", h2oUrlPredictEndPoint);
        try {
            RestTemplate restTemplate = new RestTemplate();
            String responseBody = restTemplate.getForObject(h2oUrlPredictEndPoint, String.class);
            JSONObject jsonobject = new JSONObject(responseBody);
            JSONObject response_info = (JSONObject)jsonobject.get("response_info");
            status = (String)response_info.get("status");
            System.out.println("PREDICT GBM status: " + status);
            inspect_status = PredictGBMStatus(prediction_name);

        }catch(Exception ex){
            log.debug("!!!!!! Error Occurred while getting job status  {}", ex);
            ex.printStackTrace();
            return null;
        }
        return inspect_status;
    }


    public String PredictGBMStatus( String src_key )  {

        //http://localhost:54321/2/Inspect2.json?src_key=1111
        String status ;
        String h2oUrlJobStatusEndPoint = H2O_HOST_URL + H2O_GBM_MODEL_PREDICT_STATUS_URL + "src_key=" + src_key;
        System.out.println(h2oUrlJobStatusEndPoint);
        System.out.println("@@@ Calling endpoint {} : " +  h2oUrlJobStatusEndPoint);
        log.debug("@@@ Calling endpoint {}", h2oUrlJobStatusEndPoint);
        RestTemplate restTemplate = new RestTemplate();
        try {
            while (true) {
                String responseBody = restTemplate.getForObject(h2oUrlJobStatusEndPoint, String.class);
                JSONObject jsonobject = new JSONObject(responseBody);
                JSONObject response_info = (JSONObject)jsonobject.get("response_info");
                status = (String)response_info.get("status");
                log.debug("!!!!!! JOB Status  {}", status);
                if (status.equalsIgnoreCase("done")) {
                    break;
                }
                Thread.sleep(2000L); //Should use futures here
            }
        }catch(Exception ex){
            log.debug("!!!!!! Error Occured while getting job status  {}", ex);
            return null;
        }
        return status;
    }

    public Double CalculateAUC( String actual, String vactual, String vpredict  )  {

        //http://localhost:54321/2/AUC.json?actual=prostate_csv.hex&vactual=CAPSULE&predict=predict_1&vpredict=1&thresholds=&threshold_criterion=maximum_F1
        Double AUC;
        String prediction_name = "Predict_GBM";
        String h2oUrlCalculateAUCEndPoint = H2O_HOST_URL + H2O_GBM_MODEL_AUC_URL + "actual=" + actual + "&vactual=" + vactual + "&predict=" + prediction_name + "&vpredict=" + vpredict +"&threshold_criterion=maximum_F1";
        System.out.println(h2oUrlCalculateAUCEndPoint);
        log.debug("@@@ Calling endpoint {}", h2oUrlCalculateAUCEndPoint);
        try {
            RestTemplate restTemplate = new RestTemplate();
            String responseBody = restTemplate.getForObject(h2oUrlCalculateAUCEndPoint, String.class);
            JSONObject jsonobject = new JSONObject(responseBody);
            JSONObject aucdata = (JSONObject)jsonobject.get("aucdata");
            AUC = (Double)aucdata.get("AUC");
            //status = (String).get("status");
            System.out.println("AUC: " + AUC);


        }catch(Exception ex){
            log.debug("!!!!!! Error Occurred while getting job status  {}", ex);
            ex.printStackTrace();
            return null;
        }
        return AUC;
    }

    public String DownloadPOJO( String model_key  )  {

        //http://localhost:54321/2/GBMModelView.java?_modelKey=gbmmodelDestinationKey
        File java_pojo;
        String h2oUrlDownloadPOJOEndPoint = H2O_HOST_URL + H2O_GBM_MODEL_POJO_URL+ model_key;
        System.out.println(h2oUrlDownloadPOJOEndPoint);
        log.debug("@@@ Calling endpoint {}", h2oUrlDownloadPOJOEndPoint);
        try {
            RestTemplate restTemplate = new RestTemplate();
            String responseBody = restTemplate.getForObject(h2oUrlDownloadPOJOEndPoint, String.class);

            //create java POJO file
            String pojofilename = model_key + ".java";
            java_pojo = new File(pojofilename);
            System.out.println("POJO File name" + java_pojo.getAbsolutePath());
            PrintWriter out = new PrintWriter(java_pojo.getAbsolutePath());
            out.write(responseBody);
            out.close();
        }catch(Exception ex){
            log.debug("!!!!!! Error Occurred while getting job status  {}", ex);
            ex.printStackTrace();
            return null;
        }
        return  java_pojo.getAbsolutePath();
    }
}
