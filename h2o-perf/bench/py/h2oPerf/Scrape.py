from Table import *
import json
import os
import re
import subprocess
import time
import MySQLdb

class Scraper:
    """
    Objects of this class scrape the R stdouterr for
    relevant information that needs to percolate back
    to the database.

    Because of the different phases (parse, model, predict), 
    there is a switch that redirects control to a subclass
    scraper of the appropriate type.

    Each phase will insert a row into the test_run_phase_result
    table in the db, and possibly percolate pieces of the test_run
    table back to the PerfRunner object. 

    Some subclasses of the Scraper will insert data into the math results tables.
    """
    def __init__(self, perfdb, phase, test_dir, test_short_dir, output_dir, output_file_name):
        self.perfdb = perfdb
        self.phase = phase
        self.test_dir = test_dir
        self.test_short_dir = test_short_dir
        self.output_dir = output_dir
        self.output_file_name = output_file_name
        self.did_time_pass = 0 
        self.did_correct_pass = 0 
        self.contaminated = 0 
        self.contamination_message = ""

    def scrape(self):
        """
        Switches out to the phase scraper for scraping R output.
        The subclass object is then invoked and an object with
        table information is percolated back to the caller.
        """
        phase_scraper = self.__switch__()
        res = phase_scraper.invoke()
        self.did_time_pass = phase_scraper.did_time_pass
        self.did_correct_pass = phase_scraper.did_correct_pass
        self.contaminated = phase_scraper.contaminated
        self.contamination_message = phase_scraper.contamination_message
        return res

    def __switch__(self):
        """
        Switch to scraper for the appropriate phase.
        """
        return {
            'parse': ParseScraper(self), 
            'model': ModelScraper(self), 
            'predict': PredictScraper(self), 
        }[self.phase]

class ParseScraper(Scraper):
    """
    An object that performs the scraping for the Parse phase.
    Relevant tables and their fields:
      >test_run:
          [dataset_name, dataset_source, train_dataset_url, test_dataset_url] 
      >test_run_phase_result:
          [phase_name, start/end_epoch_ms, stdouterr, passed, correctness_passed, 
           timing_passed, contaminated, contamination_message]
    """
    def __init__(self, object):
        self.perfdb = object.perfdb
        self.phase = object.phase
        self.test_dir = object.test_dir
        self.test_short_dir = object.test_short_dir
        self.output_dir = object.output_dir
        self.output_file_name = object.output_file_name
        self.contamination = os.path.join(self.output_dir, "contamination_message")
        self.contaminated = 1 if os.path.exists(self.contamination) else 0
        self.contamination_message = "No contamination."
        if self.contaminated:
            with open(self.contamination, "r") as f:
                self.contamination_message = MySQLdb.escape_string(f.read().replace('\n', ''))
        self.did_correct_pass = 0
        self.did_time_pass = 0
        
        self.test_run = {
                         'dataset_source': '', 
                         'train_dataset_url': '',
                         'test_dataset_url': '',
                        }

    def invoke(self):
        """
        Scrapes the stdouterr from the R phase. Inserts into results tables.
        The phase result is handled in the __init__ of this object.

        The work be done here is on the self.test_run dictionary
        @return: test_run dictionary
        """
        self.insert_phase_result()
        self.test_run.update(self.__scrape_parse_result__())
        return self.test_run

    def insert_phase_result(self):
        trpr = TableRow("test_run_phase_result", self.perfdb)
        with open(self.output_file_name, "r") as f:
            trpr.row['stdouterr'] = MySQLdb.escape_string(f.read().replace('\n', ''))
        trpr.row['contaminated'] = self.contaminated
        trpr.row['contamination_message'] = self.contamination_message
        trpr.row.update(self.__scrape_phase_result__())
        trpr.update()

    def __scrape_phase_result__(self):
        phase_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    phase_r = json.loads(line)
                    flag = False
                    break
                if "PHASE RESULT" in line and "print" not in line:
                    flag = True
        self.did_correct_pass = int(phase_r['phase_result']['correctness_passed'])
        self.did_time_pass = int(phase_r['phase_result']['timing_passed'])
        return phase_r['phase_result']
        
    def __scrape_parse_result__(self):
        parse_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    parse_r = json.loads(line)
                    flag = False
                    break
                if "PARSE RESULT" in line and "print" not in line:
                    flag = True
        return parse_r['parse_result']

class ModelScraper(Scraper):
    """
    An object that performs the scraping for the Model phase.
    Relevant tables and their fields: 
      >test_run_clustering_result: 
          [k, withinss]
      >test_run_model_result: 
          [model_json]
      >test_run_phase_result: 
          [phase_name, start/end_epoch_ms, stdouterr, passed, correctness_passed, 
           timing_passed, contaminated, contamination_message]
    """
    def __init__(self, object):
        self.perfdb = object.perfdb
        self.phase = object.phase
        self.test_dir = object.test_dir
        self.test_short_dir = object.test_short_dir
        self.output_dir = object.output_dir
        self.output_file_name = object.output_file_name
        self.contamination = os.path.join(self.output_dir, "contamination_message")
        self.contaminated = 1 if os.path.exists(self.contamination) else 0
        self.contamination_message = "No contamination."
        if self.contaminated:
            with open(self.contamination, "r") as f:
                self.contamination_message = MySQLdb.escape_string(f.read().replace('\n', ''))
        self.did_correct_pass = 0
        self.did_time_pass = 0

        self.test_run_model_result = TableRow("test_run_model_result", self.perfdb)

    def invoke(self):
        """
        Scrapes the stdouterr from the R phase. Inserts into results tables.
        Additionally handles the KMeans clustering results table.
        @return: None
        """
        self.insert_phase_result()
        kmeans_result = self.__scrape_kmeans_result__()
        if kmeans_result:
            self.test_run_clustering_result = TableRow("test_run_clustering_result", self.perfdb)
            self.test_run_clustering_result.row.update(kmeans_result)
            self.test_run_clustering_result.update()

        comp_result = self.__scrape_comparison_result__()
        if comp_result != "":
            self.test_run_binomial_comparison_result = TableRow("test_run_binomial_comparison", self.perfdb)
            self.test_run_binomial_comparison_result.row.update(comp_result['comparison_result'])
            self.test_run_binomial_comparison_result.update()
        else:
            self.test_run_model_result.row['model_json'] = \
                          MySQLdb.escape_string(str(self.__scrape_model_result__()))
            self.test_run_model_result.update()
        return None

    def insert_phase_result(self):
        trpr = TableRow("test_run_phase_result", self.perfdb)
        with open(self.output_file_name, "r") as f:
            trpr.row['stdouterr'] = MySQLdb.escape_string(f.read().replace('\n', ''))
        trpr.row['contaminated'] = self.contaminated
        trpr.row['contamination_message'] = self.contamination_message
        trpr.row.update(self.__scrape_phase_result__())
        trpr.update()

    def __scrape_comparison_result__(self):
        comparison_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    comparison_r = json.loads(line)
                    flag = False
                    break
                if "COMPARISON" in line and "print" not in line:
                    flag = True
        return comparison_r

    def __scrape_phase_result__(self):
        phase_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    phase_r = json.loads(line)
                    flag = False
                    break
                if "PHASE RESULT" in line and "print" not in line:
                    flag = True
        self.did_correct_pass = int(phase_r['phase_result']['correctness_passed'])
        self.did_time_pass = int(phase_r['phase_result']['timing_passed'])
        return phase_r['phase_result']

    def __scrape_kmeans_result__(self):
        kmeans_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    kmeans_r = json.loads(line)
                    flag = False
                    break
                if "KMEANS RESULT" in line and "print" not in line:
                    flag = True
        return None if kmeans_r["kmeans_result"]["k"] == "None" else kmeans_r["kmeans_result"]

    def __scrape_model_result__(self):
        model_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    model_r = json.loads(line)
                    flag = False
                    break
                if "MODEL RESULT" in line and "print" not in line:
                    flag = True
        return model_r["model_result"]["model_json"]


class PredictScraper(Scraper):
    """
    An object that performs the scraping for the Predict phase.
    This object is not awlays used, e.g. in the case of KMeans and PCA,
    there is no prediction phase, but the results still need to be 
    verified.

    Relevant tables and their fields:
      >test_run_binomial_classification_result: 
          [auc, precision, recall, error_rate, minoriy_error_rate]
      >test_run_cm_result: 
          [levels_json, cm_json, representation]
      >test_run_multinomial_classification_result: 
          [level, level_actual_count, level_predicted_correctly_count, level_error_rate]
      >test_run_phase_result: 
          [phase_name, start/end_epoch_ms, stdouterr, passed, correctness_passed, 
           timing_passed, contaminated, contamination_message]
      >test_run_regression_result:
          [aic, null_deviance, residual_deviance]
    """
    def __init__(self, object):
        self.perfdb = object.perfdb
        self.phase = object.phase
        self.test_dir = object.test_dir
        self.test_short_dir = object.test_short_dir
        self.output_dir = object.output_dir
        self.output_file_name = object.output_file_name
        self.contamination = os.path.join(self.output_dir, "contamination_message")
        self.contaminated = 1 if os.path.exists(self.contamination) else 0
        self.contamination_message = "No contamination."
        if self.contaminated:
            with open(self.contamination, "r") as f:
                self.contamination_message = MySQLdb.escape_string(f.read().replace('\n', ''))
        self.did_correct_pass = 0 
        self.did_time_pass = 0

        self.test_run_binomial_classification_result = ""
        self.test_run_cm_result = "" 
        self.test_run_phase_result = "" 
        self.test_run_regression_result = "" 
        self.test_run_binomial_comparison_result = ""
        self.test_run_multinomial_classification_result = "" 

    def invoke(self):
        """
        Scrapes the stdouterr from the R phase. 
        This invoke method will pass off control to the appropriate result scraper
        using the __switch__ override.
  
        Some preliminary scraping will be done here to obtain the correct result type.
        @return: None
        """
        self.insert_phase_result()
        predict_type = ""
        # with open(self.output_file_name, "r") as f:
        #     flag = False
        #     for line in f:
        #         if flag:
        #             print "---------------------------------"
        #             print line
        #             print "---------------------------------"
        #             predict_type = self.__get_predict_type__(line.strip())[0]
        #             flag = False
        #             break
        #         if "PREDICT TYPE" in line and "print" not in line:
        #             flag = True
        # self.result_type = predict_type
        # print "GOT RESULT TYPE:     " + predict_type
        # self.__switch__()
        return None

    def insert_phase_result(self):
        trpr = TableRow("test_run_phase_result", self.perfdb)
        with open(self.output_file_name, "r") as f:
            trpr.row['stdouterr'] = MySQLdb.escape_string(f.read().replace('\n', ''))
        trpr.row['contaminated'] = self.contaminated
        trpr.row['contamination_message'] = self.contamination_message
        trpr.row.update(self.__scrape_phase_result__())
        trpr.update()

    def __scrape_phase_result__(self):
        phase_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    phase_r = json.loads(line)
                    flag = False
                    break
                if "PHASE RESULT" in line and "print" not in line:
                    flag = True
        self.did_correct_pass = int(phase_r['phase_result']['correctness_passed'])
        self.did_time_pass = int(phase_r['phase_result']['timing_passed'])
        return phase_r['phase_result']
        
    def __get_predict_type__(self, type_candidate):
        """ 
        Returns the type: 'parse', 'model', 'predict'
        """
        print "TYPE CANDIDATE:   " + type_candidate
        types = ['binomial', 'regression', 'multinomial', 'cm']
        rf = type_candidate.lower()
        print "RETURNING TYPE:    " + str( [t for t in types if t in rf])
        return [t for t in types if t in rf] 

    def __switch__(self):
        """
        Overrides the __switch__ method of the parent class.

        This switch method handles the different types of math
        results: regression, multionomial classification, CM result,
                 binomial classification

        Multinomial classification is the only case where there will
        be multiple rows inserted, all other results constitute a single row
        in their respective tables.

        One important note is that the scrapers in this case handle the
        database insertions.
        """
        print "SWITCHING TO     " + self.result_type
        obj =  {'regression' : self.__scrape_regression_result__,
                'cm'         : self.__scrape_cm_result__,
                'multinomial': self.__scrape_multinomial_result__,
                'binomial'   : self.__scrape_binomial_result__,
                'comparison' : self.__scrape_comparison_result__,
                }.get(self.result_type, "bad key")

        if self.result_type in ['multinomial', 'binomial']:
            self.__scrape_cm_result__()
        return obj()

    def __scrape_regression_result__(self):
        regression_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    regression_r = json.loads(line)
                    flag = False
                    break
                if "REGRESSION" in line and "print" not in line:
                    flag = True
        #do the insert

    def __scrape_cm_result__(self):
        cm_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    cm_r = json.loads(line)
                    flag = False
                    break
                if "CM RESULTS JSON" in line and "print" not in line:
                    flag = True
        self.test_run_cm_result = TableRow("test_run_cm_result", self.perfdb)
        self.test_run_cm_result.row.update(cm_r["cm_json"])
        self.test_run_cm_result.update()

    def __scrape_binomial_result__(self):
        binomial_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    binomial_r = json.loads(line)
                    flag = False
                    break
                if "BINOMIAL" in line and "print" not in line:
                    flag = True
        self.test_run_binomial_classification_result = TableRow("test_run_binomial_classification_result", self.perfdb)
        self.test_run_binomial_classification_result.row.update(binomial_r['binomial_result'])
        self.test_run_binomial_classification_result.update()
        return None

    def __scrape_multinomial_result__(self):
        multinomial_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    multinomial_r = json.loads(line)
                    flag = False
                    break
                if "MULTINOMIAL" in line and "print" not in line:
                    flag = True
        for level in multinomial_r["multinomial_result"]:
            self.test_run_multinomial_classification_result = TableRow("test_run_multinomial_classification_result", self.perfdb)
            self.test_run_multinomial_classification_result.row.update(level)
            self.test_run_multinomial_classification_result.update()

    def __scrape_comparison_result__(self):
        comparison_r = ""
        with open(self.output_file_name, "r") as f:
            flag = False
            for line in f:
                if flag:
                    comparison_r = json.loads(line)
                    flag = False
                    break
                if "COMPARISON" in line and "print" not in line:
                    flag = True
        self.test_run_binomial_comparison_result = TableRow("test_run_binomial_comparison", self.perfdb)
        self.test_run_binomial_comparison_result.row.update(comparison_r['comparison_result'])
        self.test_run_binomial_comparison_result.update()

