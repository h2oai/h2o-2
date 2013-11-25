##
# Depends on smalldata.csv
# Creates a json, that is a list of datasets
# Datasets have PATHS, NAMES, NUMCOLS, NUMROWS, TYPES, RANGE, IGNORED, TARGET
##
import os, sys, time, csv, string
sys.path.extend(['.','..'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_jobs
from pprint import pprint

def toJson(DATANAME, PATHS, NAMES, NUMCOLS, NUMROWS, TYPES, RANGES, IGNORED = "NA", TARGET = "NA"):
    #{0} is DATANAME
    #{1} is PATHS
    #{2} is NAMES
    #{3} is NUMCOLS
    #{4} is NUMROWS
    #{5} is TYPES
    #{6} is RANGES
    #{7} is IGNORED
    #{8} is TARGET 
    
    print RANGES    

    PATHS = '[' + ','.join(PATHS) + ']'
    NAMES = '[' + ','.join(NAMES) + ']'
    TYPES = '[' + ','.join(TYPES) + ']'
    RANGES = '[' + ','.join(RANGES) + ']'
    
    print PATHS
    print NAMES
    print TYPES
    print RANGES

    j_s_o_n = """
    "{0}": {{
        "PATHS":{1},
        "ATTRS": {{
            "NAMES":{2},
            "NUMCOLS":"{3}",
            "NUMROWS":"{4}",
            "TYPES":{5},
            "RANGE":{6},
            "IGNORED":{7},
            "TARGET":{8}
        }}
    }}

    """.format(DATANAME,PATHS,NAMES,NUMCOLS,NUMROWS,TYPES,RANGES,IGNORED,TARGET)
    with open('data.json', 'a') as f:
        f.write(j_s_o_n)

def getSummaries():
    with open('./smalldata.csv', 'rb') as f:
        for line in f:
            PATHS = []
            NAMES = []
            NUMCOLS = 0
            NUMROWS = 0
            TYPES = []
            RANGES = []
            IGNORED = 'NA'
            TARGET = 'NA'

            DATANAME, uploadPath, importPath, importHDFS, fullPath = line.strip("\n").split(',')
            PATHS = [uploadPath, importPath, importHDFS]
            
            bucket = 'smalldata'
            path = '/'.join(importPath.split('/')[2:]).strip('"')
            parseResult = h2i.import_parse(bucket=bucket, path = path, schema='local', doSummary = False)
            summary = h2o_cmd.runSummary(key=parseResult['destination_key'])
            columns = summary['summary']['columns']
            
            NUMCOLS = len(columns)
            NUMROWS = columns[0]['N']
            for col in columns:
                NAMES.append( '\"' + col['name'] + '\"')
                TYPES.append('\"' + col['type'] + '\"')
                tup = '(' + '"' + str(min(col['min'])) + '"' + ',' + '"' + str(max(col['max'])) + '"' + ')' if col['type'] == 'number' else '("NA", "NA")'
                RANGES += [tup] 
            
            toJson(DATANAME, PATHS, NAMES, NUMCOLS, NUMROWS, TYPES, RANGES, IGNORED = "NA", TARGET = "NA")
       

if __name__ == '__main__':
    h2o.parse_our_args()
    h2o.build_cloud(1,java_heap_GB=10)
    
    getSummaries()
    h2o.tear_down_cloud()

