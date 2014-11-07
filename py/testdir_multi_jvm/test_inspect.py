import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

def crange(start, end):
    for c in xrange(ord(start), ord(end)):
        yield chr(c)

# Dummy wc -l
def wcl(csvPathname):
        lines = 0
        f = open(csvPathname)
        for line in f:
            lines += 1
        f.close()
        return lines

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(node_count=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_inspect_poker1000(self):
        csvPathname = "poker/poker1000"
        res = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        ary  = h2o_cmd.runInspect(key=res['destination_key'])
        # count lines in input file - there is no header for poker 1000
        fullPathname = h2i.find_folder_and_filename('smalldata', csvPathname, returnFullPath=True)
        rows = wcl(fullPathname)
        self.assertEqual(rows, ary['numRows'])
        self.assertEqual(11, ary['numCols'])

    def test_B_inspect_column_names_multi_space_sep(self):
        self.inspect_columns("smalldata", "test/test_26cols_multi_space_sep.csv")

    def test_C_inspect_column_names_single_space_sep(self):
        self.inspect_columns("smalldata", "test/test_26cols_single_space_sep.csv")

    def test_D_inspect_column_names_comma_sep(self):
        self.inspect_columns("smalldata", "test/test_26cols_comma_sep.csv")

    def test_E_inspect_column_names_comma_sep(self):
        self.inspect_columns("smalldata", "test/test_26cols_single_space_sep_2.csv")

    # FIX! are we never going to support this?
    def nottest_F_more_than_65535_unique_names_in_column(self):
        self.inspect_columns("smalldata", "test/test_more_than_65535_unique_names.csv", 
            rows=66001, cols=3, columnNames=['X','Y','Z'],columnTypes=['float','int','float'])

    # FIX! we set a lower limit on the # of enums now. So will have to adjust the file
    # and this test, to get a correct enum case
    # notest means it won't be run
    def notest_FF_less_than_65535_unique_names_in_column(self):
        self.inspect_columns("smalldata", "test/test_less_than_65535_unique_names.csv", 
            rows=65533, cols=3, columnNames=['X','Y','Z'],columnTypes=['enum','int','float'])

    def test_G_all_raw_top10rows(self):
        self.inspect_columns("smalldata", "test/test_all_raw_top10rows.csv", rows=12, cols=89, 
            columnNames=['Randm','Month','applicationid','zip','sex','Day_Week','TimeofDay','WebApp','entereddate','age','AnsweredSurvey','Srvy_Plan2DD','Srvy_bythngs_online','Has_bnk_AC','PlasticTypeID','FeePlanID','clientkey','PlasticType','PlanType','Activated','OnDD','Verified','RegisteredOnline','Channel','Appid','Population','HouseholdsPerZipCode','WhitePopulation','BlackPopulation','HispanicPopulation','AsianPopulation','HawaiianPopulation','IndianPopulation','OtherPopulation','MalePopulation','FemalePopulation','PersonsPerHousehold','AverageHouseValue','IncomePerHousehold','MedianAge','MedianAgeMale','MedianAgeFemale','Elevation','CityType','TimeZone','DayLightSaving','MSA','PMSA','CSA','CBSA','CBSA_Div','NumberOfBusinesses','NumberOfEmployees','BusinessFirstQuarterPayroll','BusinessAnnualPayroll','GrowthRank','GrowthHousingUnits2003','GrowthHousingUnits2004','GrowthIncreaseNumber','GrowthIncreasePercentage','CBSAPop2003','CBSADivPop2003','DeliveryResidential','DeliveryBusiness','DeliveryTotal','PopulationEstimate','LandArea','WaterArea','id','Experian_pass','Innovis_pass','TU_pass','Choicepoint_pass','LN_pass','Experian_Cx','Innovis_Cx','TU_Cx','Choicepoint_Cx','LN_Cx','checkpointscore','levelonedecisioncode','grade','white_percent','black_percent','hispanic_percent','male_percent','female_percent','region','division' ])

    def test_H_enum_domain_size(self):
        cinsp = self.inspect_columns("smalldata", "test/test_enum_domain_size.csv", 
            rows=4, cols=3, columnNames=['A1', 'A2', 'A3'], columnTypes=['Int','Int','Enum'])
        self.assertEqual(4, cinsp['cols'][2]['cardinality'])

    # Shared test implementation for smalldata/test/test_26cols_*.csv
    def inspect_columns(self, bucket, csvPathname, rows=1, cols=26, columnNames=crange('A', 'Z'), columnTypes=None):
        res = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put')
        ary  = h2o_cmd.runInspect(key=res['destination_key'])

        self.assertEqual(rows, ary['numRows'])
        self.assertEqual(cols, ary['numCols'])

        # check column names
        if not columnNames is None:
            for (col, expName) in zip(ary['cols'], columnNames):
                self.assertEqual(expName, col['name'])

        # check column types
        if not columnTypes is None:
            for (col, expType) in zip(ary['cols'], columnTypes):
                self.assertEqual(expType, col['type'])

        return ary

if __name__ == '__main__':
    h2o.unit_main()
