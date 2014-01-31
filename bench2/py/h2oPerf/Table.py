import MySQLdb

class PerfDB:
    """
    A class that represents a MySQL connection to the PerfDB
    database. Connection information is contained in a config file.
    
    All communication with the db is handled with this object.
    """
    def __init__(self):
        self.db = MySQLdb.connect(host = "localhost",#PerfDBHost,
                                 user = "spencer",#PerfDBUser,
                                 passwd = "spencer",#PerfDBPass,
                                 db = "PerfDB",
                                 port = 3306)
        self.cursor = self.db.cursor()
        self.prev_test_run_id = self.get_table_pk("test_run")
        self.this_test_run_id = self.prev_test_run_id + 1
        self.table_names = self.__get_table_names__()

    def new_table_entries(self):
        """
        Create a new, empty row (to be filled in) for each table.
        Return a dict of TableRow objects with keys being the table name.
        """
        table_rows = [TableRow(table_name) for table_name in self.table_names]
        return dict((name, table) for (name, table) in zip(self.table_names, table_rows))

    def get_table_pk(self, table_name):
        """
        Get the primary key for the table specified.
        """
        primary_key = table_name + "_id"
        last_row_sql = "SELECT * FROM {} ORDER BY {}  DESC LIMIT 1;".format(table_name, primary_key)
        self.cursor.execute(last_row_sql)
        last_row = self.cursor.fetchone()
        cols = self.cursor.description
        if last_row is None:
            last_row = [0 for i in range(len(cols))]
        last_row_assoc = dict((name[0], val) for (name,val) in zip(cols, last_row))
        return last_row_assoc[primary_key]

    def colnames(self, table_name):
        """
        Takes the table name and fetches the column names from the
        corresponding table in PerfDB.
        """
        sql = "SELECT * FROM {} LIMIT 1;".format(table_name)
        self.cursor.execute(sql)
        self.cursor.fetchone()
        table_desc = self.cursor.description
        column_names = [name[0] for name in table_desc]
        return column_names

    def insert(self, table_row):
        """
        Takes a TableRow object and writes to the db.
        """

        sql = "INSERT INTO {} ({}) VALUES ({});".format(table_row.table_name, 
                   ','.join([str(t) for t in table_row.row.keys()]), 
                   ','.join(['"' + str(t) + '"' for t in table_row.row.values()]))
        self.cursor.execute(sql)
        self.db.commit()

    def __get_table_names__(self):
        """
        Private method for getting the table names (all PerfDB objects
        have access to table_names, but don't access this function
        directly.)
        """
        sql = "SHOW TABLES FROM PerfDB;"
        self.cursor.execute(sql)
        return [t[0] for t in self.cursor.fetchall()]

class TableRow:
    """ 
    A class that represents a row of a table in the database.
    A table is a schema with keys, datatypes, and rules.
    The rule for all tables is no null entries.

    The dictionary structure is used to represent a row in the
    table (whose name is passed in).
    """
    def __init__(self, table_name):
        self.perfdb_connection = PerfDB()
        self.table_name = table_name
        self.column_names = self.perfdb_connection.colnames(table_name)
        self.row = dict((el,"") for el in self.column_names)
        if (table_name + "_id") not in self.row.keys():
            self.pk = self.perfdb_connection.this_test_run_id
            self.row['test_run_id'] = self.pk
        else:
            self.pk = self.perfdb_connection.get_table_pk(table_name)
            self.row['test_run_id'] = self.perfdb_connection.this_test_run_id
            self.row[table_name + "_id"] = self.pk + 1

    def update(self):
        """
        A function that will write the row to the database.
        Passes self to the PerfDB object for processing.
        Follows the schema rules (i.e. no nulls)
        """
        if self.__is_complete__():
            self.perfdb_connection.insert(self)
        else:
            self.__test_fail__()

    def __test_fail__(self):
        """
        Failure of attempted row write to db.
        The row is logged along with a message 
        to the python_message table.

        The row is then filled in with defaults
        corresponding to a failed test.
        """
        message = "ERROR: Could not write row to table {}.".format(self.table_name)
        py_table = TableRow("python_message")
        py_table.row['message'] = message
        py_table.row['row'] = str(self.row)
        py_table.update()
        print message

    def __is_complete__(self):
        """
        Check for all values being filled in for the row
        before attempting to write to the db.
        """
        return '' not in self.row.values()

