import pyodbc
import pandas as pd
import os

class pyvertica:
    """ Easy access to Vertica through python odbc connection. 
        All results are returned as Pandas dataframes
    """
    
    def __init__(self, dsn):
        self.dsn = dsn
    

    def query(self, q, verbose = True):
        """ Runs query on vertica, returns results as dataframe if any."""
        with pyodbc.connect(self.dsn) as conn:
            try:
                return pd.read_sql(q, conn)
            except TypeError, e:
                # If this is a NoneType object is not iterable error,
                # it probably just means the query did not produce output;
                # it was still executed.
                if e.message == "'NoneType' object is not iterable":
                    if verbose: print 'Query ran successfully, but had no output.'
                else:
                    raise

    def get_table(self, table_name):
        """ Select * from table ... """
        return self.query("SELECT * FROM {};".format(table_name))
    
    def get_columns(self, table_name):
        """ List all columns from table specified."""
        print list(self.query("SELECT * FROM {} LIMIT 0;".format(table_name)))
        
    def delete_table(self, table_name):
        """ Drops table from vertica. """ 
        self.query("DROP TABLE IF EXISTS {};".format(table_name))

    def list_tables(self):
        """ Lists all tables in Vertica."""
        return self.query("""SELECT table_schema, table_name, is_temp_table, is_system_table, create_time 
                        FROM v_catalog.tables 
                        ORDER BY table_schema, table_name;""")

    def search_tables(self, search_str):
        """ Searches Vertica for tables. Specify full name with schema and use % for wild cards"""
        schema, table_name = search_str.split('.')
        return self.query("""SELECT table_schema, table_name, is_temp_table, is_system_table, create_time 
                        FROM v_catalog.tables 
                            WHERE table_schema ILIKE '{0}' 
                            AND table_name ILIKE '{1}' 
                        ORDER BY table_schema, table_name;""".format(schema, table_name) )

    def jobs_running(self):
        """ Shows all jobs currently running. """
        return self.query("""select session_id, login_timestamp, current_statement, client_type, client_os, jvm_memory_kb 
                             from sessions where current_statement <> '';""")

    def kill_job(self, job_id):
        """ Kills job with job_id specified. """
        return self.query("select close_session('"+job_id+"');")

    def write_table(self, df, table_name, append = False, with_index = False, save_name = None, verbose = False, delimiter = '|', remove_delimiter = True):
        """
        Writes a Pandas dataframe to Vertica using the pyodbc connection. 
        Table schema is inferred from dataframe dtypes.
        
        If Table fails or is rejected for any reason error logs are saved as import_exceptions.log
        and raw_errors.log in the current working dir. 
        
        --Inputs
            * df: Pandas DataFrame to write to Vertica 
            * table_name: Name of table to save to Vertica including schema
            * append: (Boolean) Append output to existing table if table already exists (default = False)
            * with_index: (Boolean) Save index as column (default = False)
            * save_name: Name to save backup csv file. By default no backup csv is saved. 
            * verbose: (Boolean)
            * delimiter: Delimiter used to save file (Default = '|'). 
            * remove_delimiter: (Boolean) If true remove all instances of delimiter before saving csv.
            
       --Outputs: 
            * None
        """
        
        save_csv = True
        if save_name is None:
            save_csv = False
            save_name = os.getcwd()+'/'
            save_name = save_name + 'temp.csv'
        
        #remove delimiter from dataframe 
        if remove_delimiter:
            escape_chars = [".", "\\", "+", "*", ".", "?", "[", "^", "]",
                            "$", "(",")", "{", "}", "=", "!", "<", ">", "|", ":", "-"]
            regex_exp = '\\' + delimiter + '+' if delimiter in escape_chars else delimiter + '+' 
            df = df.replace(regex_exp, '', regex = True)
         
        #save temp csv
        df.to_csv(save_name, index = with_index, sep = delimiter)

        dtype_dict = {'float64':'FLOAT', 'float32': 'FLOAT',
                      'int64':'INT', 'int32':'INT',
                      'O':'VARCHAR', 'object':'VARCHAR', 
                      'datetime64[ns]':'DATE', 'datetime32[ns]':'DATE',
                      'bool':'BOOLEAN'}

        def convert_dtype(x):
            if x in dtype_dict:
                return dtype_dict[x]
            else:
                return 'VARCHAR'

        var_list = str()
        if with_index: var_list += 'index VARCHAR, '
        for col in list(df):
            var_list += col + ' ' + convert_dtype(str(df[col].dtype)) 
            if col != list(df)[-1]:
                var_list += ', '

        q1 = """
                DROP TABLE IF EXISTS {0}; 
             """.format(table_name)

        if verbose and not append : print q1

        q2 = """
                CREATE TABLE IF NOT EXISTS {0} ({1});
                COPY {0} FROM LOCAL '{2}'
                WITH
                    DELIMITER '{3}'
                    SKIP 1
                    REJECTMAX 1
                    EXCEPTIONS 'import_exceptions.log'
                    REJECTED DATA 'raw_errors.log'
            """.format(table_name, var_list, save_name, delimiter)

        if verbose: print q2

        if not append: self.query(q1, verbose = verbose) 
        self.query(q2, verbose = verbose)

        print 'Write to Vertica successful'

        if not save_csv: os.remove(save_name)
        os.remove(os.getcwd()+'/import_exceptions.log')
        os.remove(os.getcwd()+'/raw_errors.log')

        return 
