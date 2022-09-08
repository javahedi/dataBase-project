
import os
# run the Postgrs server
import psycopg2
# to return the Postgres table with their volumn title
from psycopg2.extras import RealDictCursor 
import time
import pandas as pd



def csv_files():

    #get names of only csv files
    csv_files = []
    for file in os.listdir(os.getcwd()):
        if file.endswith(".csv"):
            csv_files.append(file)

    return csv_files


def configure_dataset_directory(csv_files, dataset_dir):
  
    #make dataset folder to process csv files
    try: 
      mkdir = 'mkdir {0}'.format(dataset_dir)
      os.system(mkdir)
    except:
      pass

    #move csv files to dataset folder
    for csv in csv_files:
        mv_file = "mv '{0}' {1}".format(csv, dataset_dir)
        os.system(mv_file)

    return


def text_polish(text):
    return text.lower().replace(' ','_').replace('/','_').replace('\\','_').\
                            replace('$','').replace('%','').replace('-','_')


def clean_tbl_name(filename):
    clean_tbl_name = text_polish(filename)
    tbl_name = clean_tbl_name.split('.')[0]
    return tbl_name


def clean_colname(dataframe):

    dataframe.columns = [ text_polish(x) for x in dataframe.columns]

    # change between types in panda to SQL
    replacements = {
        'timedelta64[ns]'   : 'varchar',
        'object'            : 'varchar',
        'float64'           : 'float',
        'int64'             : 'int',
        'datetime64'        : 'timestamp'
    }

    col_str = ", ".join("{} {}".format(n,d) for (n,d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))

    return col_str, dataframe.columns


    
def create_df(dataset_dir, csv_files):

    data_path = os.getcwd()+'/'+dataset_dir+'/'
    df = {}
    for file in csv_files:
        try:
            df[file] = pd.read_csv(data_path+file)
        except UnicodeDecodeError:
            df[file] = pd.read_csv(data_path+file, encoding='ISO-8859-1') # if utf-8 encoding error
        print(file)
        
    return df


def upload_to_db(host, dbname, user, password, tbl_name, col_str, file, dataframe, dataframe_columns):

   
    while True:
        try:
            # Connect to your postgres DB
            conn = psycopg2.connect(host =host, database=dbname, 
                                    user =user, password= password,
                                    cursor_factory=RealDictCursor )
            # Open a cursor to perform database operations
            cursor = conn.cursor()
            print('database connection was succesfull!!!')
            break
        except Exception as error:
            print('database connection was failed!!!')
            print('Erorr : ',  error)
            time.sleep(2)

    cursor.execute(f'DROP TABLE IF EXISTS {tbl_name};')
    
    cursor.execute(f'CREATE TABLE {tbl_name} ({col_str});')
    print(f'{tbl_name} was created successefully')

    # insert (copy) values from csv to sql table

    # save df to csv
    dataframe.to_csv(file, header=dataframe_columns, index=False, encoding='utf-8')

    # open csv file, open as object
    my_file = open(file)
    print('file opend in memory...')


    # upload to SQL db
    SQL_STATMENT = """ 
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """

    cursor.copy_expert(sql=SQL_STATMENT % tbl_name, file=my_file)
    print('file copied to db')


    #
        
    #cursor.execute("grant select on table %s to public" % tbl_name)
    conn.commit()
    cursor.close()
    print(f'table {tbl_name} imported to db completed')

    return 
