
from csv_to_db import *
import config

dataset_dir = 'datasets'

csv_files = csv_files()
configure_dataset_directory(csv_files, dataset_dir)
df = create_df(dataset_dir, csv_files)




for c in csv_files:
    print(c)
    # call dataframe
    dataframe = df[c]

    #clean table name
    tbl_name = clean_tbl_name(c)
    
    #clean column names
    col_str, dataframe.columns = clean_colname(dataframe)
  
    
    # upload to dp
    upload_to_db(
                config.HOST,
                config.DATABASE,
                config.USER,
                config.PASSWORD,
                tbl_name,
                col_str,
                file=c,
                dataframe=dataframe,
                dataframe_columns=dataframe.columns)
   
