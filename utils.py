# IVY2 repository path (ivy_repo)
ivy_repo = '/opt/Ivy2'

# cutoff threshold value to only compute metrics for terms with a frequency > threshold
threshold = 0



# Database configurations
dbname = 'ATR_app' # database name
db_conn_str = 'mongodb://127.0.0.1/' # connection string

# MongoDB Collections
col_doc_preprocess = 'documents' # collection for storing the candidate terms obtained after text preprocessing
col_c_value = 'cvalue' # collection for storing the candidate terms obtained and their C-Value
col_nc_value = 'ncvalue' # collection for storing the candidate terms obtained and their NC-Value
col_lidf_value = 'lidfvalue' # collection for storing the candidate terms obtained and their LIDF-Value