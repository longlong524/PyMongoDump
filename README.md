# PyMongoDump
## A python script for replacing mongodump tool because of its slow speed.
  Mongodump is very slow in copying data from one mongodb to another. Because it apply just one thread to one collection. If the collection is very large, the process will be very slow.
  
  So We develope a python script to replace it with applying several processes to one collection if it is very big. After Testing its speed is about 5 times faster than mongodump.
## How to use
In the script, You can configure some parameter.

    #start to copy the data from the day before today
    MAXDAY_BEFORE=800 
    #How many parts we divide all the days
    ALLDAYS_BLOCKS=250
    #How many days one part contains 
    DAYS_INTERVAL=2
    #Source mongodb information
    MONGO_HOST='#.#.#.#' 
    MONGO_PORT=27017
    MONGO_USER='add'
    MONGO_PASS='1'
    MECHANISM='MONGODB-CR'
    
    #Destination mongodb information
    LOCAL_MONGO_HOST='localhost'
    LOCAL_MONGO_PORT=27017
    LOCAL_MONGO_USER=''
    LOCAL_MONGO_PASS='123Yuanshuju456'
    LOCAL_MECHANISM='SCRAM-SHA-1'

    #Process num in the pool
    PROCESS_NUM=50
    #the databases to copy
    DBS_TO=[]
    #the collections to copy
    COLS_TO=[]
    # the excluded databases
    EXCLUDE_DBS=[]
    # the excluded collections
    EXCLUDE_COLS=[]
    # The collections count limit. Use one process if the collection's count is lower than this parameter and use process pool otherwise. 
    COUNT_LIMIT=1000
