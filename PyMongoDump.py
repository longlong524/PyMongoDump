import  pymongo
from pymongo import MongoClient
import datetime
import time
from multiprocessing import Pool
from bson.objectid import ObjectId
import random
from pymongo import IndexModel, ASCENDING, DESCENDING

MAXDAY_BEFORE=800
ALLDAYS_BLOCKS=250
DAYS_INTERVAL=2
MONGO_HOST='119.29.116.12'
MONGO_PORT=27017
MONGO_USER='root'
MONGO_PASS='123Yuanshuju456'
MECHANISM='MONGODB-CR'

LOCAL_MONGO_HOST='localhost'
LOCAL_MONGO_PORT=27017
LOCAL_MONGO_USER=''
LOCAL_MONGO_PASS='123Yuanshuju456'
LOCAL_MECHANISM='SCRAM-SHA-1'

PROCESS_NUM=50
DBS_TO=[]
COLS_TO=[]
EXCLUDE_DBS=[]
EXCLUDE_COLS=[]
COUNT_LIMIT=1000

client =None
client2 =None
def getConn():
    global client
    global client2
    client = MongoClient(MONGO_HOST, MONGO_PORT)  # 10.104.141.211 :119.29.116.12
    if MONGO_USER !='':
        client.admin.authenticate(MONGO_USER, MONGO_PASS, mechanism=MECHANISM)
    client2 = MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT) 
    if LOCAL_MONGO_USER !='':
        client2.admin.authenticate(LOCAL_MONGO_USER, LOCAL_MONGO_PASS, mechanism=LOCAL_MECHANISM)

def getdata_task(args):
    begin=args[0]
    end=args[1]
    db_str=args[2]
    col_str=args[3]
    con={
         '_id':
                {
                '$gt':ObjectId(str(hex(begin))[2:]+'0000000000000000'),
                '$lte':ObjectId(str(hex(end))[2:]+'0000000000000000')
                }
         }
    while True:
        try:
            db=client.get_database(db_str);
            collection=db.get_collection(col_str)
            col2=client2.get_database(db_str).get_collection(col_str);
            cursor=collection.find(con)
            i=0;
            for record in cursor:
                i+=1
                if i%10000==0:
                    print(i)
                up_con=record['_id'];
                col2.update({'_id':up_con},record,True)
            break;
        except Exception,e:
            print(e)
            continue
    print(str(begin)+":"+str(end)+":finished::"+str(i))
    
def copySmallCol(args):
    db_str=args[0]
    col_str=args[1]
    print(db_str+":"+col_str+" is small, will use one thread to copy")
    while True:
        try:
            db=client.get_database(db_str);
            collection=db.get_collection(col_str)
            col2=client2.get_database(db_str).get_collection(col_str);
            cursor=collection.find()
            i=0;
            for record in cursor:
                i+=1
                if i%10000==0:
                    print(i)
                up_con=record['_id'];
                col2.update({'_id':up_con},record,True)
            break;
        except Exception,e:
            print(e)
            continue
    print(db_str+":"+col_str+":finished::"+str(i))

def copyIndex(DB_TO,COL_TO):
    while True:
        try:
            collection=client.get_database(DB_TO).get_collection(COL_TO)
            col2=client2.get_database(DB_TO).get_collection(COL_TO)
            client2.get_database(DB_TO).drop_collection(COL_TO)
            client2.get_database(DB_TO).create_collection(COL_TO)
            for index in collection.list_indexes():
                x=[]
                for k in index.get('key'):
                    x.append((k,int(index.get('key')[k])))
                print(x)
                del index['key']
                col2.create_index(x,**index)
            break;
        except Exception,e:
            print("Error:"+str(e))
            continue
def decideCopyDBS():
    if len(DBS_TO) !=0:
        return DBS_TO
    dbs=client.database_names()
    re=[]
    for db in dbs:
        if db in EXCLUDE_DBS:
            pass
        else:
            re.append(db)
    re.remove('admin')
    re.remove('local')
    return re

def decideCopyCOLS(db):
    cols=client.get_database(db).collection_names(False)
    if len(COLS_TO)!=0:
        return list(set(cols).intersection(set(COLS_TO)));
    return list(set(cols).difference(set(EXCLUDE_COLS)))

def copytBigCol(db,col):
    tt=int(time.time())
    args=[tt-24*3600*MAXDAY_BEFORE]
    for i in xrange(ALLDAYS_BLOCKS):
        args.append(tt-DAYS_INTERVAL*24*3600*(ALLDAYS_BLOCKS-i))
    args.append(tt)
    pre=[]
    for i in range(len(args)-1):
        pre.append((args[i],args[i+1],db,col))
    random.shuffle(pre)
    p.map(getdata_task, pre)
if __name__=='__main__':
    getConn()
    p=Pool(processes=PROCESS_NUM,
         initializer=getConn)
    for db in decideCopyDBS():
        for col in decideCopyCOLS(db):
            copyIndex(db,col)
            if client.get_database(db).get_collection(col).count() <COUNT_LIMIT:
                p.map(copySmallCol, [(db,col)])
                print(db+":"+col+" is small, will use one thread to copy")
                pass;
            else:
                print(db+":"+col+" is large, will use multiprocess to copy")
                copytBigCol(db,col)
                pass
    p.close()
    p.join()
    
    

