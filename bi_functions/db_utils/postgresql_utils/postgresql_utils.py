import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import json
import os
import time

def map_call_log(obj):
    object =  {
    "id"  : obj.get('id',None),
    "uri"  : obj.get('uri',None),
    "extension"  : obj.get('extension',None),
    "telephonySessionId" : obj.get('telephonySessionId',None),
    "sipUuidInfo" : obj.get('sipUuidInfo',None),
    "transferTarget" : obj.get('transferTarget',None),
    "transferee"  : obj.get('transferee',None),
    "partyId" : obj.get('partyId',None),
    "transport"  : obj.get('transport',None),
    "from" : obj.get('from',None),
    "to" : obj.get('to',None),
    "type"  : obj.get('type',None),
    "direction"  : obj.get('direction',None),
    "message"  : obj.get('message',None),
    "delegate"  : obj.get('delegate',None),
    "delegationType" : obj.get('delegationType',None),
    "action"  : obj.get('action',None),
    "result"  : obj.get('result',None),
    "reason"  : obj.get('reason',None),
    "reasonDescription" : obj.get('reasonDescription',None),
    "startTime" : obj.get('startTime',None),
    "duration"  : obj.get('duration',None),
    "durationMs" : obj.get('durationMs',None),
    "recording"  : obj.get('recording',None),
    "shortRecording" : obj.get('shortRecording',None),
    "billing"  : obj.get('billing',None),
    "internalType" : obj.get('internalType',None),
    "sessionId" : obj.get('sessionId',None),
    "deleted"  : obj.get('deleted',None),
    "legs"  : obj.get('legs',None),
    "lastModifiedTime" : obj.get('lastModifiedTime',None),
    "dwCreatedAt" : obj.get('dwCreatedAt',datetime.now())}

        # Convert nested dictionaries to JSON strings
    for key, value in object.items():
        if isinstance(value, dict):
            object[key] = json.dumps(value)  # Convert dict to JSON string
        elif isinstance(value, list):
            object[key] = json.dumps(value)  # Convert list to JSON string (if needed)
    return object

def create_engine(uri):
    engine = create_engine(uri)


def insert_data_into_db(data, engine, table_name = 'ringcentral_call_logs_test', schema = 'public', if_exists='append'):
    
    
    # Create a DataFrame
    dataframe = pd.DataFrame([data],index= None)
    df = dataframe.reset_index(drop=True)
    
    
    try:
        df.to_sql(con=engine, if_exists=if_exists,schema=schema, name=table_name, index=None)
    except Exception as e:
        if "duplicate key" in str(e):
            pass
        else :
            print(f" insert exception : {e}")