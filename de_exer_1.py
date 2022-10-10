import os
import argparse
#import psycopg2
import pg8000.dbapi
import logging
import pytest
import json
from typing import Generator
#importrpytestL, 105B written
import apache_beam as beam
import flask
from flask import jsonify
from flask_sqlalchemy import sqlalchemy
from google.cloud.sql.connector import Connector
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options_validator import PipelineOptionsValidator
from apache_beam.utils.annotations import deprecated
class db_data(beam.DoFn):
   # def process(self,element):
    def __init__(self,data_req):
       self.data_req=data_req
    def init_connection_engine(self) -> sqlalchemy.engine.Engine:
     def getconn() -> pg8000.dbapi.Connection:
        # initialize Connector object for connections to Cloud SQL
        with Connector() as connector:
            conn: pg8000.dbapi.Connection = connector.connect(
                     "sonam-sandbox-356306:australia-southeast1:sub-act",
                 "pg8000",
                 user="postgres",
                 password="sub-act-df",
                 db="subscriber-act",
             )
            return conn
    # create SQLAlchemy connection pool
     pool = sqlalchemy.create_engine(
             "postgresql+pg8000://",
              creator=getconn,
            )
     pool.dialect.description_encoding = None
     print("to test connection")
     return pool
      # [END cloud_sql_connector_postgres_pg8000]
#@pytest.fixture(name="pool")
  #def setup() -> Generator:
 #def app_context():
  #  with app.app_context():
    def read_data_req(self):
      if self.data_req == 'voice_call':
          return(sqlalchemy.text("SELECT subscription_id,actual_duration FROM voice_call_record;"))
      if self.data_req == 'sms_rec':
          return(sqlalchemy.text("SELECT subscription_id,network_id FROM sms_record;"))
      elif self.data_req == 'data_volume':
          return(sqlalchemy.text("SELECT subscription_id,uplink_volume,downlink_volume FROM mobile_data_activity;"))
     # return (select_stmnt)    
    def process(self,element):
     pool = self.init_connection_engine()
    #def test_pooled_connection_with_pg8000(pool: sqlalchemy.engine.Engine) -> None:
   #  select_stmt = sqlalchemy.text("SELECT subscription_id,uplink_volume,downlink_volume FROM mobile_data_activity;")
     select_stmt= self.read_data_req()
     print(select_stmt)
     with pool.connect() as conn:
        rows= conn.execute(select_stmt).fetchall()
       # i =0
        for row in rows:
                    
          yield (dict(row))

class transform_data(beam.DoFn):
    def __init__(self):
      pass  
    def process(self,item):
  # for item in row.items():
      if 'uplink_volume' in item and 'downlink_volume' in item: 
          if item['uplink_volume']!=" ":
             item['uplink_volume']=int(item['uplink_volume'])
          elif item['uplink_volume']==" ":
             item['uplink_volume']=0  
          if item['downlink_volume']!=" ":
              item['downlink_volume']=int(item['downlink_volume'])
          elif item['downlink_volume']==" ":
              item['downlink_volume']=0
               
      yield item
class BuildRowFn(beam.DoFn):
    def __init__(self):
      pass  
    def process(self,element):
      row = {}
  #    print("to check d element and its type",element,'type:',type(element))
      (subscription_id,data)=element
      row['subscription_id']=subscription_id
        for key,value in data.items():
         # print("to test e",e)
          if value==[]:
         #   print("to test",e)
             value1=0
          else:
             value1=value[0]
          if key == 'min_voice_data':
             row['shortest_call']= value1
          elif key =='max_voice_data':
             row['longest_call']=value1
          elif key=='trans_sms_data':
             row['total_sms_sent']=value1
          elif key=='total_uplink':
             row['total_uplink_vol'] =value1
          elif key=='total_downlink':  
              row['total_downlink_vol']=value1
         # print(row)
      
    
      yield(row)


        
            
def run():
   table_spec='sonam-sandbox-356306:mob_net.subscriber_activity_snapshotfinal'
#table_schema='subscription_id:INTEGER,shortest_call_made:INTEGER,longest_call_made:INTEGER,total_number_sms:INTEGER,total_data_uploaded:INTEGER,total_data_downloaded:INTEGER'
   table_schema={
    "fields": [
    {
        "name": "subscription_id", "type": "INTEGER", "mode": "NULLABLE",
    },
    {
        "name": "shortest_call", "type": "INTEGER", "mode": "NULLABLE"
    },
    {
        "name": "longest_call", "type": "INTEGER", "mode": "NULLABLE"
    },
    {
        "name": "total_sms_sent", "type": "INTEGER", "mode": "NULLABLE"
    },
    {
        "name": "total_uplink_vol", "type": "INTEGER", "mode": "NULLABLE"
    },
     {
        "name": "total_downlink_vol", "type": "INTEGER", "mode": "NULLABLE"
    }]
}
   parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg', help='description')
   args, beam_args = parser.parse_known_args()
   beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    project='sonam-sandbox-356306',
    job_name='subscr-activity-job',
    temp_location='gs://ss_df_exercise/temp',
    region='australia-southeast2'

                                 )
   with beam.Pipeline(options=beam_options) as pipeline:
     

#******* calculating sms records per subscrption id from sms_record ****
       trans_sms_data=(
            pipeline
            |'Pipeline start for sms data'>>beam.Create([])
            |'Sms Record'>>beam.ParDo(db_data(data_req='sms_rec'))
            |'Pair the values'>> beam.Map(lambda x: (x['subscription_id'],x['network_id']))
           # | "To string" >> beam.ToString.Iterables()
            | "Count of SMS" >> beam.combiners.Count.PerKey()
                      )
            
#***** calculating shortest and longest voice call duration from voic_call_record ****        
       trans_voice_data=(
            pipeline
            |'Pipeline start for voice data'>>beam.Create([{}])
            |'Voice Record'>>beam.ParDo(db_data(data_req='voice_call'))
           # |'voice data'>>beam.Map(print) 
            |'Pairing'>>beam.Map(lambda x:(x['subscription_id'],x['actual_duration']))
       #     |beam.Map(print)
            )
       min_voice_data=(
           trans_voice_data|'Shortest_call'>>beam.CombinePerKey(min)
        #                   |'min result to check'>>beam.Map(print)
                       )
       max_voice_data=(
           trans_voice_data|'Longest_call'>>beam.CombinePerKey(max)
#                           |'max result to check'>>beam.Map(print)

                      )


#**** calculating data uploaded from mobile_data_activity  ******


       trans_volume_data=(
            pipeline
            |'Pipeline start for volume data'>>beam.Create([{}])
            |'Volume Record'>>beam.ParDo(db_data(data_req='data_volume'))
            |'Standarise data'>>beam.ParDo(transform_data())
          #  |'Total uplink_downlink data'>>beam.CombinePerKey(sum)
                  )
       total_uplink=(
               trans_volume_data|'uplink_vol'>>beam.Map(lambda x:(x['subscription_id'],x['uplink_volume']))
                                |'Total_uplink_volume'>>beam.CombinePerKey(sum)
                #                |'result to check'>>beam.Map(print)
                    )           
       total_downlink=(
               trans_volume_data|'downlink_vol'>>beam.Map(lambda x:(x['subscription_id'],x['downlink_volume']))
                                |'Total_downlink_volume'>>beam.CombinePerKey(sum)
                 #               |'result to check for downlink '>>beam.Map(print)
                      )         
       join =(
               {
                   'min_voice_data':min_voice_data,
                   'max_voice_data': max_voice_data,
                   'trans_sms_data':trans_sms_data,
                   'total_uplink':total_uplink,
                   'total_downlink':total_downlink
               }
              | beam.CoGroupByKey()
             # |'test'>>beam.Map(print)
              |'Row build for bq'>>beam.ParDo(BuildRowFn())
             
              |beam.io.WriteToBigQuery(
               table_spec,
               schema=table_schema,
               custom_gcs_temp_location='gs://ss_df_exercise/temp',
               write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
               )
              ) 
             
           # |'output'>>beam.io.WriteToText('gs://ss_df_exercise/temp/test.json')
               

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
 # process()
