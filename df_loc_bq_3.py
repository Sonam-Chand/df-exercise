from google.cloud import firestore
import os
import argparse
#import psycopg2
import pg8000
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
    
    def read_data_req(self):
      if self.data_req == 'voice_call':
          return(sqlalchemy.text("SELECT subscription_id,actual_duration FROM voice_call_record;"))
      elif self.data_req == 'sms_rec':
          return(sqlalchemy.text("SELECT subscription_id,network_id FROM sms_record;"))
      elif self.data_req == 'data_volume':
          return(sqlalchemy.text("SELECT subscription_id,uplink_volume,downlink_volume FROM mobile_data_activity;"))
      elif self.data_req == 'sub_rec':
          #return(sqlalchemy.text("SELECT CAST(rate_plan_product_id as VARCHAR(15)),CAST(subscription_id as INTEGER) FROM mobile_data_activity limit 50;"))
          return(sqlalchemy.text("SELECT CAST(rate_plan_product_id as INTEGER),CAST(subscription_id as VARCHAR(15)) FROM mobile_data_activity limit 100;"))
      elif self.data_req == 'sms_location':
          return(sqlalchemy.text("SELECT CAST(subscription_id as VARCHAR(15)),CAST(called_location_cd as VARCHAR(15)) AS location_cd FROM sms_record limit 100;"))
      elif self.data_req == 'mobile_location':
          return(sqlalchemy.text("SELECT CAST(subscription_id as VARCHAR(15)),CAST(called_location_cd as VARCHAR(15)) AS location_cd FROM voice_call_record limit 100;"))
     # return (select_stmnt)
    def process(self,element):
     pool = self.init_connection_engine()
     select_stmt= self.read_data_req()
     print(select_stmt)
     with pool.connect() as conn:
        rows= conn.execute(select_stmt).fetchall()
        
        for row in rows:
        
          yield (dict(row))
class fs_data(beam.DoFn):
    def __init__(self,req_data):
      self.req_data=req_data
    def getconn(self):
      db = firestore.Client(project='sonam-sandbox-356306')
      return db
    def read_data_req(self):
       con=self.getconn()
       if self.req_data=='location':
        users_ref = con.collection(u'location').stream()
       return users_ref
    def process(self,element):
       docs=self.read_data_req()
       #print(docs)
       for doc in docs:
          location_cd = u'{}'.format(doc.to_dict()['location_cd'])
         # print(rate_plan_id)
          country = u'{}'.format(doc.to_dict()['country'])
         # print(rate_plan_name)
          yield(location_cd,country)

#******** To drop the location_cd field and return the required fields to find popular places *******

class reformat_loc_data(beam.DoFn):
    def __init__(self):
         pass
    def process(self,element):
          #print("the element is",element)
          location_cd,data=element
         # print("the data is",data)
          for key,value in data.items():
            if key == 'country':
             value1=value
            if key=='subscription_id':
             value2=value
        
          if  value2 != []:
            if value1 != []:
             country=value1[0]
             subscription_id=value2[0]
           #  yield (country,subscription_id)


            elif value1 == []:
             subscription_id=value2[0]
             country=(location_cd+' country')
            yield (country,subscription_id)


# to add ranks and discard spare element(count)
class format_data(beam.DoFn):
    def process(self,element):
        for i,x in enumerate (element) :
         yield (i+1,x[0])

class BuildRowFn(beam.DoFn):
    def __init__(self):
      pass
    def process(self,element):
     row = {}
  #    print("to check d element and its type",element,'type:',type(element))
     (rank,data)=element
     row['ranking']=rank
     for key,value in data.items():
         if value==[]:
            value=['Not known']
        
         if key=='Top 5 busiest locations with the highest SMS':
           row['Top5_busiest_locations_with_highest_SMS']=value[0]
         if key=='Top 5 locations with the lowest SMS':
           row['Top5_locations_with_lowest_SMS']=value[0]
         if key=='Top 5 busiest locations with the highest mobile_data_activity':
             row['Top5_busiest_locations_with_highest_mobile_data_activity']=value[0]
         if key=='Top 5 locations with the lowest mobile_data_activity':
             row['Top5_locations_with_least_mobile_data_activity']=value[0]
     yield(row)
 

      




def run():
   input='gs://ss_df_exercise/subscriber.csv'
   table_spec='sonam-sandbox-356306:mob_net.network_activity'
#table_schema='subscription_id:INTEGER,shortest_call_made:INTEGER,longest_call_made:INTEGER,total_number_sms:INTEGER,total_data_uploaded:INTEGER,total_data_downloaded:INTEGER'
   table_schema={
    "fields": [
    {
        "name": "rank", "type": "INTEGER", "mode": "NULLABLE",
    },
    {
        "name": "Top5_busiest_locations_with_highest_SMS", "type": "STRING", "mode": "NULLABLE",
    },
    {
        "name": "Top5_locations_with_lowest_SMS", "type": "STRING", "mode": "NULLABLE",
    },
    {
        "name": "Top5_busiest_locations_with_highest_mobile_data_activity", "type": "STRING", "mode": "NULLABLE",
    },
    {
        "name": "Top5_locations_with_least_mobile_data_activity", "type": "STRING", "mode": "NULLABLE",
    },
                
        ]
                }
   parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg', help='description')
   args, beam_args = parser.parse_known_args()
   beam_options = PipelineOptions(
   beam_args,
    runner='DirectRunner',
    project='sonam-sandbox-356306',
    job_name='network-activity',
    temp_location='gs://ss_df_exercise/temp',
    region='australia-southeast2'
    
                                 )
   with beam.Pipeline(options=beam_options) as pipeline:

# ****** To get the location_cd and country from firestore ********
        location_data=(
            pipeline
            |'Pipeline start for location data'>>beam.Create([{}])
            |'location record'>>beam.ParDo(fs_data(req_data ='location'))
          #  |'print test'>>beam.Map(print)
                      )
    #******* To get subscribtion_id, called_location_cd from sms_record postgres records ****
        sms_loc_data=(
            pipeline
            |'Pipeline start for sms location data'>>beam.Create([{}])
            |'Subscription Record'>>beam.ParDo(db_data(data_req='sms_location'))
           # |'print data'>>beam.Map(print)
            )

    #******* Mapping the values from loc_data ************
        map_sms_loc_data=(
            sms_loc_data
            |'pairing the datavalues'>>beam.Map(lambda x:(x['location_cd'],x['subscription_id']))
          # |'print data'>>beam.Map(print)
                              )

    #******** joining sms_record and location *********

        total_sub_loc_data=({'country': location_data,
                             'subscription_id': map_sms_loc_data}
            |'subscriber_per_location'>>beam.CoGroupByKey()
            |'reformat_sub_loc_data'>>beam.ParDo(reformat_loc_data())
           #|'to check'>>beam.Map(print)
                                )

#****** to find busy country names based on number of sms subscribers ***********
        top_five_busiest_sms_location=(
            total_sub_loc_data
           #|'pairing the values'>>beam.Map(lambda x:(x['rate_plan_name'],x['subscription_id']))
            |"Count of Subscribers" >> beam.combiners.Count.PerKey()
            |"descending order">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1])).without_defaults()
           #|"Top plans">>beam.Map(print)
            |"change the format">>beam.ParDo(format_data())
          # |"testing">>beam.Map(print)
                        )
        least_five_busiest_sms_location=(
            total_sub_loc_data
           #|'pairing the values2'>>beam.Map(lambda x:(x['rate_plan_name'],x['subscription_id']))
            | "Count" >> beam.combiners.Count.PerKey()
            |"Ascending order">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1],reverse=True)).without_defaults()
            |"Format change">>beam.ParDo(format_data())
           #|"testing2">>beam.Map(print)
                                     )

            # *********** To get subscription id and location code from voice call record ***********

        mobile_loc_data=(
            pipeline
            |'Pipeline start for mobile location data'>>beam.Create([{}])
            |'mobile data Record'>>beam.ParDo(db_data(data_req='mobile_location'))
          # |'print data'>>beam.Map(print)
            )

    #******* Mapping the values from loc_data ************
        map_mobile_loc_data=(
            mobile_loc_data
            |'pairing the mobile_datavalues'>>beam.Map(lambda x:(x['location_cd'],x['subscription_id']))
          # |'print data'>>beam.Map(print)
                             )

    #******** joining voice_call_record and location *********

        total_mobile_act_loc_data=({'country': location_data,
                                  'subscription_id': map_mobile_loc_data}
            |'subscriber_per_location_mobile_data'>>beam.CoGroupByKey()
            |'reformat_sub_loc_mobile_data'>>beam.ParDo(reformat_loc_data())
          # |'to check'>>beam.Map(print)
                                )

# #****** to find busy country names based on number of sms subscribers ***********
        top_five_busiest_mobile_act_location=(
            total_mobile_act_loc_data
           #|'pairing the values'>>beam.Map(lambda x:(x['rate_plan_name'],x['subscription_id']))
            |"Count of Subscribers for mobile data" >> beam.combiners.Count.PerKey()
            |"descending order for mobile activity">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1])).without_defaults()
           #|"Top plans">>beam.Map(print)
            |"change the format for mobile activity data">>beam.ParDo(format_data())
         #  |"testing">>beam.Map(print)
                        )
        least_five_busiest_mobile_act_location=(
            total_mobile_act_loc_data
           #|'pairing the values2'>>beam.Map(lambda x:(x['rate_plan_name'],x['subscription_id']))
            | "Count for mobile data" >> beam.combiners.Count.PerKey()
            |"Ascending order for mobile data locations">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1],reverse=True)).without_defaults()
            |"Format change for mobile location data">>beam.ParDo(format_data())
          # |"testing2">>beam.Map(print)
                                     )



# *********** To join top 5 locations with highest and lowest sms's ***********
            join = (({
                'Top 5 busiest locations with the highest SMS':top_five_busiest_sms_location,
                 'Top 5 locations with the lowest SMS':least_five_busiest_sms_location,
                 'Top 5 busiest locations with the highest mobile_data_activity':top_five_busiest_mobile_act_location,
                 'Top 5 locations with the lowest mobile_data_activity':least_five_busiest_mobile_act_location
                  
              })
               | 'Merge' >> beam.CoGroupByKey()
             #  |'testing'>>beam.Map(print))
               |'Row build for bq'>>beam.ParDo(BuildRowFn())
             #  |'test3'>>beam.Map(print))
               |beam.io.WriteToBigQuery(
                table_spec,
                schema=table_schema,
                custom_gcs_temp_location='gs://ss_df_exercise/temp',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                 )
              ) 
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()