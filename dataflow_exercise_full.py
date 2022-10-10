# A Python program for Dataflow pipeline capable of extracting, transforming and loading data from multiple sources to generate the required insights


import os
import argparse
import pg8000
import logging
import pytest
import json
import apache_beam as beam
import flask
from flask import jsonify
from typing import Generator
from google.cloud import firestore
from flask_sqlalchemy import sqlalchemy
from google.cloud.sql.connector import Connector
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options_validator import PipelineOptionsValidator
from apache_beam.utils.annotations import deprecated

# To create connection to Cloud Sql Postgres sql database
# This is common class for all three iterations
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
        print(" To display message that Postgres connection is established ")
        return pool
    # [END cloud_sql_connector_postgres_pg8000]

    # A function for fetching data from database connection based on the required query.
    def read_data_req(self):
        if self.data_req == 'voice_call':
           return(sqlalchemy.text("SELECT subscription_id,actual_duration FROM voice_call_record;"))
        elif self.data_req == 'sms_rec':
           return(sqlalchemy.text("SELECT subscription_id,network_id FROM sms_record;"))
        elif self.data_req == 'data_volume':
           return(sqlalchemy.text("SELECT subscription_id,uplink_volume,downlink_volume FROM mobile_data_activity;"))
        elif self.data_req == 'sub_rec':
           return(sqlalchemy.text("SELECT CAST(rate_plan_product_id as INTEGER),CAST(subscription_id as VARCHAR(15)) FROM mobile_data_activity limit 100;"))
        elif self.data_req == 'sms_location':
           return(sqlalchemy.text("SELECT CAST(subscription_id as VARCHAR(15)),CAST(called_location_cd as VARCHAR(15)) AS location_cd FROM sms_record limit 100;"))
        elif self.data_req == 'mobile_location':
           return(sqlalchemy.text("SELECT CAST(subscription_id as VARCHAR(15)),CAST(called_location_cd as VARCHAR(15)) AS location_cd FROM voice_call_record limit 100;"))
    
    #  The process function to be called in ParDo
    def process(self,element):
        pool = self.init_connection_engine()
    
        select_stmt= self.read_data_req()
        print(f"The query called is: {select_stmt}")
        with pool.connect() as conn:
            rows= conn.execute(select_stmt).fetchall()
            for row in rows:
                yield (dict(row))

# transform volume data for iteration 1.
class transform_volume_data(beam.DoFn):
    def __init__(self):
      pass  
    def process(self,item):
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

# To format into rows to write into Big Query    
# To be called in iteration 1

class BuildRowFn_Iter1(beam.DoFn):
    def __init__(self):
        pass  
    def process(self,element):
        row = {}
  #     print("to check d element and its type",element,'type:',type(element))
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
# To fetch data from firestore based on required data
# This class not required in iteration 1
class fs_data(beam.DoFn):
    def __init__(self,req_data):
        self.req_data=req_data
    def getconn(self):
        db = firestore.Client(project='sonam-sandbox-356306')
        return db
    def process(self,element):
        con=self.getconn()
        if self.req_data=='rate_plan':
            docs = con.collection(u'rate_plan_product').stream()
            for doc in docs:
                rate_plan_product_id = u'{}'.format(doc.to_dict()['rate_plan_product_id'])
                # print(rate_plan_id)
                rate_plan_name = u'{}'.format(doc.to_dict()['rate_plan_name'])
                # print(rate_plan_name)
                yield(int(rate_plan_product_id),rate_plan_name)

        elif self.req_data=='location':
            docs = con.collection(u'location').stream()
            for doc in docs:
                location_cd = u'{}'.format(doc.to_dict()['location_cd'])
                # print(rate_plan_id)
                country = u'{}'.format(doc.to_dict()['country'])
                # print(rate_plan_name)
                yield(location_cd,country)

# To parse and split csv data after reading from GCS bucket and before further transformation
class CustomParsing(beam.DoFn):
    # Custom ParallelDo class to apply a custom transformation

    def to_runner_api_parameter(self, unused_context):
      # Not very relevant, returns a URN (uniform resource name) and the payload
        return "beam:transforms:custom_parsing:custom_v0", None
    def process(self,element):
        
        element=element.split(",")
        
        row={}
        row={'subscription_id':element[0],
            'age':int(element[5])}
        yield row

# To transform the rows before we write into Big Query for itreration Rate Plan Trends
class BuildRowFn_Iter2(beam.DoFn):
    def __init__(self):
      pass
    def process(self,element):
      row = {}
  #    print("to check d element and its type",element,'type:',type(element))
      (rank,data)=element
      row['rank']=rank
      for key,value in data.items():
          if  value==[]:
            value1='General Plan'
          else:
            value1=value[0]
          if key=='top_five_rate_plan_perusage':
            row['top_five_plans_name_per_usage']=value1
          if key=='least_five_rate_plan_perusage':
            row['least_five_plans_name_per_usage']=value1
          if key=='top5_plan_grouped50_plus':
            row['top5_plan_grouped50_plus']=value1 
          if key=='least5_plan_grouped50_plus':
            row['least5_plan_grouped50_plus']=value1
          if key=='top5_plan_grouped30_50':
            row['top5_plan_grouped30_50']=value1
          if key=='least5_plan_grouped30_50':
            row['least5_plan_grouped30_50']=value1
          if key=='top5_plan_grouped18_30':
            row['top5_plan_grouped18_30']=value1
          if key=='least5_plan_grouped18_30':
            row['least5_plan_grouped18_30']=value1
          if key=='top5_plan_grouped18_less':
            row['top5_plan_grouped18_less']=value1
          if key=='least5_plan_grouped18_less':
            row['least5_plan_grouped18_less']=value1    
                    
      yield(row)

# to add ranks and discard spare element(count) for iter2 & iter3
class format_data(beam.DoFn):
    def process(self,element):
        for i,x in enumerate (element) :
            yield (i+1,x[0])
# to reformat to make sure we get both filds non zero for iter2
class reformat(beam.DoFn):
    def __init__(self):
        pass
    def process(self,element):
        #print("the element is",element)
        subscription_id,data=element
         # print("the data is",data)
        if data['rate_plan_product_id'] != [] and data['age'] != []:
            rate_plan_id=data['rate_plan_product_id'][0]
            yield (rate_plan_id,subscription_id)
# To reformat plan data as per requirement iter2
class reformat_plan(beam.DoFn):
    def __init__(self):
            pass
    def process(self,element):
          #print("the element is",element)
            rate_plan_product_id,data=element
         # print("the data is",data)
            for key,value in data.items():
                if key == 'rate_plan_name':
                    value1=value
                if key=='subscription_id':
                    value2=value
            if  value2 != []:
                if value1 != []:
                    rate_plan_name=value1[0]
                    subscription_id=value2[0]
                    yield (rate_plan_name,subscription_id)
                elif value1 == []:
                    subscription_id=value2[0]
                    rate_plan_name='General Plan'
                    yield (rate_plan_name,subscription_id)

# a function to be called for partioning based on age in iter 2
def by_age(row,no_partitions,age_range):
    #grouping by age
    assert no_partitions==len(age_range)
    for i,part in enumerate(age_range):
       # for i in row:
       if row['age']:
              data = int(row['age'])
             # data=value
              if data >= 50: i = 0
              elif data >= 30 and data < 50: i = 1
              elif data >= 18 and data < 30: i = 2
              elif data < 18 and data > 0: i = 3
              else: i = 4
              
            #  print(row)

    return i
#******** To drop the location_cd field and return the required fields to find popular places *******
# For inter3 Network Activity Snapshot in Datastore:

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
# To transform rows before write into Firestore
class BuildRowFn_Iter3(beam.DoFn):
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

#******** To write to firestore *********
class writetofirestore(beam.DoFn):
    def __init__(self):
      pass
    def getconn(self):
     db = firestore.Client(project='sonam-sandbox-356306')
     return db
   
    def process(self,row):
     con=self.getconn()
     #print(row)
     
     for key,value in row.items():
        if key=='ranking':
           rank=value
        if key=='Top5_busiest_locations_with_highest_SMS':
           value1=value
        if key=='Top5_locations_with_lowest_SMS':
           value2=value
        if key=='Top5_busiest_locations_with_highest_mobile_data_activity':
            value3=value
        if key=='Top5_locations_with_least_mobile_data_activity':
            value4=value
    # Add a new doc in collection 
     doc_ref = con.collection(u'Network_Activity_Snapshot').document()
     doc_ref.set({u'Rank':rank,
      u'Top 5 busiest locations with the highest SMS': value1,
      u'Top 5 locations with the lowest SMS': value2,
      u'Top 5 busiest locations with the highest mobile_data_activity': value3,
      u'Top 5 locations with the lowest mobile_data_activity': value4})
     
     #con.collection(u'network_activity_snapshot').document(u'rank').set(data)
def run():

    # xxxxxxxxxxx     table specifications and schema definitions for iteration subscriber activity snapdhot   xxxxxxxxxxxx

    table_spec_iter1='sonam-sandbox-356306:mob_net.subscriber_activity_snapshot'
    table_schema_iter1={
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

# xxxxxxxxxxx   bucket & table specifications and schema definitions for iteration rate plan trends   xxxxxxxxxxxx

    input='gs://ss_df_exercise/subscriber.csv'
    table_spec_iter2='sonam-sandbox-356306:mob_net.rate_plan_trend_snapshot'
    table_schema_iter2={
    "fields": [
    {
        "name": "rank", "type": "INTEGER", "mode": "NULLABLE",
    },
    {
        "name": "top_five_plans_name_per_usage", "type": "STRING", "mode": "NULLABLE"
    },
    {
        "name": "least_five_plans_name_per_usage", "type": "STRING", "mode": "NULLABLE"
    },
    {
        "name": "top5_plan_grouped50_plus", "type": "STRING", "mode": "NULLABLE"
    },
    {
        "name": "least5_plan_grouped50_plus", "type": "STRING", "mode": "NULLABLE"
    },
    {
        "name": "top5_plan_grouped30_50", "type": "STRING", "mode": "NULLABLE"
    },
    {
        "name": "least5_plan_grouped30_50", "type": "STRING", "mode": "NULLABLE"
    },
    {
        "name": "top5_plan_grouped18_30", "type": "STRING", "mode": "NULLABLE"
    },
    {
        "name": "least5_plan_grouped18_30", "type": "STRING", "mode": "NULLABLE"
    },
    {
        "name": "top5_plan_grouped18_less", "type": "STRING", "mode": "NULLABLE"
    },
    {
        "name": "least5_plan_grouped18_less", "type": "STRING", "mode": "NULLABLE"
    }
    ]
}
    parser = argparse.ArgumentParser()
    args, beam_args = parser.parse_known_args()
    beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    project='sonam-sandbox-356306',
    job_name='dataflow-exercise',
    temp_location='gs://ss_df_exercise/temp',
    region='australia-southeast2'
                                   )
    with beam.Pipeline(options=beam_options) as pipeline:

# xxxxxxxxxxxx Calculations for iteration subscriber sctivity snapshot    xxxxxxxxxxxxxxxxxx

#******* calculating sms records per subscrption id from sms_record ****
        trans_sms_data=(
            pipeline
            |'Pipeline start for sms data'>>beam.Create([])
            |'Sms Record'>>beam.ParDo(db_data(data_req='sms_rec'))
            |'Pair the values'>> beam.Map(lambda x: (x['subscription_id'],x['network_id']))
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
            |'Standarise data'>>beam.ParDo(transform_volume_data())
                          )
        total_uplink=(
            trans_volume_data
            |'uplink_vol'>>beam.Map(lambda x:(x['subscription_id'],x['uplink_volume']))
            |'Total_uplink_volume'>>beam.CombinePerKey(sum)
           # |'result to check'>>beam.Map(print)
                    )           
        total_downlink=(
            trans_volume_data
            |'downlink_vol'>>beam.Map(lambda x:(x['subscription_id'],x['downlink_volume']))
            |'Total_downlink_volume'>>beam.CombinePerKey(sum)
           # |'result to check for downlink '>>beam.Map(print)
                      )         
        join1 =(
                {
                    'min_voice_data':min_voice_data,
                    'max_voice_data': max_voice_data,
                    'trans_sms_data':trans_sms_data,
                    'total_uplink':total_uplink,
                    'total_downlink':total_downlink
                }
            | beam.CoGroupByKey()
          # |'test'>>beam.Map(print)
            |'Row build for bq'>>beam.ParDo(BuildRowFn_Iter1())
            |'write to BigQuery iter1'>>beam.io.WriteToBigQuery(
                table_spec_iter1,
                schema=table_schema_iter1,
                custom_gcs_temp_location='gs://ss_df_exercise/temp',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                                     )
              ) 

# xxxxxxxxxxxx Calculations for Rate Plan Trends    xxxxxxxxxxxxxxxxxx
# ****** To get the rate plan name and id from firestore ********
        rate_plan_data=(
            pipeline
            |'Pipeline start fro rate plan data'>>beam.Create([{}])
            |'rate-plan_record'>>beam.ParDo(fs_data(req_data ='rate_plan'))
           # |'print test'>>beam.Map(print)
                      )
#******* Top 5 most poplular and least popular rate_plan_id as per usage from mobile data activity records ****
        total_sub_data=(
            pipeline
            |'Pipeline start for mobile data'>>beam.Create([{}])
            |'Subscription Record'>>beam.ParDo(db_data(data_req='sub_rec'))
            )

        plan_data=(total_sub_data
            |'pairing the datavalues'>>beam.Map(lambda x:(x['rate_plan_product_id'],x['subscription_id']))
                       
           # |'print'>>beam.Map(print)
                       )

        total_sub_data_plan_name=({'rate_plan_name': rate_plan_data,
                                  'subscription_id': plan_data}
            |'rate_plan_name_perusage'>>beam.CoGroupByKey()
            |'reformat_plan_data_perusage'>>beam.ParDo(reformat_plan())
           # |'to check'>>beam.Map(print)
                                 )
        
        top_five_rate_plan_perusage=(
           total_sub_data_plan_name 
           # |'pairing the values'>>beam.Map(lambda x:(x['rate_plan_name'],x['subscription_id']))
            |"Count of Subscribers" >> beam.combiners.Count.PerKey()
            |"descending order">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1])).without_defaults()
           # |"Top plans">>beam.Map(print)
            |"change the format">>beam.ParDo(format_data())
           # |"testing">>beam.Map(print)
                        )
        least_five_rate_plan_perusage=(
           total_sub_data_plan_name
          #  |'pairing the values2'>>beam.Map(lambda x:(x['rate_plan_name'],x['subscription_id']))
            | "Count" >> beam.combiners.Count.PerKey()
            |"Ascending order">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1],reverse=True)).without_defaults()
            |"Format change">>beam.ParDo(format_data())
           # |"testing2">>beam.Map(print)
                                     )
#***** Top 5 most popular and least popular rat_plan_id as per age group ****
        age_data=(
            pipeline
            | 'GetCsv' >> beam.io.ReadFromText(input)
            | "CustomParse" >> beam.ParDo(CustomParsing())
          # |"test gcs data">>beam.Map(print)
                  )
        group50_plus,group30_50,group18_30,group18_less=(age_data|
              'partition_by_age'>>beam.Partition(by_age,4,age_range=[">50","30-50","18-30","<18"])
           )
    #    group50_plus|"print1">>beam.Map(print)
    #    group30_50|"print2">>beam.Map(print)
    #    group18_30|"print3">>beam.Map(print)
    #    group18_less|"print4">>beam.Map(print)


     #  Joining diff agegroup subscribers with rate plan data to get corresponding rate plan id ******

        required_data=(total_sub_data
            |'pairing the values3'>>beam.Map(lambda x:(x['subscription_id'],x['rate_plan_product_id']))
                     )
                    

        group50_plus_data=(group50_plus
            |beam.Map(lambda x:(x['subscription_id'],x['age']))
                          )
            #|'print2'>>beam.Map(print)

        grouped50_plus = ({'rate_plan_product_id':required_data,
                            'age':group50_plus_data}
            |'group1'>> beam.CoGroupByKey()
            |'reformat1'>>beam.ParDo(reformat())
                        )

        plan_name_group50_plus=({'rate_plan_name': rate_plan_data,
                                'subscription_id': grouped50_plus}
            |'rate_plan_name_group50_plus'>>beam.CoGroupByKey()
            |'reformat_plan_data'>>beam.ParDo(reformat_plan())
         #  |'to check'>>beam.Map(print)
                               )
        top5_plan_grouped50_plus=(plan_name_group50_plus
         #  |'mapping1'>>beam.Map(lambda x:(x['subscription_id'],x['rate_plan_product_id']))
            |"top count of Subscribers age50_plus" >> beam.combiners.Count.PerKey()
            |"descending order age50_plus">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1])).without_defaults()
            |"Format change group50_plus">>beam.ParDo(format_data())
          #  |"Top plans age50_plus">>beam.Map(print)
                                )
        least5_plan_grouped50_plus=(plan_name_group50_plus
            | "least count of subscriber age50_plus" >> beam.combiners.Count.PerKey()
            |"Ascending order for group50_plus">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1],reverse=True)).without_defaults()
            |"Format change grouped50_plus">>beam.ParDo(format_data())
           # |"least plans age50_plus">>beam.Map(print)
                                )

        
            
        group30_50_data=(group30_50
            |beam.Map(lambda x:(x['subscription_id'],x['age']))
                       )
            
        grouped30_50=  ({'rate_plan_product_id':required_data,
                            'age':group30_50_data}
            |'group2'>> beam.CoGroupByKey()
            |'reformat2'>>beam.ParDo(reformat())
                       )
        
        plan_name_group30_50=({'rate_plan_name': rate_plan_data,
                                'subscription_id': grouped30_50}
            |'rate_plan_name_group30_50'>>beam.CoGroupByKey()
            |'reformat_plan_data2'>>beam.ParDo(reformat_plan())
        #  |'to check'>>beam.Map(print)
                            )

        top5_plan_grouped30_50=(plan_name_group30_50
            |"Count of Subscribers age30_50" >> beam.combiners.Count.PerKey()
            |"descending order age30_50">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1])).without_defaults()
            |"Add ranking top group30_50">>beam.ParDo(format_data())
         #   |"Top plans age30_50">>beam.Map(print)
                              )
        least5_plan_grouped30_50=(plan_name_group30_50
            | "least count of subscriber age30_50" >> beam.combiners.Count.PerKey()
            |"Ascending order age30_50">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1],reverse=True)).without_defaults()
            |"Add ranking least group30_50">>beam.ParDo(format_data())
          #  |"least plans age30_50">>beam.Map(print)
                                 )

        
        group18_30_data= (group18_30
            |beam.Map(lambda x:(x['subscription_id'],x['age']))
                         )
        grouped18_30=  ({'rate_plan_product_id':required_data,
                            'age':group18_30_data}
            |'group3'>> beam.CoGroupByKey()
            |'reformat3'>>beam.ParDo(reformat())
                       )
        plan_name_group18_30=({'rate_plan_name': rate_plan_data,
                                'subscription_id': grouped18_30}
            |'rate_plan_name_group18_30'>>beam.CoGroupByKey()
            |'reformat_plan_data3'>>beam.ParDo(reformat_plan())
                      #  |'to check'>>beam.Map(print)
                               )
        top5_plan__grouped18_30=(plan_name_group18_30
            |"Count of Subscribers age18_30" >> beam.combiners.Count.PerKey()
            |"descending order age18_30">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1])).without_defaults()
            |"Add ranking top group18_30">>beam.ParDo(format_data())
           # |"Top plans age18_30">>beam.Map(print)
                                )
        least5_plan__grouped18_30=(plan_name_group18_30
            | "least count of subscriber age18_30" >> beam.combiners.Count.PerKey()
            |"Ascending order age18_30">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1],reverse=True)).without_defaults()
            |"Add ranking least group18_30">>beam.ParDo(format_data())
           # |"least plans age18_30">>beam.Map(print)
                                 )

                     
        group18_less_data=(group18_less
            |beam.Map(lambda x:(x['subscription_id'],x['age']))
                         )
       
        grouped18_less=  ({'rate_plan_product_id':required_data,
                            'age':group18_less_data}
            |'group4'>> beam.CoGroupByKey()
            |'reformat4'>>beam.ParDo(reformat())
                        )

        plan_name_group18_less=({'rate_plan_name': rate_plan_data,
                                'subscription_id': grouped18_less}
            |'rate_plan_name_group18_less'>>beam.CoGroupByKey()
            |'reformat_plan_data4'>>beam.ParDo(reformat_plan()
                               )
                      #  |'to check'>>beam.Map(print)
                               )
        top5_plan_grouped18_less=(plan_name_group18_less
            |"Count of Subscribers age18_less" >> beam.combiners.Count.PerKey()
            |"descending order age18_less">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1])).without_defaults()
            |"Add ranking top age18_less">>beam.ParDo(format_data())
          #  |"Top plans age18_less">>beam.Map(print)
                                 )
        least5_plan_grouped18_less=(plan_name_group18_less
            | "least count of subscriber age18_less" >> beam.combiners.Count.PerKey()
            |"Ascending order age18_less">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1],reverse=True)).without_defaults()
            |"Add ranking least group18_less">>beam.ParDo(format_data())
           # |"least plans age18_less">>beam.Map(print)
                                  )
 


      
        join2 = (({
                'top_five_rate_plan_perusage':top_five_rate_plan_perusage,
                 'least_five_rate_plan_perusage':least_five_rate_plan_perusage,
                  'top5_plan_grouped50_plus':top5_plan_grouped50_plus,
                  'least5_plan_grouped50_plus':least5_plan_grouped50_plus,
                  'top5_plan_grouped30_50':top5_plan_grouped30_50,
                  'least5_plan_grouped30_50':least5_plan_grouped30_50,
                  'top5_plan_grouped18_30':top5_plan__grouped18_30,
                  'least5_plan_grouped18_30':least5_plan__grouped18_30,
                  'top5_plan_grouped18_less':top5_plan_grouped18_less,
                   'least5_plan_grouped18_less':least5_plan_grouped18_less

              })
              | 'Merge' >> beam.CoGroupByKey()
             # |'testing'>>beam.Map(print))
              |'Row build for bq iter2'>>beam.ParDo(BuildRowFn_Iter2())
            # |'test3'>>beam.Map(print))
              |'write to Big Query Iter2'>>beam.io.WriteToBigQuery(
                table_spec_iter2,
                schema=table_schema_iter2,
                custom_gcs_temp_location='gs://ss_df_exercise/temp',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                 )
              ) 

    # # ****** To get the location_cd and country from firestore ********
        location_data=(
            pipeline
            |'Pipeline start for location data'>>beam.Create([{}])
            |'location record'>>beam.ParDo(fs_data(req_data ='location')))
           #|'print test'>>beam.Map(print))
#                       )
    #******* To get subscribtion_id, called_location_cd from sms_record postgres records ****
        sms_loc_data=(
            pipeline
            |'Pipeline start for sms location data'>>beam.Create([{}])
            |'Subscription Record for location data'>>beam.ParDo(db_data(data_req='sms_location'))
           #|'print data'>>beam.Map(print)
            )

    #******* Mapping the values from loc_data ************
        map_sms_loc_data=(sms_loc_data
            |'pairing the datalocation values'>>beam.Map(lambda x:(x['location_cd'],x['subscription_id']))
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
            |"Count of Subscribers for sms data" >> beam.combiners.Count.PerKey()
            |"descending order for subscriber for sms_location">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1])).without_defaults()
           #|"Top plans">>beam.Map(print)
            |"change the format iter3">>beam.ParDo(format_data())
          # |"testing">>beam.Map(print)
                        )
        least_five_busiest_sms_location=(
            total_sub_loc_data
            | "Count iter3" >> beam.combiners.Count.PerKey()
            |"Ascending order for sms subscriber for location">>beam.CombineGlobally(beam.combiners.TopCombineFn(5,lambda a:a[1],reverse=True)).without_defaults()
            |"Format_change">>beam.ParDo(format_data())
           #|"testing2">>beam.Map(print)
                                     )

            # *********** To get subscription id and location code from voice call record ***********

        mobile_loc_data=(
            pipeline
            |'Pipeline start for mobile location data'>>beam.Create([{}])
            |'mobile data Record for location'>>beam.ParDo(db_data(data_req='mobile_location'))
          # |'print data'>>beam.Map(print)
            )

    #******* Mapping the values from loc_data ************
        map_mobile_loc_data=(mobile_loc_data
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
        join3 = (({
                'Top 5 busiest locations with the highest SMS':top_five_busiest_sms_location,
                 'Top 5 locations with the lowest SMS':least_five_busiest_sms_location,
                 'Top 5 busiest locations with the highest mobile_data_activity':top_five_busiest_mobile_act_location,
                 'Top 5 locations with the lowest mobile_data_activity':least_five_busiest_mobile_act_location
                  
              })
            | 'Merge iter3' >> beam.CoGroupByKey()
            |'Row build for datastore'>>beam.ParDo(BuildRowFn_Iter3())
            |'write to datastore'>>beam.ParDo(writetofirestore())
                   
              ) 

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
 