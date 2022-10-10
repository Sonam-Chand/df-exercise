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
      elif self.data_req == 'sms_rec':
          return(sqlalchemy.text("SELECT subscription_id,network_id FROM sms_record;"))
      elif self.data_req == 'data_volume':
          return(sqlalchemy.text("SELECT subscription_id,uplink_volume,downlink_volume FROM mobile_data_activity;"))
      elif self.data_req == 'sub_rec':
          #return(sqlalchemy.text("SELECT CAST(rate_plan_product_id as VARCHAR(15)),CAST(subscription_id as INTEGER) FROM mobile_data_activity limit 50;"))
          return(sqlalchemy.text("SELECT CAST(rate_plan_product_id as INTEGER),CAST(subscription_id as VARCHAR(15)) FROM mobile_data_activity limit 100;"))
     # return (select_stmnt)
    def process(self,element):
     pool = self.init_connection_engine()
    #def test_pooled_connection_with_pg8000(pool: sqlalchemy.engine.Engine) -> None:
   #  select_stmt = sqlalchemy.text("SELECT subscription_id,uplink_volume,downlink_volume FROM mobile_data_activity;"
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
       if self.req_data=='rate_plan':
        users_ref = con.collection(u'rate_plan_product').stream()
       return users_ref
    def process(self,element):
       docs=self.read_data_req()
       #print(docs)
       for doc in docs:
          rate_plan_product_id = u'{}'.format(doc.to_dict()['rate_plan_product_id'])
         # print(rate_plan_id)
          rate_plan_name = u'{}'.format(doc.to_dict()['rate_plan_name'])
         # print(rate_plan_name)
          yield(int(rate_plan_product_id),rate_plan_name)

class BuildRowFn(beam.DoFn):
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

# to add ranks and discard spare element(count)
class format_data(beam.DoFn):
    def process(self,element):
        for i,x in enumerate (element) :
         yield (i+1,x[0])
class build_row_age(beam.DoFn):
    def __init__(self):
        pass
    def process(self,element):
         subscription_id,data=element
         rate_plan_id,age=data
         if rate_plan_id==[]:
             rate_plan_product_id=0
         else:
             rate_plan_product_id=rate_plan_id[0]
         yield(rate_plan_product_id,subscription_id)

class reformat(beam.DoFn):
    def __init__(self):
         pass
    def process(self,element):
          #print("the element is",element)
          subscription_id,data=element
         # print("the data is",data)
          if data['rate_plan_product_id'] != [] and data['age'] != []:
             rate_plan_id=data['rate_plan_product_id'][0]
        #   else:
        #      rate_plan_id='0000'
             yield (rate_plan_id,subscription_id)

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


 
def run():
   input='gs://ss_df_exercise/subscriber.csv'
   table_spec='sonam-sandbox-356306:mob_net.rate_plan_prod3'
#table_schema='subscription_id:INTEGER,shortest_call_made:INTEGER,longest_call_made:INTEGER,total_number_sms:INTEGER,total_data_uploaded:INTEGER,total_data_downloaded:INTEGER'
   table_schema={
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
# parser.add_argument('--my-arg', help='description')
   args, beam_args = parser.parse_known_args()
   beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    project='sonam-sandbox-356306',
    job_name='rate-plan-trends',
    temp_location='gs://ss_df_exercise/temp',
    region='australia-southeast2'
    
                                 )
    with beam.Pipeline(options=beam_options) as pipeline:

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
 


      
        join = (({
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
              |'Row build for bq'>>beam.ParDo(BuildRowFn())
            # |'test3'>>beam.Map(print))
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
