'''
Created on Nov 27, 2019

@author: gonzalo
'''
from __future__ import absolute_import

import argparse
import logging
import json



import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class GetData(beam.DoFn):
    def process(self):
        #process the JSON file from GCS bucket
        from google.cloud import storage
        bucket = storage.Client().get_bucket('mybucket')
        blob = bucket.get_blob('train-v2.0.json')
        data = blob.download_as_string()
        
        js = json.loads(data)
        group = 0
        items = 0
        for data in js["data"]:
          for i in data["paragraphs"]:
              for j in i["qas"]:
                  column = {}
                  column["context"] = i["context"]
                  for z in j["answers"]:
                      column["answer_start"] = z["answer_start"]
                      column["text"] = z["text"]
                  
                  column["question"] = j["question"]
                  try:
                      column["is_impossible"] = str(j["is_impossible"])
                  except:
                      ''
                  column["id"] = j["id"]
                  if items > 50:
                      items = 0
                      group += 1
                  items += 1
                  #return each value in the format (group, value) in order to
                  # use the GoupByKey function
                  yield (group,column)
              

    
def toBQ(element):
    #transform each item into Bigquery item
    for item in element:
        yield item

def run(argv=None, save_main_session=True):

  parser = argparse.ArgumentParser()
  
  
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      '--runner=DataflowRunner',
      '--region=us-central1',
      '--streaming',
      '--enable_streaming_engine',
      '--setup_file=./setup.py',
      '--project=project',
      '--staging_location=gs://mybucket/jsonBQ',
      '--temp_location=gs://mybucket/jsonBQ/temp',
      '--job_name=jobapachebeam',
      '--num_workers=50'
  ])

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  
  
  
  table = "project:dataset.SQuADv20"
  table_schema = {"fields": [{"name": "context", "type": "STRING", "mode": "REQUIRED"},
                             {"name": "answer_start", "type": "INT64", "mode": "REQUIRED"},
                             {"name": "text", "type": "STRING", "mode": "REQUIRED"},
                             {"name": "question", "type": "STRING", "mode": "REQUIRED"},
                             {"name": "is_impossible", "type": "STRING", "mode": "REQUIRED"},
                             {"name": "id", "type": "STRING", "mode": "REQUIRED"}]}
  with beam.Pipeline(options=pipeline_options) as p:

    #initialize the list to one element in order to process the JSON as streamming process
    (p | beam.Create([1]) 
           | beam.ParDo(GetData())
           | beam.GroupByKey()
           | beam.Map(lambda (x,y): y)
           | beam.ParDo(lambda x: toBQ(x))
           | beam.io.WriteToBigQuery(
                     table,
                     schema=table_schema,
                     #write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                     ))
    
    
    
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
