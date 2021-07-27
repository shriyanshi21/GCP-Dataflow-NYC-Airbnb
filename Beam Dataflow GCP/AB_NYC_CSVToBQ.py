"""Workflow to read the csv file and write the bigQuery table"""


import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.dataframe.io import read_csv
from apache_beam.io.gcp import gcsio
import csv
import io
   
# Function handles the reading of csv file and converting it into JSON format

def get_csv_reader(readable_file):

    # Open a channel to read the file from GCS
    gcs_file = beam.io.filesystems.FileSystems.open(readable_file)
    reader=csv.DictReader(io.TextIOWrapper(gcs_file))
    jsonArray=[]
    for row in reader: 
            #add this python dict to json array
            jsonArray.append(row)
    return jsonArray



def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the springmltest pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://trygcloudproject/AB_NYC_2019.csv',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  #Defining the table schema guided by the hearder of the CSV file 
 
  table_schema = ('id:INTEGER, name:STRING, host_id:STRING, host_name:STRING, neighbourhood_group:STRING,	neighbourhood:STRING, latitude:STRING,	longitude:STRING,	room_type:STRING	, price:STRING,	minimum_nights:STRING,	number_of_reviews:STRING,	last_review:STRING, reviews_per_month:STRING, calculated_host_listings_count:INTEGER, availability_365:STRING')

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  p1=beam.Pipeline(options=pipeline_options) 
 

  with beam.Pipeline(options=pipeline_options) as p:
      lines=(p|"ReadFromFile" >> beam.Create([known_args.input])
              |"ParseCSV" >> beam.FlatMap(get_csv_reader)  
              |"write to bigquery :" >> beam.io.WriteToBigQuery(table="springmltest",dataset="springmltest",project="springmltest",schema=table_schema,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))


      

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()


