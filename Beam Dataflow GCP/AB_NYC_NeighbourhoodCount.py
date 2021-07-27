"""Workflow to read the original data from a bigQuery table and run query to calculate listings count by neighbourhood"""

import re
import sys
import logging
import argparse
import apache_beam as beam
from apache_beam.dataframe.io import read_csv
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the neighbourhoodcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://trygcloudproject/abc.csv',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
 
  #Defining the table schema guided by the hearder of the CSV file 
  
  table_count_schema=('neighbourhood:STRING, count:INTEGER')
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  #TODO: can parametirize few things here to allow different queries and schema
  
  with beam.Pipeline(options=pipeline_options) as p:
      nb_count=(p|'QueryTableStdSQL' >> beam.io.ReadFromBigQuery(
          
            query='SELECT neighbourhood, sum(calculated_host_listings_count) as count FROM '\
            '`springmltest.springmltest.springmltest` group by neighbourhood',
            use_standard_sql=True)
            |"write count to bigquery :" >> beam.io.WriteToBigQuery(table="neighbourhoodcount",dataset="neighbourhoodcount",project="springmltest",schema=table_count_schema,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))
 

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()


