import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
from sys import argv

# reads from Project A
# writes to Project B
options = PipelineOptions(flags=argv)
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = "project-b"
google_cloud_options.job_name = 'this-is-my-job'
google_cloud_options.service_account_email = "service-account@project-b.iam.gserviceaccount.com"
google_cloud_options.staging_location = 'gs://project-b_sto_dataflow_mtl/staging'
google_cloud_options.temp_location = 'gs://project-b_sto_dataflow_mtl/temp'
google_cloud_options.region = 'northamerica-northeast1'
options.view_as(StandardOptions).runner = 'DataflowRunner'
worker_options = options.view_as(WorkerOptions)
worker_options.subnetwork = 'regions/northamerica-northeast1/subnetworks/subnet'
worker_options.use_public_ips = False


with beam.Pipeline(options=options)as p:
    query = "SELECT Id, KeyPartitionDate, Col1, Col2 " \
            "FROM `project-a.domain_data.table` LIMIT 100"

    bq_source = beam.io.BigQuerySource(query=query, use_standard_sql=True)

    bq_data = p | "ReadFromBQ" >> beam.io.Read(bq_source)

    table_schema = 'Id:STRING, KeyPartitionDate:DATE, Col1:STRING, Col2:NUMERIC'
    bq_data | beam.io.WriteToBigQuery(
        project="project-b",
        dataset="model_dataset",
        table="dataflow_table",
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
