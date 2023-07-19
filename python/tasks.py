"""
Title: tasks.py

Created on: 2023-07-18

Author: FriscianViales

Encoding: UTF-8

Description: Some description.
"""

import os
import luigi
import logging
import json
from google.cloud import bigquery
from custom_luigi import CustomExternalTask


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\alche\PycharmProjects\GlobantDataEngineerChallange\gdet-001-keys.json'
bq_client = bigquery.Client()


class LoadCSVtoBigQuery(CustomExternalTask):
    """ Load Master-Table partitions from GCS into a BQ table."""
    OBJECT = luigi.Parameter()
    SOURCE = luigi.Parameter()
    DESTINATION = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"tmp/{self.get_task_family()}-{self.SOURCE}.txt")

    def run(self):
        logging.info(f"Submitting BatchLoad job from: {self.SOURCE} to: {self.DESTINATION}")
        table = bigquery.Table(str(self.DESTINATION), schema=self.get_json_schema())
        _ = bq_client.create_table(table, exists_ok=True)
        job = self.submit_job()
        self.write_txt(str(job))

    def submit_job(self):
        load_job = bigquery.Client().load_table_from_uri(
            source_uris=self.SOURCE,
            destination=self.DESTINATION,
            job_config=self.job_config()
        )
        return load_job.result().__dict__

    def job_config(self):
        return bigquery.LoadJobConfig(
            allow_quoted_newlines=True,
            create_disposition='CREATE_IF_NEEDED',
            encoding='UTF-8',
            source_format=bigquery.SourceFormat.CSV,
            write_disposition='WRITE_TRUNCATE',
            schema=self.get_json_schema()
        )

    def get_json_schema(self) -> list:
        file_name = str(self.OBJECT).split('-')[0]
        with open(os.path.join(os.path.dirname(os.getcwd()), 'schemas', f"{file_name}.json"), "r") as f:
            return json.loads(f.read())


# class BackupTableToAvro(luigi.Task):
#     table = luigi.DictParameter()
#
#     def requires(self):
#         return LoadCSVtoBigQuery(self.table)
#
#     def run(self):
#         dataset_id = self.table.get('dataset_id')
#         table_id = self.table.get('table_id')
#         bucket_name = self.table.get('bucket_name')
#         blob_name = self.table.get('blob_name')
#
#         table_ref = bq_client.dataset(dataset_id).table(table_id)
#         destination_uri = f"gs://{bucket_name}/{blob_name}"
#         extract_job_config = bigquery.ExtractJobConfig()
#         extract_job_config.destination_format = bigquery.DestinationFormat.AVRO
#
#         extract_job = bq_client.extract_table(
#             table_ref,
#             destination_uri,
#             job_config=extract_job_config
#         )
#         extract_job.result()
#
#
# class RestoreTableFromAvro(luigi.Task):
#     table = luigi.DictParameter()
#
#     def requires(self):
#         return BackupTableToAvro(self.table)
#
#     def run(self):
#         dataset_id = self.table.get('dataset_id')
#         table_id = self.table.get('table_id')
#         bucket_name = self.table.get('bucket_name')
#         blob_name = self.table.get('blob_name')
#
#         dataset_ref = bq_client.dataset(dataset_id)
#         job_config = bigquery.LoadJobConfig()
#         job_config.source_format = bigquery.SourceFormat.AVRO
#
#         gcs_uri = f"gs://{bucket_name}/{blob_name}"
#         load_job = bq_client.load_table_from_uri(gcs_uri, dataset_ref.table(table_id), job_config=job_config)
#         load_job.result()


if __name__ == '__main__':
    _object = "departments-20230718"
    _source = "gdet-data-lake-001/hr/manual-uploads/departments-20230718.csv"
    _destination = "gdet-001.sdz.sdz_hr_departments"
    luigi.build([LoadCSVtoBigQuery(OBJECT=_object, SOURCE=_source, DESTINATION=_destination)], local_scheduler=True)
