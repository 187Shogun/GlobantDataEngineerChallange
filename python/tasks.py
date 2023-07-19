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
from datetime import datetime


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\alche\PycharmProjects\GlobantDataEngineerChallange\gdet-001-keys.json'
bq_client = bigquery.Client()
ts = datetime.now().date()


class LoadCSVtoBigQuery(CustomExternalTask):
    """
    Load a CSV file from GCS into a BigQuery table.

    Args:
        OBJECT (str): The name of the CSV file in GCS.

    Output:
        luigi.LocalTarget: A file containing the output of the BigQuery job.
    """
    OBJECT = luigi.Parameter()
    SOURCE = "gs://gdet-data-lake-001/hr/manual-uploads/"
    DESTINATION = "gdet-001.sdz.sdz_hr_"

    def output(self):
        return luigi.LocalTarget(f"tmp/{self.get_task_family()}-{self.OBJECT}.txt")

    def run(self):
        """
        Submit a BigQuery job to load the CSV file into a table.

        Logging is used to track the progress of the job.
        """
        logging.info(f"Submitting BatchLoad job from: {self.SOURCE}{self.OBJECT} to: {self.DESTINATION}{self.OBJECT}")
        table = bigquery.Table(f"{self.DESTINATION}{self.OBJECT}", schema=self.get_json_schema())
        _ = bq_client.create_table(table, exists_ok=True)
        job = self.submit_job()
        self.write_txt(str(job))

    def submit_job(self):
        """
        Submit a BigQuery job to load the CSV file into a table.

        Returns:
            The result of the BigQuery job as a dictionary.
        """
        load_job = bigquery.Client().load_table_from_uri(
            source_uris=f"{self.SOURCE}{self.OBJECT}.csv",
            destination=f"{self.DESTINATION}{self.OBJECT}",
            job_config=self.job_config()
        )
        return load_job.result().__dict__

    def job_config(self):
        """
        Configure the BigQuery job.

        Returns:
            The BigQuery job configuration as a dictionary.
        """
        return bigquery.LoadJobConfig(
            allow_quoted_newlines=True,
            create_disposition='CREATE_IF_NEEDED',
            encoding='UTF-8',
            source_format=bigquery.SourceFormat.CSV,
            write_disposition='WRITE_TRUNCATE',
            schema=self.get_json_schema()
        )

    def get_json_schema(self) -> list:
        """
        Load the JSON schema from a file.

        Returns:
            The JSON schema as a list.
        """
        with open(f"schemas/{self.OBJECT}.json", "r") as f:
            return json.loads(f.read())


class LoadHRObjectsToBigQuery(luigi.WrapperTask):
    """
    Wrapper to load all tables from GCS to BigQuery.

    Args:
        OBJECTS (list): A list of the names of the tables to load.

    Output:
        None.
    """
    OBJECTS = ['departments', 'jobs', 'hired_employees']

    def requires(self):
        """
        Iterate over the list of objects and yield a `LoadCSVtoBigQuery` task for each object.

        Returns:
            None.
        """
        for i in self.OBJECTS:
            yield LoadCSVtoBigQuery(OBJECT=i)


class BackupTableToAvro(CustomExternalTask):
    """
    Backup a BigQuery table to Avro. Inherits from `CustomExternalTask` and requires `luigi.Parameter`.
    """
    TABLE_ID = luigi.Parameter()
    DESTINATION = "gs://gdet-data-lake-001/hr/backups/"

    def output(self):
        """
        Define the output for Luigi to check for task completion.

        Returns:
            luigi.LocalTarget: Local path where the task result is stored.
        """
        table_name = str(self.TABLE_ID).split('.')[-1]
        return luigi.LocalTarget(f"tmp/{self.get_task_family()}-{table_name}.txt")

    def run(self):
        """
        Code to backup the BigQuery table to Avro format.

        Raises:
            Exception: If the BigQuery job fails.
        """
        table_name = str(self.TABLE_ID).split('.')[-1]
        extract_job_config = bigquery.ExtractJobConfig()
        extract_job_config.destination_format = bigquery.DestinationFormat.AVRO
        extract_job = bq_client.extract_table(
            f"{self.TABLE_ID}",
            f"{self.DESTINATION}{table_name}-{ts}",
            job_config=extract_job_config
        )
        job = extract_job.result()
        self.write_txt(str(job))


class BackupHRTablesToAVRO(luigi.WrapperTask):
    """
    Luigi wrapper task to backup multiple BigQuery tables to Avro format.
    """
    TABLES = ['departments', 'jobs', 'hired_employees']

    def requires(self):
        """
        Task dependencies. Each table in the TABLES list is backed up.

        Yields:
            object: BackupTableToAvro task for each table in the TABLES list.
        """
        for i in self.TABLES:
            yield BackupTableToAvro(TABLE_ID=f"gdet-001.sdz.sdz_hr_{i}")


class RestoreTableFromAvro(CustomExternalTask):
    """
    Restore a BigQuery table from Avro format. Inherits from `CustomExternalTask` and requires `luigi.Parameter`.
    """
    TABLE_NAME = luigi.Parameter()
    SOURCE = "gs://gdet-data-lake-001/hr/backups/"
    DESTINATION = "gdet-001.sdz.sdz_hr_"

    def output(self):
        """
        Define the output for Luigi to check for task completion.

        Returns:
            luigi.LocalTarget: Local path where the task result is stored.
        """
        table_name = self.TABLE_NAME
        return luigi.LocalTarget(f"tmp/{self.get_task_family()}-{table_name}.txt")

    def run(self):
        """
        Code to restore the BigQuery table from Avro format.

        Raises:
            Exception: If the BigQuery job fails.
        """
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.AVRO
        gcs_uri = f"{self.SOURCE}{'sdz_hr_'}{self.TABLE_NAME}-{ts}"
        table_id = f"{self.DESTINATION}{self.TABLE_NAME}"
        load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        job = load_job.result()
        self.write_txt(str(job))


class RestoreHRTablesFromAVRO(luigi.WrapperTask):
    """
    Luigi wrapper task to restore multiple BigQuery tables from Avro format.
    """
    TABLES = ['departments', 'jobs', 'hired_employees']

    def requires(self):
        """
        Task dependencies. Each table in the TABLES list is restored.

        Yields:
            object: RestoreTableFromAvro task for each table in the TABLES list.
        """
        for i in self.TABLES:
            yield RestoreTableFromAvro(TABLE_NAME=i)


if __name__ == '__main__':
    luigi.build([RestoreHRTablesFromAVRO()], local_scheduler=True)
