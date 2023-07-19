"""
Title: custom_luigi.py

Created on: 2021-09-23

Author: FriscianViales

Encoding: UTF-8

Description: Custom Luigi components.
"""

# Import libraries:
from datetime import datetime
from pytz import timezone
import luigi
import pandas as pd
import requests
import os


# Global Vars:
LUIGI_SLACK_CHANNEL = "NA"


class CustomTask(luigi.Task):
    """ Custom version of a Luigi Task. """
    EXEC_TS = datetime.now(timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")

    def write_csv(self, df: pd.DataFrame):
        with self.output().open('w') as csv:
            csv.write(df.to_csv(index=False, lineterminator='\n'))

    def write_txt(self, text='DONE'):
        with self.output().open('w') as checkpoint:
            checkpoint.write(text)

    def read_csv_input(self, single_input: bool = True, input_key: str = None, **kwargs):
        if single_input:
            with self.input().open('r') as csv:
                return pd.read_csv(csv, low_memory=kwargs.get("low_memory"))
        else:
            assert type(self.input()) == dict, "Your input task is not a dictionary."
            assert input_key is not None, "The input key name has to be a valid string."
            with self.input().get(input_key).open('r') as csv:
                return pd.read_csv(csv, low_memory=kwargs.get("low_memory"))

    def read_txt_input(self):
        with self.input().open("r") as txt:
            return txt.read()

    def on_failure(self, exception):
        self.notify_slack_channel(exception)

    def notify_slack_channel(self, exception):
        with requests.Session() as sess:
            response = sess.post(url=LUIGI_SLACK_CHANNEL, json=self.get_slack_payload(exception))
            status = response.status_code
            return status

    def get_slack_payload(self, exception: ValueError) -> dict:
        a = f"Hey <@friscian.viales>; Something went wrong with a task:\n"
        b = f"```Task Name:\t{self.get_task_family()}\nFile Loc:\t{os.getcwd()}```\n"
        c = f"```ErrorType: {exception.__class__}\nTraceback:\t{exception}```\n\n"
        return {
            "text": f"{a}{b}{c}"
        }

    def add_exec_date_to_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df["__exec_ts__"] = self.EXEC_TS
        df["__exec_ts__"] = pd.to_datetime(df["__exec_ts__"])
        return df


class CustomExternalTask(luigi.ExternalTask):
    """ Custom version of a Luigi External Task. """
    EXEC_TS = datetime.now(timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")

    def write_csv(self, df: pd.DataFrame):
        with self.output().open('w') as csv:
            csv.write(df.to_csv(index=False, lineterminator='\n'))

    def write_txt(self, text: str = 'DONE'):
        with self.output().open('w') as checkpoint:
            checkpoint.write(text)

    def on_failure(self, exception):
        self.notify_slack_channel(exception)

    def notify_slack_channel(self, exception):
        with requests.Session() as sess:
            response = sess.post(url=LUIGI_SLACK_CHANNEL, json=self.get_slack_payload(exception))
            status = response.status_code
            return status

    def get_slack_payload(self, exception: ValueError) -> dict:
        a = f"Hey <@friscian.viales>; Something went wrong with a task:\n"
        b = f"```Task Name:\t{self.get_task_family()}\nFile Loc:\t{os.getcwd()}```\n"
        c = f"```ErrorType: {exception.__class__}\nTraceback:\t{exception}```\n\n"
        return {
            "text": f"{a}{b}{c}"
        }

    def add_exec_date_to_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df["__exec_ts__"] = self.EXEC_TS
        df["__exec_ts__"] = pd.to_datetime(df["__exec_ts__"])
        return df