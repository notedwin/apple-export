import logging
import os
import re
from collections import defaultdict
from typing import Dict, Optional
from xml.etree.ElementTree import iterparse
from zipfile import ZipFile

import click
import duckdb
import pandas as pd
from pydantic import BaseModel
from sqlalchemy import create_engine

# logging.basicConfig(level=logging.DEBUG)

HEALTH_FILE = "./apple_health_export/export.xml"
LEADING_IDEN = re.compile(
    "HKQuantityTypeIdentifier|HKCategoryTypeIdentifier|HKDataType"
)


@click.command()
@click.option(
    "--health-file",
    default=os.path.expanduser("~/Downloads/export.zip"),
    help="Apple health export archive location",
)
@click.option("--duckdb-file", help="Location of duckdb export location")
@click.option(
    "--postgres-url", help="postgres URL where apple health should be exported to"
)
@click.option("--min-rows", default=0, help="minimum number of rows to create a table")
def main(health_file, duckdb_file, postgres_url, min_rows):
    HealthExport(
        postgres_url=postgres_url if postgres_url else None,
        duckdb_file=duckdb_file if duckdb else None,
        health_archive=health_file,
        min_rows=min_rows,
    ).run_etl()


class HealthExport(BaseModel):
    postgres_url: Optional[str]
    duckdb_file: Optional[str]
    health_archive: str
    min_rows: int
    _records: Dict = defaultdict(list)

    def extract_archive(self):
        with ZipFile(self.health_archive, "r") as zip_ref:
            logging.debug(f"extracting archive: {self.health_archive}")
            zip_ref.extractall()

    def transform_records(self):
        for num, elem in iterparse(HEALTH_FILE):
            if elem.tag == "Record":
                record = elem.attrib
                type = record.get("type")
                entries_to_remove = ("type", "sourceName", "sourceVersion", "device")
                for k in entries_to_remove:
                    record.pop(k, None)
                self._records[type].append(record)
        logging.debug(
            f"{len(self._records.keys())} Total tables extracted from {HEALTH_FILE}"
        )

    def load_duckdb(self):
        CON = duckdb.connect(self.duckdb_file)
        for name, array in self._records.items():
            if len(array) < self.min_rows:
                continue
            df = pd.DataFrame(array)
            table_name = LEADING_IDEN.sub("", name)
            logging.debug(f"Duckdb: Loading in {table_name} with {len(array)} elements")
            CON.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    def load_postgres(self):
        with create_engine(self.postgres_url).connect() as c:
            for name, array in self._records.items():
                if len(array) < self.min_rows:
                    continue
                df = pd.DataFrame(array)
                table_name = LEADING_IDEN.sub("", name)
                df.to_sql(
                    table_name,
                    con=c,
                    if_exists="replace",
                    index=False,
                    chunksize=1000,
                    method="multi",
                )

    def run_etl(self):
        logging.debug(f"options: {self}")
        self.extract_archive()
        self.transform_records()
        if self.duckdb_file:
            self.load_duckdb()
        if self.postgres_url:
            self.load_postgres()
