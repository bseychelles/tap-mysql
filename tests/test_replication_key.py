"""Tests standard tap features using the built-in SDK tests library."""
# flake8: noqa

import json

import datetime
import sqlalchemy
from singer_sdk.testing.templates import TapTestTemplate
from singer_sdk._singerlib import Catalog
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, text

from tap_mysql.tap import TapMySQL

TABLE_NAME = "test_replication_key"
SAMPLE_CONFIG = {
    "start_date": datetime.datetime(2022, 11, 1).isoformat(),
    # Using 127.0.0.1 instead of localhost because of mysqlclient dialect.
    # See: https://stackoverflow.com/questions/72294279/how-to-connect-to-mysql-databas-using-github-actions
    "sqlalchemy_url": "mysql+pymysql://root:password@127.0.0.1:3306/melty",
}


def replication_key_test(tap, table_name):
    """Originally built to address
    https://github.com/meltano/sdk/issues/1268.
    """
    tap.run_discovery()
    catalog = Catalog.from_dict({"streams": tap.catalog_dict["streams"]})

    for stream in catalog.streams:
        if table_name not in stream.tap_stream_id:
            stream.metadata.root.selected = False
        else:
            stream.metadata.root.selected = True
            stream.metadata.root.forced_replication_method = "INCREMENTAL"
            stream.replication_key = "updated_at"
            stream.metadata.root.replication_key = "updated_at"

    tap = TapMySQL(config=SAMPLE_CONFIG, catalog=catalog.to_dict())
    tap.sync_all()


def test_synthetic_replication_key_cursor_ts():
    """Ensure cursor_ts can drive incremental extraction using COALESCE."""
    table_name = "test_synthetic_replication_key"
    engine = sqlalchemy.create_engine(SAMPLE_CONFIG["sqlalchemy_url"])

    metadata_obj = MetaData()
    table = Table(
        table_name,
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("created_at", DateTime(), nullable=False),
        Column("updated_at", DateTime(), nullable=True),
        Column("name", String(length=100)),
    )

    created_only_ts = datetime.datetime(2022, 11, 5, 8, 0, 0)
    updated_ts = datetime.datetime(2022, 11, 20, 12, 30, 0)

    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            metadata_obj.create_all(conn)
            conn.execute(
                table.insert(),
                [
                    {
                        "id": 1,
                        "created_at": created_only_ts,
                        "updated_at": None,
                        "name": "new-row",
                    },
                    {
                        "id": 2,
                        "created_at": datetime.datetime(2022, 11, 6, 9, 0, 0),
                        "updated_at": updated_ts,
                        "name": "updated-row",
                    },
                ],
            )

    tap = TapMySQL(config=SAMPLE_CONFIG)
    catalog = Catalog.from_dict({"streams": tap.catalog_dict["streams"]})

    for stream in catalog.streams:
        if table_name not in stream.tap_stream_id:
            stream.metadata.root.selected = False
        else:
            stream.metadata.root.selected = True
            stream.metadata.root.forced_replication_method = "INCREMENTAL"
            stream.replication_key = "cursor_ts"
            stream.metadata.root.replication_key = "cursor_ts"

    tap = TapMySQL(config=SAMPLE_CONFIG, catalog=catalog.to_dict())
    selected_stream = next(s for s in tap.discover_streams() if s.selected is True)
    selected_stream.replication_key = "cursor_ts"

    assert "cursor_ts" in selected_stream.get_selected_schema()["properties"]

    records = list(selected_stream.get_records(context=None))
    assert [record["id"] for record in records] == [1, 2]
    assert records[0]["cursor_ts"] == created_only_ts
    assert records[1]["cursor_ts"] == updated_ts

    state = {
        "bookmarks": {
            selected_stream.tap_stream_id: {
                "replication_key": "cursor_ts",
                "replication_key_value": datetime.datetime(
                    2022,
                    11,
                    10,
                    0,
                    0,
                    0,
                ).isoformat(),
            },
        },
    }
    incremental_tap = TapMySQL(
        config=SAMPLE_CONFIG, catalog=catalog.to_dict(), state=state
    )
    filtered_stream = next(
        s for s in incremental_tap.discover_streams() if s.selected is True
    )
    filtered_stream.replication_key = "cursor_ts"
    filtered_stream._write_starting_replication_value(None)
    filtered_records = list(filtered_stream.get_records(context=None))

    assert [record["id"] for record in filtered_records] == [2]

    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))


class TapTestReplicationKey(TapTestTemplate):
    """Test class for tap replication key tests."""

    name = "replication_key"
    table_name = TABLE_NAME

    def test(self):
        """Run the replication key test."""
        replication_key_test(self.tap, self.table_name)
