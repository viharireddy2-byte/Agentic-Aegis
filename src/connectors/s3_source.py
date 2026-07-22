"""S3Source — reads a CSV or Parquet object out of an S3 (or S3-compatible)
bucket straight into a Polars DataFrame.

Requires ``boto3`` from ``requirements-extra.txt``. Credentials are
resolved the normal boto3 way (env vars, shared config, instance role) —
Aegis never asks for or stores AWS keys itself.
"""

from __future__ import annotations

import io
from typing import Any

import polars as pl

from .base import ConnectorError, DataSource

try:
    import boto3
except ImportError:  # pragma: no cover - optional dependency
    boto3 = None


class S3Source(DataSource):
    source_type = "s3"

    def __init__(self, bucket: str, key: str, file_format: str = "csv",
                 endpoint_url: str | None = None):
        file_format = file_format.lower()
        if file_format not in {"csv", "parquet"}:
            raise ValueError("S3Source file_format must be 'csv' or 'parquet'")
        self.bucket = bucket
        self.key = key
        self.file_format = file_format
        self.endpoint_url = endpoint_url  # set for MinIO / other S3-compatible stores

    def extract(self) -> pl.DataFrame:
        if boto3 is None:
            raise ConnectorError(
                "S3Source requires boto3. Install with: pip install -r requirements-extra.txt"
            )
        try:
            client = boto3.client("s3", endpoint_url=self.endpoint_url)
            obj = client.get_object(Bucket=self.bucket, Key=self.key)
            body = obj["Body"].read()
        except Exception as exc:  # noqa: BLE001 - re-raised as a ConnectorError
            raise ConnectorError(
                f"S3Source failed to read s3://{self.bucket}/{self.key}: {exc}"
            ) from exc

        buffer = io.BytesIO(body)
        if self.file_format == "csv":
            return pl.read_csv(buffer, try_parse_dates=True)
        return pl.read_parquet(buffer)

    def describe(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "bucket": self.bucket,
            "key": self.key,
            "file_format": self.file_format,
        }
