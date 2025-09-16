"""
InfluxDB v2 / Flux collector (drop-in replacement for the old v1 version).
- Uses the v2 Python client (influxdb_client).
- Computes experiment IDs live from Flux (no stale startup cache).
- Reads Swarm secret at /run/secrets/analytics_connections by default.

Expected secret YAML (example):
  uma:
    url: http://influxdb:8086
    org: uma-org
    bucket: testdb
    token: <REAL_V2_TOKEN>
"""

from __future__ import annotations

from os import environ
from typing import Iterable, Optional, Dict, Any

import yaml
import pandas as pd
from influxdb_client import InfluxDBClient  # v2 client


# ---------------------------- secrets ----------------------------

def _load_secret(path: Optional[str] = None) -> Dict[str, Any]:
    """
    Read the Swarm secret YAML. If the top-level has a key like "uma",
    return that nested object; otherwise return the top-level mapping.
    """
    path = path or environ.get("ANALYTICS_CONN_FILE", "/run/secrets/analytics_connections")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    # most stacks use { "uma": {url,org,bucket,token} }
    return data.get("uma", data)


# ---------------------------- collector ----------------------------

class DataCollector:
    """
    InfluxDB v2 collector that replaces the old v1 client.

    Typical usage inside the container:
        dc = DataCollector.from_secret()

    Back-compat note:
      The old constructor was DataCollector(host, port, user, password, database)
      For v2, user/password are ignored; "database" maps to "bucket".
      You *must* supply org+token (via kwargs) or use from_secret().
    """

    # ---- preferred constructor

    @classmethod
    def from_secret(cls, secret_path: Optional[str] = None) -> "DataCollector":
        cfg = _load_secret(secret_path)
        return cls(url=cfg["url"], org=cfg["org"], bucket=cfg["bucket"], token=cfg["token"])

    # ---- init

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[str] = None,
        user: Optional[str] = None,       # ignored (v1 relic)
        password: Optional[str] = None,   # ignored (v1 relic)
        database: Optional[str] = None,   # maps to v2 bucket if provided
        *,
        url: Optional[str] = None,
        org: Optional[str] = None,
        bucket: Optional[str] = None,
        token: Optional[str] = None,
        secret_path: Optional[str] = None,
        timeout_ms: int = 30000,
    ):
        # If any core setting missing, try secret
        if not (url and org and bucket and token):
            try:
                cfg = _load_secret(secret_path)
                url = url or cfg.get("url")
                org = org or cfg.get("org")
                bucket = bucket or cfg.get("bucket")
                token = token or cfg.get("token")
            except FileNotFoundError:
                pass

        # Fallback: allow host+port to build URL
        if not url and host and port:
            url = f"http://{host}:{port}"

        # Map old "database" arg to v2 "bucket"
        if database and not bucket:
            bucket = database

        # Validate
        missing = [k for k, v in dict(url=url, org=org, bucket=bucket, token=token).items() if not v]
        if missing:
            raise ValueError(
                f"Missing Influx v2 settings: {', '.join(missing)}. "
                f"Provide url/org/bucket/token or call DataCollector.from_secret()."
            )

        # Save + connect
        self.url = url
        self.org = org
        self.bucket = bucket
        self.token = token

        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=timeout_ms)
        self.query_api = self.client.query_api()

        # Best-effort snapshot (public API computes live):
        try:
            self.experimentIds = self.get_all_experimentIds()
        except Exception:
            self.experimentIds = []

    # ---------------------------- public API ----------------------------

    def get_data(
        self,
        experimentId: str,
        measurements: Iterable[str] | None = None,
        fields: Iterable[str] | None = None,
        additional_clause: Optional[str] = None,  # kept for compatibility; ignored in v2
        chunked: bool = False,                    # ignored
        chunk_size: int = 10000,                  # ignored
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        max_lag: str = "1s",
        start: str = "-30d",
    ) -> pd.DataFrame:
        """
        Fetch time series rows filtered by ExperimentId/ExecutionId.
        - If no measurements passed, discover them for this experimentId.
        - If fields passed, filter to those _field names.
        - Align to 'max_lag' using aggregateWindow(mean), then pivot.
        Returns a DataFrame indexed by time; columns are fields.
        """
        if not measurements:
            measurements = self.get_measurements_for_experimentId(experimentId, start=start)

        dfs = []
        for m in measurements:
            flux = self._flux_base(start=start)
            flux += f'  |> filter(fn: (r) => r._measurement == "{m}")\n'
            flux += f'  |> filter(fn: (r) => r.ExperimentId == "{experimentId}" or r.ExecutionId == "{experimentId}")\n'

            if fields:
                fields_list = ", ".join([f'"{f}"' for f in fields])
                flux += f"  |> filter(fn: (r) => contains(value: r._field, set: [{fields_list}]))\n"

            # bucket to max_lag and pivot to wide format
            flux += f'  |> aggregateWindow(every: duration(v: "{max_lag}"), fn: mean, createEmpty: false)\n'
            flux += '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'

            if limit is not None:
                flux += f"  |> limit(n: {int(limit)}"
                if offset is not None:
                    flux += f", offset: {int(offset)}"
                flux += ")\n"

            df = self._query_df(flux)
            if not df.empty:
                # set time index, drop helper cols
                if "_time" in df.columns:
                    df = df.set_index(pd.to_datetime(df["_time"]))
                for c in ("_time", "_start", "_stop", "result", "table"):
                    if c in df.columns:
                        df = df.drop(columns=[c])
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        out = pd.concat(dfs, axis=1).sort_index()
        return out

    def get_experimentIds_for_measurement(self, measurement: str, start: str = "-90d") -> list[str]:
        """
        Distinct experiment IDs for a specific measurement (live).
        Uses schema.tagValues() so values are always in the _value column.
        """
        ids: set[str] = set()
        for tag in ("ExperimentId", "ExecutionId"):
            flux = f'''
import "influxdata/influxdb/schema"
schema.tagValues(
  bucket: "{self.bucket}",
  tag: "{tag}",
  predicate: (r) => r._measurement == "{measurement}"
)
'''
            df = self._query_df(flux)
            if not df.empty and "_value" in df.columns:
                ids |= set(df["_value"].dropna().astype(str))
        return sorted(ids)

    def get_measurements_for_experimentId(self, experimentId: str, start: str = "-90d") -> list[str]:
        """
        List measurements that contain rows for the given experimentId.
        """
        flux = self._flux_base(start=start) + f'''  |> filter(fn: (r) => r.ExperimentId == "{experimentId}" or r.ExecutionId == "{experimentId}")
  |> keep(columns: ["_measurement"])
  |> group()
  |> distinct(column: "_measurement")
'''
        df = self._query_df(flux)
        if df.empty:
            return []
        col = "_measurement" if "_measurement" in df.columns else "_value"
        return sorted(df[col].dropna().astype(str).unique().tolist())

    def get_all_experimentIds(self, start: str = "-90d") -> list[str]:
        """
        Union of distinct IDs across the entire bucket (computed live).
        Uses schema.tagValues() so results are read from _value.
        """
        ids: set[str] = set()
        for tag in ("ExperimentId", "ExecutionId"):
            flux = f'''
import "influxdata/influxdb/schema"
schema.tagValues(bucket: "{self.bucket}", tag: "{tag}")
'''
            df = self._query_df(flux)
            if not df.empty and "_value" in df.columns:
                ids |= set(df["_value"].dropna().astype(str))
        return sorted(ids)

    # ---------------------------- internals ----------------------------

    def _flux_base(self, start: str) -> str:
        return f'from(bucket: "{self.bucket}")\n  |> range(start: {start})\n'

    def _query_df(self, flux: str) -> pd.DataFrame:
        """
        Run Flux and return a single pandas DataFrame.
        query_data_frame may return a DataFrame or a list of DataFrames.
        """
        tables = self.query_api.query_data_frame(flux, org=self.org)
        if isinstance(tables, list):
            tables = [t for t in tables if isinstance(t, pd.DataFrame) and not t.empty]
            return pd.concat(tables, ignore_index=True) if tables else pd.DataFrame()
        return tables if isinstance(tables, pd.DataFrame) else pd.DataFrame()

    # Old v1 name (explicitly unsupported on v2)
    def query_df(self, query: str) -> pd.DataFrame:
        raise NotImplementedError("InfluxQL is not supported on v2; use Flux via get_data()/get_* helpers.")

    # Compatibility alias some code might call
    def query_dataframe(self, *args, **kwargs) -> pd.DataFrame:
        return self.get_data(*args, **kwargs)


if __name__ == "__main__":
    # Quick CLI sanity check (only works if the secret exists in this environment)
    dc = DataCollector.from_secret()
    print("URL:", dc.url, "ORG:", dc.org, "BUCKET:", dc.bucket)
    print("Experiment IDs (sample):", dc.get_all_experimentIds()[:10])
