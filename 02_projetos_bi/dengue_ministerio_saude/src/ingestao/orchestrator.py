"""Orquestrador de ingestao com suporte multipla fonte."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from src.config.settings import Settings
from src.ingestao.api_client import ArbovirosesDengueApiClient, BaseApiClient
from src.ingestao.reader_csv import read_csv_file
from src.ingestao.reader_json import read_json_file
from src.ingestao.reader_xml import read_xml_file
from src.utils.logger import get_logger


logger = get_logger(__name__)

SourceType = Literal["api", "json", "csv", "xml"]


@dataclass
class IngestionRequest:
    """Parametros de ingestao."""

    source_type: SourceType
    source_identifier: str
    historical: bool = True
    incremental_key: str | None = None
    start_year: int | None = None
    end_year: int | None = None
    since_date: date | None = None
    api_limit: int | None = None
    api_max_pages_per_year: int | None = None
    api_params: dict[str, Any] | None = None


class IngestionOrchestrator:
    """Coordena ingestao historica e incremental por tipo de fonte."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.api_client = BaseApiClient(
            base_url=settings.api_base_url,
            timeout_seconds=settings.api_timeout_seconds,
            max_retries=settings.api_max_retries,
            backoff_seconds=settings.api_backoff_seconds,
        )
        self.dengue_api_client = ArbovirosesDengueApiClient.from_settings()

    def run(self, request: IngestionRequest) -> pd.DataFrame:
        """Executa ingestao e retorna DataFrame bruto."""
        logger.info(
            "Executando ingestao. source_type=%s source_identifier=%s historical=%s",
            request.source_type,
            request.source_identifier,
            request.historical,
        )

        df = self._dispatch_reader(request)

        self._save_raw_snapshot(df=df, source_type=request.source_type, source=request.source_identifier)

        if not request.historical and request.incremental_key and request.incremental_key in df.columns:
            # Placeholder para filtro incremental real com base na ultima carga processada.
            df = df.sort_values(by=request.incremental_key)

        return df

    def _dispatch_reader(self, request: IngestionRequest) -> pd.DataFrame:
        if request.source_type == "api":
            if request.source_identifier in {"dengue", "arboviroses_dengue", "casos_dengue"}:
                return self._ingest_dengue_api(request)

            payload = self.api_client.get_json(
                endpoint=request.source_identifier,
                params=request.api_params,
                with_auth=False,
            )
            if isinstance(payload, dict):
                return pd.json_normalize(payload)
            return pd.json_normalize(payload)

        path = Path(request.source_identifier)

        if request.source_type == "json":
            data = read_json_file(path=path, normalize=True)
            return data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)

        if request.source_type == "csv":
            return read_csv_file(path=path)

        if request.source_type == "xml":
            return read_xml_file(path=path)

        raise ValueError(f"Tipo de fonte nao suportado: {request.source_type}")

    def _ingest_dengue_api(self, request: IngestionRequest) -> pd.DataFrame:
        """Ingestao da API de arboviroses dengue com paginação e incremental."""
        current_year = datetime.now(tz=timezone.utc).year
        default_start_year = 2000 if request.historical else current_year

        start_year = request.start_year or default_start_year
        end_year = request.end_year or current_year

        if start_year > end_year:
            raise ValueError("start_year nao pode ser maior que end_year")

        limit = request.api_limit or self.settings.api_page_size
        max_pages = request.api_max_pages_per_year or self.settings.api_max_pages_per_year

        records: list[dict[str, Any]] = []
        for year in range(start_year, end_year + 1):
            logger.info("Coletando dengue API para nu_ano=%s", year)
            year_rows = self.dengue_api_client.fetch_year(
                year=year,
                limit=limit,
                max_pages=max_pages,
                since_date=request.since_date,
            )
            for row in year_rows:
                row["_nu_ano_coleta"] = str(year)
            records.extend(year_rows)

        if not records:
            return pd.DataFrame()

        return pd.DataFrame(records)

    def _save_raw_snapshot(self, df: pd.DataFrame, source_type: SourceType, source: str) -> None:
        """Persistencia de snapshot bruto para rastreabilidade."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{Path(source).stem}_{timestamp}.parquet"
        target_dir = self.settings.raw_dir / source_type
        target_dir.mkdir(parents=True, exist_ok=True)

        output_path = target_dir / filename
        df.to_parquet(output_path, index=False)
        logger.info("Snapshot bruto salvo em %s", output_path)
