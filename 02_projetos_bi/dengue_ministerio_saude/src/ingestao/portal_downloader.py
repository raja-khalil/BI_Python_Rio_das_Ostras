"""Downloader de recursos de dengue no portal de dados abertos do SUS."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time
from typing import Any
from urllib.parse import urlparse
import zipfile

import requests

from src.config.settings import Settings
from src.utils.logger import get_logger


logger = get_logger(__name__)

RESOURCE_PATTERN = re.compile(r"dengue[^\d]*(\d{4})", flags=re.IGNORECASE)
SCRIPT_PATTERN = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    flags=re.DOTALL | re.IGNORECASE,
)


@dataclass(frozen=True)
class PortalResource:
    """Representa um arquivo anual disponível no portal."""

    resource_id: str
    year: int
    format: str
    url: str
    metadata_modified: str
    name: str

    @property
    def format_lower(self) -> str:
        return self.format.strip().lower()


class DenguePortalDownloader:
    """Sincroniza recursos de dengue (json/csv/xml) do portal SUS."""

    dataset_url = "https://dadosabertos.saude.gov.br/dataset/arboviroses-dengue"

    def __init__(self, settings: Settings, timeout_seconds: int | None = None) -> None:
        self.settings = settings
        self.timeout_seconds = timeout_seconds or settings.api_timeout_seconds
        self.session = requests.Session()
        self.manifest_path = settings.data_dir / "external" / "portal_sus_manifest.json"
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)

    def sync_once(
        self,
        formats: list[str],
        year_start: int,
        year_end: int,
        extract_zip: bool = True,
        keep_zip: bool = False,
        force: bool = False,
    ) -> dict[str, int]:
        """Sincroniza uma vez com filtros por formato e intervalo de ano."""
        allowed_formats = {f.strip().lower() for f in formats}
        resources = self._list_resources()
        manifest = self._load_manifest()

        downloaded = 0
        skipped = 0
        failed = 0

        for resource in resources:
            if resource.format_lower not in allowed_formats:
                continue
            if resource.year < year_start or resource.year > year_end:
                continue

            if not force and self._is_resource_unchanged(resource, manifest):
                skipped += 1
                continue

            try:
                output_file = self._download_resource(resource)
                if extract_zip and output_file.suffix.lower() == ".zip":
                    self._extract_zip(resource, output_file)
                    if not keep_zip and output_file.exists():
                        output_file.unlink()
                        logger.info("Zip removido apos extracao: %s", output_file)

                manifest[resource.resource_id] = {
                    "year": resource.year,
                    "format": resource.format_lower,
                    "name": resource.name,
                    "url": resource.url,
                    "metadata_modified": resource.metadata_modified,
                    "downloaded_at": datetime.now(timezone.utc).isoformat(),
                    "file_path": str(output_file),
                }
                downloaded += 1
            except Exception:
                logger.exception(
                    "Falha ao baixar recurso year=%s format=%s id=%s",
                    resource.year,
                    resource.format_lower,
                    resource.resource_id,
                )
                failed += 1

        self._save_manifest(manifest)
        logger.info(
            "Sincronizacao finalizada: downloaded=%s skipped=%s failed=%s",
            downloaded,
            skipped,
            failed,
        )
        return {"downloaded": downloaded, "skipped": skipped, "failed": failed}

    def sync_periodic(
        self,
        formats: list[str],
        year_start: int,
        year_end: int,
        interval_minutes: int,
        extract_zip: bool = True,
        keep_zip: bool = False,
    ) -> None:
        """Executa sincronizacao em loop periódico."""
        while True:
            self.sync_once(
                formats=formats,
                year_start=year_start,
                year_end=year_end,
                extract_zip=extract_zip,
                keep_zip=keep_zip,
            )
            time.sleep(max(1, interval_minutes) * 60)

    def _list_resources(self) -> list[PortalResource]:
        html = self._get_dataset_page_html()
        next_data = self._extract_next_data_json(html)
        raw_resources = next_data.get("props", {}).get("pageProps", {}).get("resources", [])

        resources: list[PortalResource] = []
        for item in raw_resources:
            name = str(item.get("name", "")).strip()
            normalized_name = self._normalize_name(name)
            match = RESOURCE_PATTERN.search(normalized_name)
            if not match:
                continue

            resource_id = str(item.get("id", "")).strip()
            url = str(item.get("url", "")).strip()
            fmt = str(item.get("format", "")).strip()
            metadata_modified = str(item.get("metadata_modified", "")).strip()
            if not resource_id or not url or not fmt:
                continue

            resources.append(
                PortalResource(
                    resource_id=resource_id,
                    year=int(match.group(1)),
                    format=fmt,
                    url=url,
                    metadata_modified=metadata_modified,
                    name=name,
                )
            )

        return sorted(resources, key=lambda r: (r.year, r.format_lower))

    @staticmethod
    def _normalize_name(value: str) -> str:
        normalized = value.replace("\u00a0", " ").replace("–", "-").replace("—", "-")
        return re.sub(r"\s+", " ", normalized).strip()

    def _get_dataset_page_html(self) -> str:
        response = self.session.get(self.dataset_url, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.text

    def _extract_next_data_json(self, html: str) -> dict[str, Any]:
        match = SCRIPT_PATTERN.search(html)
        if not match:
            raise RuntimeError("Nao foi possivel localizar __NEXT_DATA__ na pagina do dataset")
        return json.loads(match.group(1))

    def _download_resource(self, resource: PortalResource) -> Path:
        extension = self._infer_extension(resource.url)
        target_dir = self._target_dir_for_format(resource.format_lower)
        target_dir.mkdir(parents=True, exist_ok=True)

        date_stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        file_name = f"dengue_{resource.year}_{date_stamp}.{extension}"
        target_path = target_dir / file_name

        logger.info("Baixando %s -> %s", resource.url, target_path)
        try:
            with self.session.get(resource.url, stream=True, timeout=self.timeout_seconds) as response:
                response.raise_for_status()
                with target_path.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            output.write(chunk)
        except Exception:
            if target_path.exists():
                try:
                    target_path.unlink()
                    logger.warning("Arquivo parcial removido: %s", target_path)
                except OSError:
                    logger.warning("Nao foi possivel remover arquivo parcial: %s", target_path)
            raise

        return target_path

    def _extract_zip(self, resource: PortalResource, zip_path: Path) -> None:
        extract_dir = self._target_dir_for_format(resource.format_lower) / "extracted" / str(resource.year)
        extract_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Extraindo zip %s -> %s", zip_path, extract_dir)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

    def _target_dir_for_format(self, fmt: str) -> Path:
        if fmt == "json":
            return self.settings.raw_dir / "json" / "portal_sus"
        if fmt == "csv":
            return self.settings.raw_dir / "csv" / "portal_sus"
        if fmt == "xml":
            return self.settings.raw_dir / "xml" / "portal_sus"
        return self.settings.raw_dir / "external" / "portal_sus"

    @staticmethod
    def _infer_extension(url: str) -> str:
        path = urlparse(url).path.lower()
        suffixes = Path(path).suffixes
        if not suffixes:
            return "bin"
        return ".".join(s.lstrip(".") for s in suffixes)

    def _load_manifest(self) -> dict[str, dict[str, Any]]:
        if not self.manifest_path.exists():
            return {}
        try:
            with self.manifest_path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except (json.JSONDecodeError, OSError):
            logger.warning("Manifesto invalido. Recriando: %s", self.manifest_path)
            return {}
        return data if isinstance(data, dict) else {}

    def _save_manifest(self, manifest: dict[str, dict[str, Any]]) -> None:
        with self.manifest_path.open("w", encoding="utf-8") as file:
            json.dump(manifest, file, ensure_ascii=False, indent=2)

    @staticmethod
    def _is_resource_unchanged(resource: PortalResource, manifest: dict[str, dict[str, Any]]) -> bool:
        old = manifest.get(resource.resource_id)
        if not old:
            return False
        return old.get("metadata_modified") == resource.metadata_modified
