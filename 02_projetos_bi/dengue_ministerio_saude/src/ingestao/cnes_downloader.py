"""Downloader da base CNES (arquivo mais recente)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import time
from typing import Any
import zipfile

import requests

from src.config.settings import Settings
from src.utils.logger import get_logger


logger = get_logger(__name__)

CNES_FILE_PATTERN = re.compile(r"BASE_DE_DADOS_CNES_(\d{6})\.ZIP$", flags=re.IGNORECASE)


@dataclass(frozen=True)
class CnesResource:
    """Representa um arquivo CNES disponivel para download."""

    file_name: str
    period_yyyymm: str
    url: str


class CnesDownloader:
    """Sincroniza apenas o arquivo CNES mais recente."""

    base_page_url = "https://cnes.datasus.gov.br/pages/downloads/arquivosBaseDados.jsp"
    list_url = "https://cnes.datasus.gov.br/services/arquivos-download/base-dados/"
    download_url_template = "https://cnes.datasus.gov.br/EstatisticasServlet?path={file_name}"

    def __init__(self, settings: Settings, timeout_seconds: int | None = None) -> None:
        self.settings = settings
        self.timeout_seconds = timeout_seconds or settings.api_timeout_seconds
        self.session = requests.Session()
        self.manifest_path = settings.data_dir / "external" / "cnes_manifest.json"
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)

    def sync_once(
        self,
        extract_zip: bool = True,
        keep_zip: bool = False,
        check_interval_days: int = 40,
        force: bool = False,
    ) -> dict[str, Any]:
        """Sincroniza uma vez respeitando o intervalo de verificacao."""
        manifest = self._load_manifest()

        if not force and not self._should_check_now(manifest, check_interval_days):
            next_check_at = self._next_check_at(manifest, check_interval_days)
            logger.info(
                "Verificacao CNES ignorada. Proxima checagem apos: %s",
                next_check_at.isoformat() if next_check_at else "desconhecido",
            )
            return {"checked": False, "downloaded": 0, "skipped": 1, "failed": 0}

        latest = self._get_latest_resource()
        manifest_meta = manifest.setdefault("_meta", {})
        manifest_meta["last_checked_at"] = datetime.now(timezone.utc).isoformat()

        if not force and self._is_unchanged(latest, manifest):
            logger.info("Arquivo mais recente ja baixado: %s", latest.file_name)
            self._save_manifest(manifest)
            return {"checked": True, "downloaded": 0, "skipped": 1, "failed": 0}

        try:
            output_file = self._download_resource(latest)
            extracted_dir = None
            if extract_zip and output_file.suffix.lower() == ".zip":
                extracted_dir = self._extract_zip(latest, output_file)
                if not keep_zip and output_file.exists():
                    output_file.unlink()
                    logger.info("Zip removido apos extracao: %s", output_file)

            manifest["latest"] = {
                "file_name": latest.file_name,
                "period_yyyymm": latest.period_yyyymm,
                "url": latest.url,
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "zip_path": str(output_file),
                "extracted_dir": str(extracted_dir) if extracted_dir else None,
            }
            self._save_manifest(manifest)
            logger.info("Sincronizacao CNES concluida: arquivo=%s", latest.file_name)
            return {"checked": True, "downloaded": 1, "skipped": 0, "failed": 0}
        except Exception:
            logger.exception("Falha ao baixar arquivo mais recente do CNES")
            self._save_manifest(manifest)
            return {"checked": True, "downloaded": 0, "skipped": 0, "failed": 1}

    def sync_periodic(
        self,
        interval_minutes: int = 60,
        extract_zip: bool = True,
        keep_zip: bool = False,
        check_interval_days: int = 40,
    ) -> None:
        """Executa sincronizacao em loop periodico."""
        while True:
            self.sync_once(
                extract_zip=extract_zip,
                keep_zip=keep_zip,
                check_interval_days=check_interval_days,
                force=False,
            )
            time.sleep(max(1, interval_minutes) * 60)

    def _get_latest_resource(self) -> CnesResource:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": self.base_page_url,
            "Accept": "application/json, text/plain, */*",
        }
        response = self.session.get(self.list_url, headers=headers, timeout=self.timeout_seconds)
        response.raise_for_status()
        items = response.json()
        if not isinstance(items, list):
            raise RuntimeError("Resposta inesperada da API de downloads do CNES")

        resources: list[CnesResource] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            file_name = str(item.get("nomeArquivo", "")).strip()
            match = CNES_FILE_PATTERN.search(file_name)
            if not match:
                continue
            period = match.group(1)
            url = self.download_url_template.format(file_name=file_name)
            resources.append(CnesResource(file_name=file_name, period_yyyymm=period, url=url))

        if not resources:
            raise RuntimeError("Nenhum arquivo CNES valido encontrado na listagem")

        return max(resources, key=lambda r: (r.period_yyyymm, r.file_name))

    def _download_resource(self, resource: CnesResource) -> Path:
        target_dir = self.settings.raw_dir / "external" / "cnes"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / resource.file_name.lower()

        headers = {"User-Agent": "Mozilla/5.0", "Referer": self.base_page_url}
        logger.info("Baixando CNES %s -> %s", resource.url, target_path)
        try:
            with self.session.get(
                resource.url,
                headers=headers,
                stream=True,
                timeout=self.timeout_seconds,
            ) as response:
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

    def _extract_zip(self, resource: CnesResource, zip_path: Path) -> Path:
        extract_dir = self.settings.raw_dir / "external" / "cnes" / "extracted" / resource.period_yyyymm
        extract_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Extraindo zip %s -> %s", zip_path, extract_dir)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
        return extract_dir

    @staticmethod
    def _is_unchanged(resource: CnesResource, manifest: dict[str, Any]) -> bool:
        old = manifest.get("latest", {})
        return (
            isinstance(old, dict)
            and old.get("file_name") == resource.file_name
            and old.get("period_yyyymm") == resource.period_yyyymm
        )

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _should_check_now(self, manifest: dict[str, Any], check_interval_days: int) -> bool:
        meta = manifest.get("_meta", {})
        if not isinstance(meta, dict):
            return True
        last_checked_at = self._parse_datetime(str(meta.get("last_checked_at", "")).strip())
        if not last_checked_at:
            return True
        now = datetime.now(timezone.utc)
        return now >= last_checked_at + timedelta(days=max(1, check_interval_days))

    def _next_check_at(self, manifest: dict[str, Any], check_interval_days: int) -> datetime | None:
        meta = manifest.get("_meta", {})
        if not isinstance(meta, dict):
            return None
        last_checked_at = self._parse_datetime(str(meta.get("last_checked_at", "")).strip())
        if not last_checked_at:
            return None
        return last_checked_at + timedelta(days=max(1, check_interval_days))

    def _load_manifest(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            return {}
        try:
            with self.manifest_path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except (json.JSONDecodeError, OSError):
            logger.warning("Manifesto CNES invalido. Recriando: %s", self.manifest_path)
            return {}
        return data if isinstance(data, dict) else {}

    def _save_manifest(self, manifest: dict[str, Any]) -> None:
        with self.manifest_path.open("w", encoding="utf-8") as file:
            json.dump(manifest, file, ensure_ascii=False, indent=2)
