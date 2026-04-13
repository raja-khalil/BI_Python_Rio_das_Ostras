"""Configuracoes centralizadas do projeto."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import os
from urllib.parse import quote

from dotenv import load_dotenv


ROOT_MARKERS = {"run_pipeline.py", "pyproject.toml"}


def _find_project_root(start: Path | None = None) -> Path:
    """Encontra raiz do projeto buscando arquivos marcadores."""
    current = (start or Path(__file__)).resolve()
    for parent in [current, *current.parents]:
        if any((parent / marker).exists() for marker in ROOT_MARKERS):
            return parent
    raise FileNotFoundError("Nao foi possivel localizar a raiz do projeto")


@dataclass(frozen=True)
class Settings:
    """Armazena configuracoes e caminhos principais do projeto."""

    project_root: Path
    data_dir: Path
    raw_dir: Path
    staging_dir: Path
    processed_dir: Path
    logs_dir: Path

    app_env: str
    log_level: str

    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_schema: str

    api_base_url: str
    api_timeout_seconds: int
    api_max_retries: int
    api_backoff_seconds: float
    api_page_size: int
    api_max_pages_per_year: int
    api_user: str | None
    api_password: str | None

    @property
    def sqlalchemy_database_url(self) -> str:
        """Retorna URL SQLAlchemy usando driver psycopg."""
        db_user = quote(self.db_user, safe="")
        db_password = quote(self.db_password, safe="")
        return (
            f"postgresql+psycopg://{db_user}:{db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Carrega variaveis de ambiente e retorna configuracao unica."""
    project_root = _find_project_root()
    load_dotenv(project_root / ".env", override=False)

    data_dir = project_root / "data"

    return Settings(
        project_root=project_root,
        data_dir=data_dir,
        raw_dir=data_dir / "raw",
        staging_dir=data_dir / "staging",
        processed_dir=data_dir / "processed",
        logs_dir=project_root / "logs",
        app_env=os.getenv("APP_ENV", "dev"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_name=os.getenv("DB_NAME", "dengue_bi"),
        db_user=os.getenv("DB_USER", "postgres"),
        db_password=os.getenv("DB_PASSWORD", "postgres"),
        db_schema=os.getenv("DB_SCHEMA", "saude"),
        api_base_url=os.getenv("API_BASE_URL", "https://apidadosabertos.saude.gov.br"),
        api_timeout_seconds=int(os.getenv("API_TIMEOUT_SECONDS", "30")),
        api_max_retries=int(os.getenv("API_MAX_RETRIES", "3")),
        api_backoff_seconds=float(os.getenv("API_BACKOFF_SECONDS", "1.0")),
        api_page_size=int(os.getenv("API_PAGE_SIZE", "20")),
        api_max_pages_per_year=int(os.getenv("API_MAX_PAGES_PER_YEAR", "500")),
        api_user=os.getenv("API_USER"),
        api_password=os.getenv("API_PASSWORD"),
    )
