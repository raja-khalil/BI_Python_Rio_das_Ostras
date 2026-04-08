"""Clientes HTTP para consumo da API de dados abertos do Ministerio da Saude."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import time
from typing import Any

import requests

from src.config.settings import get_settings
from src.utils.logger import get_logger


logger = get_logger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class ApiCredentials:
    """Credenciais de acesso para endpoints que exigem autenticacao."""

    login: str
    senha: str


@dataclass
class BaseApiClient:
    """Cliente base HTTP com retry, timeout e autenticacao opcional."""

    base_url: str
    timeout_seconds: int = 30
    max_retries: int = 3
    backoff_seconds: float = 1.0
    credentials: ApiCredentials | None = None

    def __post_init__(self) -> None:
        self.session = requests.Session()
        self._bearer_token: str | None = None

    def authenticate(self) -> str | None:
        """Efetua login na API e armazena token Bearer quando configurado."""
        if self._bearer_token:
            return self._bearer_token

        if not self.credentials:
            return None

        payload = {"login": self.credentials.login, "senha": self.credentials.senha}
        response_data = self._request_json(
            method="POST",
            endpoint="/autenticacao/login",
            json_body=payload,
            with_auth=False,
        )

        token = None
        if isinstance(response_data, dict):
            token = (
                response_data.get("access_token")
                or response_data.get("token")
                or response_data.get("jwt")
            )

        if not token:
            raise RuntimeError("Autenticacao executada, mas token nao foi retornado pela API")

        self._bearer_token = str(token)
        return self._bearer_token

    def get_json(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        with_auth: bool = False,
    ) -> dict[str, Any] | list[Any]:
        """Executa GET e retorna payload JSON."""
        return self._request_json(
            method="GET",
            endpoint=endpoint,
            params=params,
            with_auth=with_auth,
        )

    def _request_json(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        with_auth: bool = False,
    ) -> dict[str, Any] | list[Any]:
        """Executa request HTTP com retry e valida resposta JSON."""
        url = self._build_url(endpoint=endpoint)
        headers: dict[str, str] = {"Accept": "application/json"}

        if with_auth:
            token = self.authenticate()
            if token:
                headers["Authorization"] = f"Bearer {token}"

        attempts = self.max_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    timeout=self.timeout_seconds,
                    headers=headers,
                )
            except requests.RequestException as exc:
                if attempt == attempts:
                    raise RuntimeError(f"Falha de conexao ao consultar API: {url}") from exc
                self._wait_retry(attempt)
                continue

            if response.status_code in RETRYABLE_STATUS and attempt < attempts:
                logger.warning(
                    "Resposta %s para %s. Tentando novamente (tentativa %s/%s).",
                    response.status_code,
                    url,
                    attempt,
                    attempts,
                )
                self._wait_retry(attempt)
                continue

            if response.status_code >= 400:
                body_preview = response.text[:400]
                raise RuntimeError(
                    f"Erro HTTP {response.status_code} em {url}. Resposta: {body_preview}"
                )

            try:
                payload = response.json()
            except ValueError as exc:
                raise RuntimeError(f"Resposta nao JSON para endpoint: {url}") from exc

            return payload

        raise RuntimeError(f"Falha inesperada ao consultar API: {url}")

    def _wait_retry(self, attempt: int) -> None:
        time.sleep(self.backoff_seconds * attempt)

    def _build_url(self, endpoint: str) -> str:
        return f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"


class ArbovirosesDengueApiClient(BaseApiClient):
    """Cliente especializado para endpoint /arboviroses/dengue."""

    endpoint = "/arboviroses/dengue"

    @classmethod
    def from_settings(cls) -> "ArbovirosesDengueApiClient":
        settings = get_settings()
        credentials = None

        if settings.api_user and settings.api_password:
            credentials = ApiCredentials(login=settings.api_user, senha=settings.api_password)

        return cls(
            base_url=settings.api_base_url,
            timeout_seconds=settings.api_timeout_seconds,
            max_retries=settings.api_max_retries,
            backoff_seconds=settings.api_backoff_seconds,
            credentials=credentials,
        )

    def fetch_page(self, year: int, limit: int, offset: int) -> list[dict[str, Any]]:
        """Busca uma pagina da base de dengue."""
        params = {
            "nu_ano": str(year),
            "limit": limit,
            "offset": offset,
        }
        payload = self.get_json(endpoint=self.endpoint, params=params, with_auth=False)

        if isinstance(payload, dict):
            rows = payload.get("parametros", [])
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        return []

    def fetch_year(
        self,
        year: int,
        limit: int,
        max_pages: int,
        since_date: date | None = None,
    ) -> list[dict[str, Any]]:
        """Busca dados paginados de um ano e retorna lista de registros."""
        collected: list[dict[str, Any]] = []

        for page in range(max_pages):
            page_rows = self.fetch_page(year=year, limit=limit, offset=page)
            if not page_rows:
                break

            if since_date:
                page_rows = self._filter_since_date(page_rows=page_rows, since_date=since_date)

            collected.extend(page_rows)

            if len(page_rows) < limit:
                break

        return collected

    @staticmethod
    def _filter_since_date(page_rows: list[dict[str, Any]], since_date: date) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        for row in page_rows:
            raw_date = row.get("dt_notific")
            if not raw_date:
                continue
            try:
                dt_notific = date.fromisoformat(str(raw_date))
            except ValueError:
                continue
            if dt_notific >= since_date:
                filtered.append(row)
        return filtered
