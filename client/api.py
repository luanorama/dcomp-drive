"""Cliente HTTP do plano de controle e parser tolerante de erros."""
from __future__ import annotations

from typing import Any

import httpx


class ApiError(Exception):
    """Resposta HTTP inesperada do controlador ou de um nó."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def parse_error(response: httpx.Response) -> str:
    """Extrai mensagem dos formatos FastAPI, JSON simples e texto puro."""
    try:
        payload = response.json()
    except (ValueError, TypeError):
        text = response.text.strip()
        return text or f"resposta vazia (HTTP {response.status_code})"
    if isinstance(payload, dict):
        detail = payload.get("detail")
        if isinstance(detail, dict):
            error = detail.get("error") or detail.get("message")
            if error is not None:
                return str(error)
        if isinstance(detail, list):
            messages = [str(item.get("msg", item)) if isinstance(item, dict) else str(item)
                        for item in detail]
            if messages:
                return "; ".join(messages)
        for key in ("error", "message"):
            if payload.get(key) is not None:
                return str(payload[key])
    return response.text.strip() or f"resposta sem mensagem (HTTP {response.status_code})"


def make_http(transport: httpx.BaseTransport | None = None, *, control: bool = True) -> httpx.Client:
    """Cria cliente HTTP comum, também compatível com MockTransport."""
    timeout = httpx.Timeout(15.0) if control else httpx.Timeout(60.0, connect=5.0)
    return httpx.Client(transport=transport, timeout=timeout, follow_redirects=False)


class ControllerClient:
    """Encapsula as rotas da Pessoa A sem alterar o contrato."""

    def __init__(self, base_url: str, transport: httpx.BaseTransport | None = None,
                 client: httpx.Client | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.transport = transport
        self.http = client or make_http(transport, control=True)
        self._owns_client = client is None

    def close(self) -> None:
        """Fecha a sessão se ela foi criada por esta instância."""
        if self._owns_client:
            self.http.close()

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Faz uma chamada de metadados e normaliza falhas de transporte."""
        try:
            response = self.http.request(method, f"{self.base_url}{path}", **kwargs)
        except httpx.HTTPError as exc:
            raise ApiError(0, f"Não foi possível conectar ao controlador em {self.base_url}.") from exc
        if not 200 <= response.status_code < 300:
            raise ApiError(response.status_code, parse_error(response))
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError(response.status_code, "Resposta inválida do controlador.") from exc

    def health(self) -> dict[str, Any]:
        return self.request("GET", "/health")

    def allocate(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.request("POST", "/files/allocate", json=payload)

    def commit(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.request("POST", "/files/commit", json=payload)

    def locate(self, path: str) -> dict[str, Any]:
        return self.request("GET", "/files/locate", params={"path": path})

    def stat(self, path: str) -> dict[str, Any]:
        return self.request("GET", "/files/stat", params={"path": path})

    def list_dir(self, path: str) -> dict[str, Any]:
        return self.request("GET", "/dirs", params={"path": path})

    def mkdir(self, path: str) -> dict[str, Any]:
        return self.request("POST", "/dirs", json={"path": path})

    def move(self, source: str, destination: str) -> dict[str, Any]:
        return self.request("POST", "/files/move", json={"from": source, "to": destination})

    def remove_file(self, path: str) -> dict[str, Any]:
        return self.request("DELETE", "/files", params={"path": path})

    def remove_dir(self, path: str) -> dict[str, Any]:
        return self.request("DELETE", "/dirs", params={"path": path})

    def status(self) -> dict[str, Any]:
        return self.request("GET", "/status")

    def drain(self, node_id: str) -> dict[str, Any]:
        return self.request("POST", f"/nodes/{node_id}/drain")

    def report_corrupt(self, chunk_id: str, disk_id: str) -> dict[str, Any]:
        return self.request("POST", "/chunks/corrupt", json={
            "chunk_id": chunk_id, "disk_id": disk_id,
        })
