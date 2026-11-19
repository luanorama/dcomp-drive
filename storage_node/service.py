"""Entrada do nó no Compose. A aplicação de chunks fica em storage_node.main."""
from storage_node.agent import attach_agent
from storage_node.main import app

attach_agent(app)
