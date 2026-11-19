"""Ponto de entrada Uvicorn para o nó configurado por ambiente."""
from storage_node.app import create_app
from storage_node.settings import NodeSettings

app = create_app(NodeSettings.from_env())
