import asyncio
import random
import logging
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)

class SwarmManager:
    def __init__(self, num_nodes: int, replication_factor: int):
        self.num_nodes = num_nodes
        self.replication_factor = replication_factor
        self.nodes: List[Node] = [Node(i) for i in range(num_nodes)]
        self.data: Dict[str, List[Node]] = {}

    async def add_data(self, key: str, value: str):
        nodes = self._select_nodes()
        self.data[key] = nodes
        for node in nodes:
            await node.store_data(key, value)

    async def get_data(self, key: str) -> str:
        nodes = self.data[key]
        responses = await asyncio.gather(*[node.fetch_data(key) for node in nodes])
        value = max(set(responses), key=responses.count)
        return value

    async def remove_data(self, key: str):
        nodes = self.data[key]
        await asyncio.gather(*[node.delete_data(key) for node in nodes])
        del self.data[key]

    def _select_nodes(self) -> List[Node]:
        available_nodes = [node for node in self.nodes if not node.is_failed()]
        selected_nodes = random.sample(available_nodes, self.replication_factor)
        return selected_nodes

    async def monitor_nodes(self):
        while True:
            await asyncio.sleep(60)
            for node in self.nodes:
                if node.is_failed():
                    logger.warning(f"Node {node.id} has failed.")
                    self._recover_node(node)

    def _recover_node(self, failed_node: 'Node'):
        new_node = Node(failed_node.id)
        self.nodes[failed_node.id] = new_node
        for key, nodes in self.data.items():
            if failed_node in nodes:
                nodes.remove(failed_node)
                nodes.append(new_node)
                for node in nodes:
                    asyncio.create_task(node.store_data(key, self.data[key][0].data[key]))

class Node:
    def __init__(self, id: int):
        self.id = id
        self.data: Dict[str, str] = {}
        self.is_failed_probability = 0.01

    async def store_data(self, key: str, value: str):
        self.data[key] = value
        if random.random() < self.is_failed_probability:
            self.fail()

    async def fetch_data(self, key: str) -> str:
        if key in self.data:
            return self.data[key]
        else:
            raise KeyError(f"Key {key} not found.")

    async def delete_data(self, key: str):
        if key in self.data:
            del self.data[key]

    def is_failed(self) -> bool:
        return random.random() < self.is_failed_probability

    def fail(self):
        logger.warning(f"Node {self.id} has failed.")
        self.is_failed_probability = 1.0
