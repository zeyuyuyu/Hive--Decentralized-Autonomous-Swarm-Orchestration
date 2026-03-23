import asyncio
from typing import Dict, List, Set
import time

class SwarmManager:
    def __init__(self):
        self.nodes: Dict[str, float] = {}
        self.active_nodes: Set[str] = set()
        self.min_nodes = 3
        self.max_nodes = 10
        self.health_check_interval = 30

    async def register_node(self, node_id: str) -> bool:
        """Register a new node in the swarm"""
        if len(self.nodes) >= self.max_nodes:
            return False
        
        self.nodes[node_id] = time.time()
        self.active_nodes.add(node_id)
        return True

    async def heartbeat(self, node_id: str) -> None:
        """Update node's last seen timestamp"""
        if node_id in self.nodes:
            self.nodes[node_id] = time.time()

    async def remove_node(self, node_id: str) -> None:
        """Remove a node from the swarm"""
        self.nodes.pop(node_id, None)
        self.active_nodes.discard(node_id)

    async def health_check(self) -> None:
        """Periodic health check of all nodes"""
        while True:
            current_time = time.time()
            dead_nodes = [
                node_id for node_id, last_seen in self.nodes.items()
                if current_time - last_seen > self.health_check_interval
            ]

            for node_id in dead_nodes:
                await self.remove_node(node_id)

            # Auto-scaling logic
            if len(self.active_nodes) < self.min_nodes:
                await self.scale_up()
            elif len(self.active_nodes) > self.max_nodes:
                await self.scale_down()

            await asyncio.sleep(self.health_check_interval)

    async def scale_up(self) -> None:
        """Add new nodes to meet minimum requirement"""
        nodes_needed = self.min_nodes - len(self.active_nodes)
        for _ in range(nodes_needed):
            # Implementation would integrate with cloud provider API
            # or container orchestration platform
            pass

    async def scale_down(self) -> None:
        """Remove excess nodes to stay within limits"""
        excess_nodes = len(self.active_nodes) - self.max_nodes
        nodes_to_remove = list(self.active_nodes)[-excess_nodes:]
        for node_id in nodes_to_remove:
            await self.remove_node(node_id)

    async def get_active_nodes(self) -> List[str]:
        """Return list of currently active nodes"""
        return list(self.active_nodes)

    async def start(self) -> None:
        """Start the swarm manager"""
        await self.health_check()
