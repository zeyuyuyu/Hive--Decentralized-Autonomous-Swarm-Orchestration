import asyncio
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

class NodeStatus(Enum):
    HEALTHY = 'healthy'
    DEGRADED = 'degraded'
    OFFLINE = 'offline'

@dataclass
class SwarmNode:
    id: str
    status: NodeStatus
    load: float
    last_heartbeat: float
    tasks: List[str]

class SwarmManager:
    def __init__(self):
        self.nodes: Dict[str, SwarmNode] = {}
        self.task_queue: asyncio.Queue = asyncio.Queue()
        self.load_threshold = 0.8
        self.heartbeat_timeout = 30.0

    async def register_node(self, node_id: str) -> None:
        self.nodes[node_id] = SwarmNode(
            id=node_id,
            status=NodeStatus.HEALTHY,
            load=0.0,
            last_heartbeat=asyncio.get_event_loop().time(),
            tasks=[]
        )

    async def heartbeat(self, node_id: str, load: float) -> None:
        if node_id in self.nodes:
            node = self.nodes[node_id]
            node.last_heartbeat = asyncio.get_event_loop().time()
            node.load = load
            node.status = NodeStatus.HEALTHY if load < self.load_threshold else NodeStatus.DEGRADED

    async def monitor_nodes(self) -> None:
        while True:
            current_time = asyncio.get_event_loop().time()
            for node_id, node in list(self.nodes.items()):
                if current_time - node.last_heartbeat > self.heartbeat_timeout:
                    node.status = NodeStatus.OFFLINE
                    await self.rebalance_tasks(node_id)
            await asyncio.sleep(5)

    async def rebalance_tasks(self, failed_node_id: str) -> None:
        failed_node = self.nodes[failed_node_id]
        tasks_to_reassign = failed_node.tasks.copy()
        failed_node.tasks.clear()

        healthy_nodes = [
            node for node in self.nodes.values()
            if node.status == NodeStatus.HEALTHY
        ]

        if not healthy_nodes:
            # Queue tasks for later reassignment
            for task in tasks_to_reassign:
                await self.task_queue.put(task)
            return

        # Distribute tasks across healthy nodes
        for task in tasks_to_reassign:
            target_node = min(healthy_nodes, key=lambda n: n.load)
            target_node.tasks.append(task)
            target_node.load += 0.1  # Approximate load increase

    async def assign_task(self, task_id: str) -> Optional[str]:
        healthy_nodes = [
            node for node in self.nodes.values()
            if node.status == NodeStatus.HEALTHY
        ]

        if not healthy_nodes:
            await self.task_queue.put(task_id)
            return None

        target_node = min(healthy_nodes, key=lambda n: n.load)
        target_node.tasks.append(task_id)
        target_node.load += 0.1
        return target_node.id

    async def get_swarm_status(self) -> Dict:
        return {
            'total_nodes': len(self.nodes),
            'healthy_nodes': len([n for n in self.nodes.values() if n.status == NodeStatus.HEALTHY]),
            'degraded_nodes': len([n for n in self.nodes.values() if n.status == NodeStatus.DEGRADED]),
            'offline_nodes': len([n for n in self.nodes.values() if n.status == NodeStatus.OFFLINE]),
            'queued_tasks': self.task_queue.qsize()
        }
