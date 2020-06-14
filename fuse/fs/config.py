from dataclasses import dataclass
from typing import List


@dataclass
class ReplicaFSConfig:
    master_mount_point: str
    slave_mount_points: List[str]
    master_backing: str
    slave_backings: List[str]

    nbr_slaves: int
