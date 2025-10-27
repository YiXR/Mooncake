#!/usr/bin/env python3
"""
Mooncake Configuration
"""
import json
import os
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

@dataclass
class TierConfig:
    """
    Configuration for a single cache tier.
    
    Attributes:
        id: Unique integer identifier for the tier.
        type: The type of storage medium. Must be one of "dram", "vram", or "ssd".
        capacity: The total capacity of this tier in bytes.
        priority: The priority level of the tier (lower number means higher priority).
                  Used by the scheduler to decide eviction/promotion paths.
        tags: A list of arbitrary string tags for policy decisions (e.g., "hot", "numa-0").
        device_id: Required for 'vram' tiers. The GPU device index.
    """
    id: int
    type: str
    capacity: int
    priority: int
    tags: List[str] = field(default_factory=list)

    # Type-specific optional fields
    device_id: Optional[int] = None

    def __post_init__(self):
        """Perform validation after the object is created."""
        if self.type not in ["dram", "vram"]:
            raise ValueError(f"Invalid tier type '{self.type}' for tier {self.id}. Must be 'dram', 'vram', or 'ssd'.")
        if self.type == "vram" and self.device_id is None:
            raise ValueError(f"Tier {self.id} of type 'vram' must have a 'device_id' specified.")

@dataclass
class TieredCacheConfig:
    """Configuration for the entire tiered cache system."""
    tiers: List[TierConfig]

    def to_json_str(self) -> str:
        tier_list = [dataclass.asdict(t) for t in self.tiers]
        cleaned_tier_list = [{k: v for k, v in t.items() if v is not None} for t in tier_list]
        return json.dumps({"tiers": cleaned_tier_list})

DEFAULT_GLOBAL_SEGMENT_SIZE = 3355443200  # 3.125 GiB
DEFAULT_LOCAL_BUFFER_SIZE = 1073741824  # 1.0 GiB

def _parse_global_segment_size(value) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s.endswith("gb"):
            num = s[:-2].strip()
            if not num:
                raise ValueError(
                    "Invalid global_segment_size: missing number before 'gb'"
                )
            return int(num) * 1024 * 1024 * 1024
        return int(s)
    return int(value)

@dataclass
class MooncakeConfig:
    """The configuration class for Mooncake.

    Attributes:
        local_hostname (str): The hostname of the local machine.
        metadata_server (str): The address of the metadata server.
        global_segment_size (int): The size of each global segment in bytes.
        local_buffer_size (int): The size of the local buffer in bytes.
        protocol (str): The communication protocol to use.
        device_name (Optional[str]): The name of the device to use.
        master_server_address (str): The address of the master server.
        use_tiered_cache (bool): Whether to use tiered cache.
        tiered_cache (Optional[TieredCacheConfig]): The configuration for the tiered cache.

    Example of configuration file:
        {
            "local_hostname": "localhost",
            "metadata_server": "localhost:8080",
            "global_segment_size": 3355443200,
            "local_buffer_size": 1073741824,
            "protocol": "tcp",
            "device_name": "",
            "master_server_address": "localhost:8081",
            "use_tiered_cache": true,
            "tiered_cache": {
                "tiers": [
                    {
                        "id": 0,
                        "type": "vram",
                        "capacity": 2147483648,
                        "priority": 0，
                        "device_id": 0,
                        "tags": ["hot", "gpu-0"]
                    },
                    {
                        "id": 1,
                        "type": "dram",
                        "capacity": 1073741824,
                        "priority": 1,
                        "tags": ["cold", "numa-0"]
                    },
                    {
                        "id": 2,
                        "type": "dram",
                        "capacity": 1073741824,
                        "priority": 1,
                        "tags": ["cold", "numa-1"]
                    }
                ]
            }
        }
    """
    local_hostname: str
    metadata_server: str
    global_segment_size: int
    local_buffer_size: int
    protocol: str
    device_name: Optional[str]
    master_server_address: str
    use_tiered_cache: bool = False
    tiered_cache: Optional[TieredCacheConfig] = None

    @staticmethod
    def from_file(file_path: str) -> 'MooncakeConfig':
        """Load the config from a JSON or YAML file."""
        try:
            import yaml
            with open(file_path) as fin:
                config = yaml.safe_load(fin)
        except (ImportError, yaml.YAMLError):
            with open(file_path) as fin:
                config = json.load(fin)

        required_fields = [
            "local_hostname",
            "metadata_server",
            "master_server_address",
        ]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")
        
        tiered_cache_dict = config.pop("tiered_cache", None)

        mooncake_config = MooncakeConfig(
            local_hostname=config.get("local_hostname"),
            metadata_server=config.get("metadata_server"),
            global_segment_size=_parse_global_segment_size(
                config.get("global_segment_size", DEFAULT_GLOBAL_SEGMENT_SIZE)
            ),
            local_buffer_size=config.get("local_buffer_size",
                                         DEFAULT_LOCAL_BUFFER_SIZE),
            protocol=config.get("protocol", "tcp"),
            device_name=config.get("device_name", ""),
            master_server_address=config.get("master_server_address"),
            use_tiered_cache=config.get("use_tiered_cache", False)
        )

        if tiered_cache_dict and isinstance(tiered_cache_dict, dict):
            tier_data_list = tiered_cache_dict.get("tiers", [])
            if not isinstance(tier_data_list, list):
                raise TypeError("'tiers' field in 'tiered_cache' must be a list.")
            
            tier_configs = [TierConfig(**tier_data) for tier_data in tier_data_list]
            mooncake_config.tiered_cache = TieredCacheConfig(tiers=tier_configs)

        if mooncake_config.use_tiered_cache and not mooncake_config.tiered_cache:
            raise ValueError("'use_tiered_cache' is true, but no 'tiered_cache' configuration was provided in the file.")
            
        return mooncake_config

    @staticmethod
    def load_from_env() -> 'MooncakeConfig':
        """Load config from a file specified in the environment variable.
        export MOONCAKE_MASTER=10.13.3.232:50051
        export MOONCAKE_PROTOCOL="rdma"
        export MOONCAKE_DEVICE=""
        export MOONCAKE_TE_META_DATA_SERVER="P2PHANDSHAKE"
        """
        config_file_path = os.getenv('MOONCAKE_CONFIG_PATH')
        if config_file_path is None:
            if not os.getenv("MOONCAKE_MASTER"):
                raise ValueError("Neither the environment variable 'MOONCAKE_CONFIG_PATH' nor 'MOONCAKE_MASTER' is set.")
            return MooncakeConfig(
                local_hostname=os.getenv("LOCAL_HOSTNAME", "localhost"),
                metadata_server=os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE"),
                global_segment_size=_parse_global_segment_size(
                    os.getenv("MOONCAKE_GLOBAL_SEGMENT_SIZE", DEFAULT_GLOBAL_SEGMENT_SIZE)
                ),
                # Zero copy interface does not need local buffer
                local_buffer_size=DEFAULT_LOCAL_BUFFER_SIZE,
                protocol=os.getenv("MOONCAKE_PROTOCOL", "tcp"),
                device_name=os.getenv("MOONCAKE_DEVICE", ""),
                master_server_address=os.getenv("MOONCAKE_MASTER"),
                use_tiered_cache=False,
                tiered_cache=None
            )
        return MooncakeConfig.from_file(config_file_path)