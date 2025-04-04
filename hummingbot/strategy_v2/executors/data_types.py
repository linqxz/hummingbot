import hashlib
import random
import time

import base58
from pydantic.v1 import BaseModel, validator

from hummingbot.client.settings import AllConnectorSettings


class ExecutorConfigBase(BaseModel):
    id: str = None  # Make ID optional
    type: str
    timestamp: float
    controller_id: str = "main"
    
    # Registry to keep track of subclass types
    _config_types = {}
    
    def __init_subclass__(cls, **kwargs):
        """
        Register each subclass with a unique identifier.
        This helps with proper type identification during isinstance checks.
        """
        super().__init_subclass__(**kwargs)
        ExecutorConfigBase._config_types[cls.__name__] = cls
        # Ensure each class has its own unique type identifier
        if not hasattr(cls, 'type') or cls.type == "":
            cls.type = cls.__name__.lower()
    
    @classmethod
    def get_config_type(cls, type_name):
        """Get a config class by its type name."""
        return cls._config_types.get(type_name)

    @validator('id', pre=True, always=True)
    def set_id(cls, v, values):
        if v is None:
            # Use timestamp from values if available, else current time
            timestamp = values.get('timestamp', time.time())
            unique_component = random.randint(0, 99999)
            raw_id = f"{timestamp}-{unique_component}"
            hashed_id = hashlib.sha256(raw_id.encode()).digest()  # Get bytes
            return base58.b58encode(hashed_id).decode()  # Base58 encode
        return v


class ConnectorPair(BaseModel):
    connector_name: str
    trading_pair: str

    def is_amm_connector(self) -> bool:
        return self.connector_name in sorted(
            AllConnectorSettings.get_gateway_amm_connector_names()
        )
