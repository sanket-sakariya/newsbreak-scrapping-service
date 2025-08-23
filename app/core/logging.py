import logging
import logging.config
import yaml
import os
from pathlib import Path

def setup_logging():
    """Setup logging configuration"""
    # Default logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('app.log')
        ]
    )
    
    # Try to load logging config from file
    config_path = Path("config/logging.yaml")
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logging.config.dictConfig(config)
        except Exception as e:
            print(f"Failed to load logging config: {e}")
    
    # Set specific logger levels
    logging.getLogger('aio_pika').setLevel(logging.WARNING)
    logging.getLogger('asyncpg').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

logger = setup_logging()
