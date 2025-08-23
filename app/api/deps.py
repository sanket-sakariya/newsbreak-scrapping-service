from typing import Generator
from app.core.database import db_manager
from app.core.workers import data_worker, url_worker, url_feeder

def get_data_pool():
    """Get data database pool"""
    return db_manager.get_data_pool()

def get_url_pool():
    """Get URL database pool"""
    return db_manager.get_url_pool()

def get_rabbitmq_channel():
    """Get RabbitMQ channel"""
    return db_manager.get_rabbitmq_channel()

def get_workers():
    """Get worker instances"""
    return {
        'data_worker': data_worker,
        'url_worker': url_worker,
        'url_feeder': url_feeder
    }
