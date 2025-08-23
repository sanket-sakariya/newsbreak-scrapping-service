from fastapi import APIRouter, HTTPException, Depends
from app.schema.base import ResponseSchema
from app.api.deps import get_rabbitmq_channel
from app.core.config import settings
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

class QueueEndpoint:
    def __init__(self):
        self.router = APIRouter()
        self.setup_routes()
    
    def setup_routes(self):
        """Setup the router routes"""
        self.router.add_api_route(
            "/",
            self.get_queue_statistics,
            methods=["GET"],
            response_model=ResponseSchema,
            summary="Get all queue statistics and information"
        )
        
        self.router.add_api_route(
            "/{queue_name}/purge",
            self.purge_queue,
            methods=["DELETE"],
            response_model=ResponseSchema,
            summary="Purge all messages from a specific queue"
        )
    
    async def get_queue_statistics(self):
        """Get all queue statistics and information"""
        try:
            queues_info = []
            queue_names = [settings.scraper_queue, settings.newsbreak_urls_queue, settings.newsbreak_data_queue, settings.dlx_queue]
            rabbitmq_channel = get_rabbitmq_channel()
            
            for queue_name in queue_names:
                try:
                    # Declare queue and get info from the returned object
                    if queue_name == settings.scraper_queue:
                        queue = await rabbitmq_channel.declare_queue(
                            queue_name, 
                            durable=True,
                            arguments={
                                "x-dead-letter-exchange": "",
                                "x-dead-letter-routing-key": settings.dlx_queue
                            }
                        )
                    else:
                        queue = await rabbitmq_channel.declare_queue(queue_name, durable=True)
                    
                    # Use the declaration result to get queue info
                    queue_info = queue.declaration_result
                    
                    queues_info.append({
                        "name": queue_name,
                        "message_count": queue_info.message_count,
                        "consumer_count": queue_info.consumer_count,
                        "status": "active" if queue_info.consumer_count > 0 else "idle",
                        "last_activity": datetime.now(timezone.utc).isoformat()
                    })
                    
                except Exception as e:
                    logger.warning(f"Could not get info for queue {queue_name}: {e}")
                    queues_info.append({
                        "name": queue_name,
                        "message_count": 0,
                        "consumer_count": 0,
                        "status": "unknown",
                        "last_activity": datetime.now(timezone.utc).isoformat(),
                        "error": str(e)
                    })
            
            return ResponseSchema(
                success=True,
                data={"queues": queues_info, "total_queues": len(queues_info)}
            )
            
        except Exception as e:
            logger.error(f"Error getting queue statistics: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def purge_queue(self, queue_name: str):
        """Purge all messages from a specific queue"""
        try:
            valid_queues = [settings.scraper_queue, settings.newsbreak_urls_queue, settings.newsbreak_data_queue, settings.dlx_queue]
            
            if queue_name not in valid_queues:
                raise HTTPException(status_code=400, detail="Invalid queue name")
            
            rabbitmq_channel = get_rabbitmq_channel()
            
            # Declare queue to get info
            if queue_name == settings.scraper_queue:
                queue = await rabbitmq_channel.declare_queue(
                    queue_name, 
                    durable=True,
                    arguments={
                        "x-dead-letter-exchange": "",
                        "x-dead-letter-routing-key": settings.dlx_queue
                    }
                )
            else:
                queue = await rabbitmq_channel.declare_queue(queue_name, durable=True)
            
            messages_purged = queue.declaration_result.message_count
            await queue.purge()
            
            return ResponseSchema(
                success=True,
                data={
                    "queue_name": queue_name,
                    "messages_purged": messages_purged
                }
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error purging queue: {e}")
            raise HTTPException(status_code=500, detail=str(e))

# Create endpoint instance
queue_endpoint = QueueEndpoint()
