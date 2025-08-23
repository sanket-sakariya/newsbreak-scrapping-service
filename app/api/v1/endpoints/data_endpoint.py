from fastapi import APIRouter, HTTPException, Depends, Query
from app.schema.scraper_schema import NewsbreakDataSchema, NewsbreakUrlSchema
from app.schema.base import ResponseSchema
from app.api.deps import get_data_pool, get_url_pool
from typing import List
import logging

logger = logging.getLogger(__name__)

class DataEndpoint:
    def __init__(self):
        self.router = APIRouter()
        self.setup_routes()
    
    def setup_routes(self):
        """Setup the router routes"""
        self.router.add_api_route(
            "/",
            self.get_newsbreak_data,
            methods=["GET"],
            response_model=ResponseSchema,
            summary="Retrieve scraped NewsBreak data with pagination"
        )
        
        self.router.add_api_route(
            "/{id}",
            self.get_newsbreak_data_by_id,
            methods=["GET"],
            response_model=ResponseSchema,
            summary="Retrieve specific NewsBreak data record"
        )
    
    async def get_newsbreak_data(self, page: int = Query(1, ge=1), limit: int = Query(100, ge=1, le=1000)):
        """Retrieve scraped NewsBreak data with pagination"""
        try:
            offset = (page - 1) * limit
            data_pool = get_data_pool()
            
            async with data_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT nd.*, sd.source_name, cd.city_name,
                           tc1.category_name as first_category_name,
                           tc2.category_name as second_category_name,
                           tc3.category_name as third_category_name,
                           ne.entity_name, nt.tag_name
                    FROM newsbreak_data nd
                    LEFT JOIN source_data sd ON nd.source_id = sd.source_id
                    LEFT JOIN city_data cd ON nd.city_id = cd.city_id
                    LEFT JOIN text_category_data tc1 ON nd.first_text_category_id = tc1.text_category_id
                    LEFT JOIN text_category_data tc2 ON nd.second_text_category_id = tc2.text_category_id
                    LEFT JOIN text_category_data tc3 ON nd.third_text_category_id = tc3.text_category_id
                    LEFT JOIN nf_entities_data ne ON nd.nf_entities_id = ne.nf_entities_id
                    LEFT JOIN nf_tags_data nt ON nd.nf_tags_id = nt.nf_tags_id
                    ORDER BY nd.created_at DESC
                    LIMIT $1 OFFSET $2
                """, limit, offset)
            
            data = []
            for row in rows:
                data.append({
                    "id": row["id"],
                    "likeCount": row["likecount"],
                    "commentCount": row["commentcount"],
                    "source_id": row["source_id"],
                    "source_name": row["source_name"],
                    "city_id": row["city_id"],
                    "city_name": row["city_name"],
                    "title": row["title"],
                    "origin_url": row["origin_url"],
                    "share_count": row["share_count"],
                    "first_text_category_id": row["first_text_category_id"],
                    "first_category_name": row["first_category_name"],
                    "second_text_category_id": row["second_text_category_id"],
                    "second_category_name": row["second_category_name"],
                    "third_text_category_id": row["third_text_category_id"],
                    "third_category_name": row["third_category_name"],
                    "first_text_category_value": row["first_text_category_value"],
                    "second_text_category_value": row["second_text_category_value"],
                    "third_text_category_value": row["third_text_category_value"],
                    "nf_entities_id": row["nf_entities_id"],
                    "entity_name": row["entity_name"],
                    "nf_entities_value": row["nf_entities_value"],
                    "nf_tags_id": row["nf_tags_id"],
                    "tag_name": row["tag_name"],
                    "workspace_id": row["workspace_id"],
                    "created_by": row["created_by"],
                    "is_active": row["is_active"],
                    "status": row["status"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                })
            
            return ResponseSchema(
                success=True,
                data={"records": data, "total_records": len(data), "page": page, "limit": limit}
            )
            
        except Exception as e:
            logger.error(f"Error getting NewsBreak data: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def get_newsbreak_data_by_id(self, id: str):
        """Retrieve specific NewsBreak data record"""
        try:
            data_pool = get_data_pool()
            
            async with data_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT nd.*, sd.source_name, cd.city_name,
                           tc1.category_name as first_category_name,
                           tc2.category_name as second_category_name,
                           tc3.category_name as third_category_name,
                           ne.entity_name, nt.tag_name
                    FROM newsbreak_data nd
                    LEFT JOIN source_data sd ON nd.source_id = sd.source_id
                    LEFT JOIN city_data cd ON nd.city_id = cd.city_id
                    LEFT JOIN text_category_data tc1 ON nd.first_text_category_id = tc1.text_category_id
                    LEFT JOIN text_category_data tc2 ON nd.second_text_category_id = tc2.text_category_id
                    LEFT JOIN text_category_data tc3 ON nd.third_text_category_id = tc3.text_category_id
                    LEFT JOIN nf_entities_data ne ON nd.nf_entities_id = ne.nf_entities_id
                    LEFT JOIN nf_tags_data nt ON nd.nf_tags_id = nt.nf_tags_id
                    WHERE nd.id = $1
                """, id)
            
            if not row:
                raise HTTPException(status_code=404, detail="NewsBreak data not found")
            
            data = {
                "id": row["id"],
                "likeCount": row["likecount"],
                "commentCount": row["commentcount"],
                "source_id": row["source_id"],
                "source_name": row["source_name"],
                "city_id": row["city_id"],
                "city_name": row["city_name"],
                "title": row["title"],
                "origin_url": row["origin_url"],
                "share_count": row["share_count"],
                "first_text_category_id": row["first_text_category_id"],
                "first_category_name": row["first_category_name"],
                "second_text_category_id": row["second_text_category_id"],
                "second_category_name": row["second_category_name"],
                "third_text_category_id": row["third_text_category_id"],
                "third_category_name": row["third_category_name"],
                "first_text_category_value": row["first_text_category_value"],
                "second_text_category_value": row["second_text_category_value"],
                "third_text_category_value": row["third_text_category_value"],
                "nf_entities_id": row["nf_entities_id"],
                "entity_name": row["entity_name"],
                "nf_entities_value": row["nf_entities_value"],
                "nf_tags_id": row["nf_tags_id"],
                "tag_name": row["tag_name"],
                "workspace_id": row["workspace_id"],
                "created_by": row["created_by"],
                "is_active": row["is_active"],
                "status": row["status"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"]
            }
            
            return ResponseSchema(
                success=True,
                data=data
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting NewsBreak data by ID: {e}")
            raise HTTPException(status_code=500, detail=str(e))

# Create endpoint instance
data_endpoint = DataEndpoint()
