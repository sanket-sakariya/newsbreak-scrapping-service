from typing import Generic, TypeVar, Type, Optional, List, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from app.model.base import BaseModel

ModelType = TypeVar("ModelType", bound=BaseModel)

class BaseRepository(Generic[ModelType]):
    def __init__(self, model: Type[ModelType]):
        self.model = model
    
    async def get(self, session: AsyncSession, id: Any) -> Optional[ModelType]:
        """Get a single record by ID"""
        result = await session.execute(select(self.model).where(self.model.id == id))
        return result.scalar_one_or_none()
    
    async def get_all(self, session: AsyncSession, skip: int = 0, limit: int = 100) -> List[ModelType]:
        """Get all records with pagination"""
        result = await session.execute(select(self.model).offset(skip).limit(limit))
        return result.scalars().all()
    
    async def create(self, session: AsyncSession, obj_in: dict) -> ModelType:
        """Create a new record"""
        db_obj = self.model(**obj_in)
        session.add(db_obj)
        await session.commit()
        await session.refresh(db_obj)
        return db_obj
    
    async def update(self, session: AsyncSession, id: Any, obj_in: dict) -> Optional[ModelType]:
        """Update a record"""
        result = await session.execute(
            update(self.model).where(self.model.id == id).values(**obj_in)
        )
        await session.commit()
        return await self.get(session, id)
    
    async def delete(self, session: AsyncSession, id: Any) -> bool:
        """Delete a record"""
        result = await session.execute(delete(self.model).where(self.model.id == id))
        await session.commit()
        return result.rowcount > 0
