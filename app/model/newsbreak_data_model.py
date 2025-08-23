from sqlalchemy import Column, Integer, String, Text, Boolean, Float, ForeignKey, UUID
from sqlalchemy.orm import relationship
from .base import BaseModel

class SourceDataModel(BaseModel):
    __tablename__ = "source_data"
    
    source_id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(255), unique=True, nullable=False)

class CityDataModel(BaseModel):
    __tablename__ = "city_data"
    
    city_id = Column(Integer, primary_key=True, autoincrement=True)
    city_name = Column(String(255), unique=True, nullable=False)

class TextCategoryDataModel(BaseModel):
    __tablename__ = "text_category_data"
    
    text_category_id = Column(Integer, primary_key=True, autoincrement=True)
    category_name = Column(String(255), unique=True, nullable=False)

class NfEntitiesDataModel(BaseModel):
    __tablename__ = "nf_entities_data"
    
    nf_entities_id = Column(Integer, primary_key=True, autoincrement=True)
    entity_name = Column(String(255), unique=True, nullable=False)

class NfTagsDataModel(BaseModel):
    __tablename__ = "nf_tags_data"
    
    nf_tags_id = Column(Integer, primary_key=True, autoincrement=True)
    tag_name = Column(String(255), unique=True, nullable=False)

class NewsbreakDataModel(BaseModel):
    __tablename__ = "newsbreak_data"
    
    id = Column(String(255), primary_key=True)
    likeCount = Column(Integer)
    commentCount = Column(Integer)
    source_id = Column(Integer, ForeignKey("source_data.source_id"))
    city_id = Column(Integer, ForeignKey("city_data.city_id"))
    title = Column(Text)
    origin_url = Column(Text)
    share_count = Column(Integer)
    first_text_category_id = Column(Integer, ForeignKey("text_category_data.text_category_id"))
    second_text_category_id = Column(Integer, ForeignKey("text_category_data.text_category_id"))
    third_text_category_id = Column(Integer, ForeignKey("text_category_data.text_category_id"))
    first_text_category_value = Column(Float)
    second_text_category_value = Column(Float)
    third_text_category_value = Column(Float)
    nf_entities_id = Column(Integer, ForeignKey("nf_entities_data.nf_entities_id"))
    nf_entities_value = Column(Float)
    nf_tags_id = Column(Integer, ForeignKey("nf_tags_data.nf_tags_id"))
    workspace_id = Column(UUID)
    created_by = Column(UUID)
    is_active = Column(Boolean, default=True)
    status = Column(String(50), default="creating")
    
    # Relationships
    source = relationship("SourceDataModel")
    city = relationship("CityDataModel")
    first_text_category = relationship("TextCategoryDataModel", foreign_keys=[first_text_category_id])
    second_text_category = relationship("TextCategoryDataModel", foreign_keys=[second_text_category_id])
    third_text_category = relationship("TextCategoryDataModel", foreign_keys=[third_text_category_id])
    nf_entities = relationship("NfEntitiesDataModel")
    nf_tags = relationship("NfTagsDataModel")
