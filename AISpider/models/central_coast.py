from sqlalchemy import Column, String, Text, Integer, DateTime, func
from .metadata_base import Base


class CentralCoast(Base):
    __tablename__ = 'central_coast'

    id = Column(Integer, primary_key=True)
    application_id = Column(String(125), unique=True, nullable=False)
    application_num = Column(String(125), nullable=True, server_default=None)
    description = Column(Text, nullable=True, server_default=None)
    lodgement_date = Column(Integer, nullable=True,)
    status = Column(String(125), nullable=True, server_default=None)
    responsible_officer = Column(String(256), nullable=True, server_default=None)
    address = Column(Text, nullable=True, server_default=None)
    decision = Column(String(125), nullable=True, server_default=None)
    decision_date = Column(Integer, nullable=True,)
    names = Column(String(512), nullable=True, server_default=None)
    documents = Column(Text, nullable=True, server_default=None)

    _tid = Column(String(255), nullable=False, server_default=None)
    created_at = Column(DateTime, server_default=func.now())
    update_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
