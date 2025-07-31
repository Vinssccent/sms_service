# -*- coding: utf-8 -*-
import uuid
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, DateTime, Text, UniqueConstraint, Index
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

Base = declarative_base()

class AllowedSender(Base):
    __tablename__ = 'allowed_senders'
    __table_args__ = (UniqueConstraint('name', 'service_id', name='uq_sender_service'),)
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, index=True)
    service_id = Column(Integer, ForeignKey('services.id', ondelete="CASCADE"), nullable=False)
    service = relationship("Service", back_populates="allowed_senders")
    def __str__(self): return self.name

class Service(Base):
    __tablename__ = 'services'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    code = Column(String(10), unique=True, nullable=False, index=True)
    allowed_senders = relationship("AllowedSender", back_populates="service", cascade="all, delete-orphan")
    sessions = relationship("Session", back_populates="service", cascade="all, delete-orphan")
    def __str__(self): return f"{self.name} ({self.code})"

class Country(Base):
    __tablename__ = 'countries'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    iso_code = Column(String(10), unique=True, nullable=False, index=True)
    phone_code = Column(String, nullable=False, index=True)
    operators = relationship("Operator", back_populates="country", cascade="all, delete-orphan")
    def __str__(self): return self.name

class Operator(Base):
    __tablename__ = 'operators'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, index=True)
    country_id = Column(Integer, ForeignKey('countries.id', ondelete="CASCADE"), nullable=False)
    country = relationship("Country", back_populates="operators")
    def __str__(self): return self.name

class Provider(Base):
    __tablename__ = 'providers'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False, index=True)
    smpp_host = Column(String, nullable=False)
    smpp_port = Column(Integer, nullable=False)
    system_id = Column(String, nullable=False)
    password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    phone_numbers = relationship("PhoneNumber", back_populates="provider", cascade="all, delete-orphan")
    def __str__(self): return self.name

class PhoneNumber(Base):
    __tablename__ = 'phone_numbers'
    id = Column(Integer, primary_key=True)
    number_str = Column(String, unique=True, nullable=False, index=True)
    provider_id = Column(Integer, ForeignKey('providers.id', ondelete="CASCADE"), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    is_in_use = Column(Boolean, default=False, nullable=False, index=True)
    country_id = Column(Integer, ForeignKey('countries.id', ondelete="CASCADE"), nullable=False)
    operator_id = Column(Integer, ForeignKey('operators.id', ondelete="SET NULL"), nullable=True)
    sort_order = Column(Integer, default=0, nullable=False, index=True)
    provider = relationship("Provider", back_populates="phone_numbers")
    country = relationship("Country")
    operator = relationship("Operator")
    sessions = relationship("Session", back_populates="phone_number", cascade="all, delete-orphan")
    def __str__(self): return self.number_str

class Session(Base):
    __tablename__ = 'sessions'
    id = Column(Integer, primary_key=True)
    phone_number_str = Column(String, nullable=False, index=True)
    status = Column(Integer, nullable=False, default=1, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    service_id = Column(Integer, ForeignKey('services.id', ondelete="CASCADE"), nullable=False)
    phone_number_id = Column(Integer, ForeignKey('phone_numbers.id', ondelete="CASCADE"), nullable=False)
    api_key_id = Column(Integer, ForeignKey('api_keys.id', ondelete="CASCADE"), nullable=False)
    service = relationship("Service", back_populates="sessions")
    phone_number = relationship("PhoneNumber", back_populates="sessions")
    api_key = relationship("ApiKey", back_populates="sessions")
    sms_messages = relationship("SmsMessage", back_populates="session", cascade="all, delete-orphan")

class SmsMessage(Base):
    __tablename__ = 'sms_messages'
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id', ondelete="CASCADE"), nullable=False)
    source_addr = Column(String, nullable=False, index=True)
    text = Column(Text, nullable=False)
    code = Column(String(20), nullable=True) # <-- ВОТ ЭТА СТРОКА ПРОПАЛА
    received_at = Column(DateTime(timezone=True), server_default=func.now())
    session = relationship("Session", back_populates="sms_messages")
    __table_args__ = (
        Index('ix_sms_code', 'code'),
    )
    def __str__(self) -> str:
        return f'{self.text[:50]}...' if len(self.text) > 50 else self.text

class ApiKey(Base):
    __tablename__ = 'api_keys'
    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True, nullable=False, index=True, default=lambda: uuid.uuid4().hex)
    description = Column(String)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    sessions = relationship("Session", back_populates="api_key", cascade="all, delete-orphan")
    def __str__(self): return self.description or self.key

