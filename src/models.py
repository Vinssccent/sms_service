# src/models.py
# -*- coding: utf-8 -*-
import uuid
import sqlalchemy as sa # <--- ВОТ ИСПРАВЛЕНИЕ. ЭТА СТРОКА ДОБАВЛЕНА.
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, DateTime, Text, Index, UniqueConstraint, select, func, BigInteger, Enum, Numeric
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func
from sqlalchemy.ext.hybrid import hybrid_property
from starlette.requests import Request

Base = declarative_base()


class Service(Base):
    __tablename__ = 'services'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    code = Column(String(10), unique=True, nullable=False, index=True)
    icon_class = Column(String, nullable=True)
    allowed_senders = Column(Text, nullable=True, comment="Через запятую: Google,GO,CloudOTP")
    daily_limit = Column(Integer, nullable=True, comment="Default daily limit if no specific rule applies")
    sessions = relationship("Session", back_populates="service", cascade="all, delete-orphan")
    service_limits = relationship("ServiceLimit", back_populates="service", cascade="all, delete-orphan", lazy="selectin")
    def __str__(self): return f"{self.name} ({self.code})"
    async def __admin_repr__(self, request: Request):
        count = len(self.service_limits)
        if count == 0:
            return self.name
        rule_word = "rule" if count == 1 else "rules"
        return f"{self.name} ({count} {rule_word})"

class ServiceLimit(Base):
    __tablename__ = 'service_limits'
    id = Column(Integer, primary_key=True)
    service_id = Column(Integer, ForeignKey('services.id', ondelete="CASCADE"), nullable=False)
    provider_id = Column(Integer, ForeignKey('providers.id', ondelete="CASCADE"), nullable=False)
    country_id = Column(Integer, ForeignKey('countries.id', ondelete="CASCADE"), nullable=False)
    daily_limit = Column(Integer, nullable=False)
    service = relationship("Service", back_populates="service_limits")
    provider = relationship("Provider", back_populates="service_limits")
    country = relationship("Country", back_populates="service_limits")
    __table_args__ = (
        UniqueConstraint('service_id', 'provider_id', 'country_id', name='_service_provider_country_uc'),
        Index('ix_service_limit_lookup', 'service_id', 'provider_id', 'country_id'),
    )
    def __str__(self):
        service_name = self.service.name if self.service else "N/A"
        provider_name = self.provider.name if self.provider else "N/A"
        country_name = self.country.name if self.country else "N/A"
        return f"Limit for {service_name} via {provider_name} in {country_name}: {self.daily_limit}"

class Country(Base):
    __tablename__ = 'countries'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    iso_code = Column(String(10), unique=True, nullable=False, index=True)
    phone_code = Column(String, nullable=False, index=True)
    operators = relationship("Operator", back_populates="country", cascade="all, delete-orphan")
    service_limits = relationship("ServiceLimit", back_populates="country", cascade="all, delete-orphan")
    def __str__(self): return self.name
    async def __admin_repr__(self, request: Request):
        return self.name

class Operator(Base):
    __tablename__ = 'operators'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, index=True)
    country_id = Column(Integer, ForeignKey('countries.id', ondelete="CASCADE"), nullable=False)
    provider_id = Column(Integer, ForeignKey('providers.id', ondelete="CASCADE"), nullable=True)

    # Старое поле (для совместимости, можно оставлять пустым):
    price_eur_cent = Column(Integer, nullable=True, comment="Цена за 1 SMS в евроцентах")

    # Новое точное поле в евро (например 0.017):
    price_eur = Column(Numeric(10, 5), nullable=True, comment="Цена за 1 SMS в евро (десятичная)")

    country = relationship("Country", back_populates="operators")
    provider = relationship("Provider", back_populates="operators")

    def __str__(self):
        return self.name

    async def __admin_repr__(self, request: Request):
        return self.name

class Provider(Base):
    __tablename__ = 'providers'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False, index=True)
    connection_type = Column(String(10), default='outbound', nullable=False, server_default='outbound', index=True)
    smpp_host = Column(String, nullable=False)
    smpp_port = Column(Integer, nullable=False)
    system_id = Column(String, nullable=False)
    password = Column(String, nullable=False)
    system_type = Column(String(50), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    daily_limit = Column(Integer, nullable=True, comment="Total daily limit for provider across all services/countries")
    phone_numbers = relationship("PhoneNumber", back_populates="provider", cascade="all, delete-orphan")
    operators = relationship("Operator", back_populates="provider", cascade="all, delete-orphan")
    service_limits = relationship("ServiceLimit", back_populates="provider", cascade="all, delete-orphan")
    allowed_ips = relationship("ProviderIP", back_populates="provider", cascade="all, delete-orphan")
    def __str__(self): return self.name
    async def __admin_repr__(self, request: Request):
        return self.name

class ProviderIP(Base):
    __tablename__ = 'provider_ips'
    id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('providers.id', ondelete="CASCADE"), nullable=False, index=True)
    ip_cidr = Column(String(45), nullable=False)
    is_active = Column(Boolean, default=True)
    provider = relationship("Provider", back_populates="allowed_ips")
    __table_args__ = (
        UniqueConstraint('provider_id', 'ip_cidr', name='_provider_ip_uc'),
    )
    def __str__(self):
        return f"{self.ip_cidr}"

class PhoneNumber(Base):
    __tablename__ = 'phone_numbers'
    id = Column(Integer, primary_key=True)
    number_str = Column(String, unique=True, nullable=False, index=True)
    provider_id = Column(Integer, ForeignKey('providers.id', ondelete="CASCADE"), nullable=False, index=True)
    is_active = Column(Boolean, default=True, nullable=False)
    is_in_use = Column(Boolean, default=False, nullable=False, index=True)
    country_id = Column(Integer, ForeignKey('countries.id', ondelete="CASCADE"), nullable=False, index=True)
    operator_id = Column(Integer, ForeignKey('operators.id', ondelete="SET NULL"), nullable=True, index=True)
    sort_order = Column(Integer, default=0, nullable=False, index=True)
    provider = relationship("Provider", back_populates="phone_numbers")
    country = relationship("Country")
    operator = relationship("Operator")
    sessions = relationship("Session", back_populates="phone_number", cascade="all, delete-orphan")
    def __str__(self): return self.number_str

    __table_args__ = (
        Index('ix_phone_numbers_number_str_gin', 'number_str', postgresql_using='gin', postgresql_ops={'number_str': 'gin_trgm_ops'}),
        Index('idx_pn_free_c_p_id', 'country_id', 'provider_id', 'id', postgresql_where=sa.text('is_active IS TRUE AND is_in_use IS FALSE')),
        Index('idx_pn_free_c_op_p_id', 'country_id', 'operator_id', 'provider_id', 'id', postgresql_where=sa.text('is_active IS TRUE AND is_in_use IS FALSE')),
        Index('idx_pn_number_str_prefix', 'number_str', postgresql_ops={'number_str': 'text_pattern_ops'}),
    )

class PhoneNumberUsage(Base):
    __tablename__ = 'phone_number_usage'
    id = Column(Integer, primary_key=True)
    phone_number_id = Column(Integer, ForeignKey('phone_numbers.id', ondelete="CASCADE"), nullable=False, index=True)
    service_id = Column(Integer, ForeignKey('services.id', ondelete="CASCADE"), nullable=False, index=True)
    usage_count = Column(Integer, default=1, nullable=False)
    last_used_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    __table_args__ = (Index('ix_phone_service_usage', 'phone_number_id', 'service_id', unique=True),)

class Session(Base):
    __tablename__ = 'sessions'
    id = Column(Integer, primary_key=True)
    phone_number_str = Column(String, nullable=False, index=True)
    status = Column(Integer, nullable=False, default=1, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    service_id = Column(Integer, ForeignKey('services.id', ondelete="CASCADE"), nullable=False, index=True)
    phone_number_id = Column(Integer, ForeignKey('phone_numbers.id', ondelete="CASCADE"), nullable=False, index=True)
    api_key_id = Column(Integer, ForeignKey('api_keys.id', ondelete="CASCADE"), nullable=False, index=True)
    service = relationship("Service", back_populates="sessions")
    phone_number = relationship("PhoneNumber", back_populates="sessions")
    api_key = relationship("ApiKey", back_populates="sessions")
    sms_messages = relationship("SmsMessage", back_populates="session", cascade="all, delete-orphan")
    __table_args__ = (
        Index('ix_session_cleanup', 'status', 'created_at'),
        Index('idx_sessions_phone_number_id', 'phone_number_id'),
        Index('idx_sessions_service_id', 'service_id'),
    )

class SmsMessage(Base):
    __tablename__ = 'sms_messages'
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id', ondelete="CASCADE"), nullable=False, index=True)
    source_addr = Column(String, nullable=False, index=True)
    text = Column(Text, nullable=False)
    code = Column(String(20), nullable=True, index=True)
    received_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    session = relationship("Session", back_populates="sms_messages")
    def __str__(self) -> str:
        return f'{self.text[:50]}...' if len(self.text) > 50 else self.text
    @hybrid_property
    def phone_number(self) -> str | None:
        return self.session.phone_number_str if self.session else None
    @phone_number.expression
    def phone_number(cls):
        return (
            select(Session.phone_number_str)
            .where(Session.id == cls.session_id)
            .scalar_subquery()
        )
    __table_args__ = (
        Index('idx_sms_session_received', 'session_id', 'received_at'),
        Index('idx_sms_received', 'received_at'),
    )

class ApiKey(Base):
    __tablename__ = 'api_keys'
    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True, nullable=False, index=True, default=lambda: uuid.uuid4().hex)
    description = Column(String)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    sessions = relationship("Session", back_populates="api_key", cascade="all, delete-orphan")
    def __str__(self): return self.description or self.key

class Admin(Base):
    __tablename__ = 'admins'
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    password = Column(String(255), nullable=False)
    last_login = Column(DateTime)
    avatar = Column(String(255))
    def __str__(self): return self.username

class OrphanSms(Base):
    __tablename__ = 'orphan_sms'
    client_ip = Column(String(45), index=True, nullable=True)
    system_id = Column(String(64), index=True, nullable=True)
    id = Column(Integer, primary_key=True)
    phone_number_str = Column(String, nullable=False, index=True)
    source_addr = Column(String, nullable=False, index=True)
    text = Column(Text, nullable=False)
    received_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    provider_id = Column(Integer, ForeignKey('providers.id', ondelete="SET NULL"), nullable=True, index=True)
    country_id = Column(Integer, ForeignKey('countries.id', ondelete="SET NULL"), nullable=True, index=True)
    operator_id = Column(Integer, ForeignKey('operators.id', ondelete="SET NULL"), nullable=True, index=True)
    def __str__(self):
        preview = self.text[:50] + ('...' if len(self.text) > 50 else '')
        return f"{self.source_addr} → {self.phone_number_str}: {preview}"
    __table_args__ = (
        Index('idx_orphan_received', 'received_at'),
        Index('idx_orphan_filters', 'provider_id', 'country_id', 'operator_id', 'received_at'),
        Index('idx_orphan_sender', 'source_addr'),
        Index('idx_orphan_phone', 'phone_number_str'),
    )

