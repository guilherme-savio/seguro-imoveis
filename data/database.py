from sqlalchemy import create_engine, Column, Integer, String, Date, DECIMAL, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()

class Cliente(Base):
    __tablename__ = 'cliente'
    id_cliente = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String(255))
    dt_nasc = Column(Date)
    endereco = Column(String(255))
    telefone = Column(String(255))
    email = Column(String(255))

class Imovel(Base):
    __tablename__ = 'imovel'
    id_imovel = Column(Integer, primary_key=True, autoincrement=True)
    id_proprietario = Column(Integer, ForeignKey('cliente.id_cliente'))
    id_inquilino = Column(Integer, ForeignKey('cliente.id_cliente'))
    endereco = Column(String(255))
    tipo_imovel = Column(String(255))
    valor_imovel = Column(DECIMAL(12, 2))
    area_imovel = Column(DECIMAL(12, 2))
    ano_construcao = Column(Integer)
    proprietario = relationship('Cliente', foreign_keys=[id_proprietario])
    inquilino = relationship('Cliente', foreign_keys=[id_inquilino])

class Apolice(Base):
    __tablename__ = 'apolice'
    id_apolice = Column(Integer, primary_key=True, autoincrement=True)
    id_imovel = Column(Integer, ForeignKey('imovel.id_imovel'))
    dt_inicio = Column(Date)
    dt_termino = Column(Date)
    valor_apolice = Column(DECIMAL(12, 2))
    imovel = relationship('Imovel')

class Cobertura(Base):
    __tablename__ = 'cobertura'
    id_cobertura = Column(Integer, primary_key=True, autoincrement=True)
    descricao = Column(String(255))
    valor = Column(DECIMAL(12, 2))

apolice_cobertura = Table(
    'apolice_cobertura', Base.metadata,
    Column('id_apolice', Integer, ForeignKey('apolice.id_apolice')),
    Column('id_cobertura', Integer, ForeignKey('cobertura.id_cobertura'))
)

class Sinistro(Base):
    __tablename__ = 'sinistro'
    id_sinistro = Column(Integer, primary_key=True, autoincrement=True)
    id_apolice = Column(Integer, ForeignKey('apolice.id_apolice'))
    dt_sinistro = Column(Date)
    descricao = Column(String(255))
    valor_sinistro = Column(DECIMAL(12, 2))
    apolice = relationship('Apolice')

class Pagamento(Base):
    __tablename__ = 'pagamento'
    id_pagamento = Column(Integer, primary_key=True, autoincrement=True)
    id_apolice = Column(Integer, ForeignKey('apolice.id_apolice'))
    dt_pagamento = Column(Date)
    valor_pagamento = Column(DECIMAL(12, 2))
    apolice = relationship('Apolice')

class Avaliacao(Base):
    __tablename__ = 'avaliacao'
    id_avaliacao = Column(Integer, primary_key=True, autoincrement=True)
    id_imovel = Column(Integer, ForeignKey('imovel.id_imovel'))
    dt_avaliacao = Column(Date)
    valor_avaliado = Column(DECIMAL(12, 2))
    imovel = relationship('Imovel')