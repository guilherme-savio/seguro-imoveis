from database import Base, apolice_cobertura, Apolice, Avaliacao, Cliente, Cobertura, Imovel, Pagamento, Sinistro
from datetime import date
from faker import Faker
import os
import random
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL


fake = Faker('pt_BR')

connection_url = URL.create(
    "mssql+pyodbc",
    username=os.getenv("SQL_USERNAME"),
    password=os.getenv("SQL_PASSWORD"),
    host=os.getenv("SQL_SERVER"),
    port=1433,
    database=os.getenv("SQL_DATABASE"),
    query={
        "driver": "ODBC Driver 18 for SQL Server",
        "Encrypt": "yes",
        "TrustServerCertificate": "yes",
    },
)

print(connection_url)

engine = create_engine(connection_url)

Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

print("Conectado!")

def insert_cliente(coberturas, tipos_imovel):
    name = fake.name()
    address = fake.address()

    cliente = Cliente(
        nome=name,
        dt_nasc=fake.date_of_birth(minimum_age=18),
        endereco=address,
        telefone=fake.phone_number(),
        email=name.replace(" ", "").lower() + "@" + fake.domain_name()
    )

    session.add(cliente)
    session.flush()  # Flush to get the client ID without committing

    imoveis = []
    avaliacoes = []
    apolices = []
    sinistros = []
    pagamentos = []
    apolice_coberturas_entries = []

    for _ in range(random.randint(1, 5)):
        valor_random = random.random()
        valor_imovel = valor_random * 1000000
        ano_construcao = fake.year()

        imovel = Imovel(
            id_proprietario=cliente.id_cliente,
            id_inquilino=cliente.id_cliente,
            endereco=address if _ == 0 else fake.address(),
            tipo_imovel=random.choice(tipos_imovel),
            valor_imovel=valor_imovel,
            area_imovel=random.random() * valor_random * 2500,
            ano_construcao=ano_construcao
        )
        imoveis.append(imovel)

    session.bulk_save_objects(imoveis)
    session.flush()

    for imovel in imoveis:
        data_inicio = fake.date_between(date(int(imovel.ano_construcao), 1, 1), date.today())
        data_termino = fake.date_between(data_inicio, date(2050, 12, 31))

        avaliacao = Avaliacao(
            id_imovel=imovel.id_imovel,
            dt_avaliacao=data_inicio,
            valor_avaliado=imovel.valor_imovel
        )
        avaliacoes.append(avaliacao)

        apolice = Apolice(
            id_imovel=imovel.id_imovel,
            dt_inicio=data_inicio,
            dt_termino=data_termino
        )
        apolices.append(apolice)

    session.bulk_save_objects(avaliacoes)
    session.bulk_save_objects(apolices)
    session.flush()

    for apolice in apolices:
        valor_apolice = 0.0
        coberturas_inseridas = random.sample(coberturas, random.randint(1, 10))

        for cobertura in coberturas_inseridas:
            apolice_coberturas_entries.append({
                'id_apolice': apolice.id_apolice,
                'id_cobertura': cobertura.id_cobertura
            })
            valor_apolice += float(cobertura.valor)

        apolice.valor_apolice = (valor_apolice * (imovel.valor_imovel * 0.01)) / 2

        for _ in range(random.randint(0, 5)):
            cobertura_utilizada = random.choice(coberturas_inseridas)
            sinistro = Sinistro(
                id_apolice=apolice.id_apolice,
                dt_sinistro=fake.date_between(apolice.dt_inicio, apolice.dt_termino),
                descricao=cobertura_utilizada.descricao,
                valor_sinistro=cobertura_utilizada.valor
            )
            sinistros.append(sinistro)

        dt_pagamento = apolice.dt_inicio
        while dt_pagamento <= date.today():
            pagamento = Pagamento(
                id_apolice=apolice.id_apolice,
                dt_pagamento=dt_pagamento,
                valor_pagamento=valor_apolice
            )
            pagamentos.append(pagamento)
            dt_pagamento = date(dt_pagamento.year + (dt_pagamento.month // 12), (dt_pagamento.month % 12) + 1, 5)

    session.bulk_save_objects(sinistros)
    session.bulk_save_objects(pagamentos)
    session.execute(apolice_cobertura.insert(), apolice_coberturas_entries)
    session.commit()

if __name__ == '__main__':
    tipos_imovel = [
        "Casa",
        "Apartamento"
    ]

    coberturas = [
        Cobertura(descricao="Incêndio", valor=50000.00),
        Cobertura(descricao="Roubo", valor=10000.00),
        Cobertura(descricao="Danos Elétricos", valor=15000.00),
        Cobertura(descricao="Vendaval", valor=20000.00),
        Cobertura(descricao="Desmoronamento", valor=30000.00),
        Cobertura(descricao="Responsabilidade Civil", valor=25000.00),
        Cobertura(descricao="Perda de Aluguel", valor=1000.00),
        Cobertura(descricao="Inundação", valor=35000.00),
        Cobertura(descricao="Furto", valor=8000.00),
        Cobertura(descricao="Desastres Naturais", valor=40000.00),
    ]

    session.add_all(coberturas)
    session.commit()
    print("Coberturas inseridas")

    total = 10000
    for i in range(total + 1):
        insert_cliente(coberturas, tipos_imovel)
        print(f"Cliente {i + 1}/{total}")