import asyncio
from database import Base, apolice_cobertura, Apolice, Avaliacao, Cliente, Cobertura, Imovel, Pagamento, Sinistro
from datetime import date, datetime
from faker import Faker
import os
import random
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()

fake = Faker('pt_BR')
today = date.today()


def insert_clientes(session, total):
    clientes = []

    for i in range(total):
        name = fake.name()
        address = fake.address()

        clientes.append(Cliente(
            nome=name,
            dt_nasc=fake.date_of_birth(minimum_age=18),
            endereco=address,
            telefone=fake.phone_number(),
            email=name.replace(" ", "").lower() + "@" + fake.domain_name()
        ))

    session.add_all(clientes)
    session.flush()

    return clientes


def insert_imoveis(session, clientes, tipos_imovel):
    imoveis = []

    for cliente in clientes:
        for _ in range(randint_min(1, 4)):
            valor_random = random.random()
            valor_imovel = valor_random * 1000000
            ano_construcao = int(fake.date_between(date(2021, 1, 1), today).year)

            imoveis.append(Imovel(
                id_proprietario=cliente.id_cliente,
                id_inquilino=cliente.id_cliente,
                endereco=cliente.endereco if _ == 0 else fake.address(),
                tipo_imovel=random.choice(tipos_imovel),
                valor_imovel=valor_imovel,
                area_imovel=random.random() * valor_random * 2500,
                ano_construcao=ano_construcao
            ))

    session.add_all(imoveis)
    session.flush()

    return imoveis


def insert_avaliacoes_apolices(session, imoveis):
    avaliacoes = []
    apolices = []

    for imovel in imoveis:
        data_inicio = fake.date_between(date(imovel.ano_construcao, 1, 1), date.today())

        avaliacoes.append(Avaliacao(
            id_imovel=imovel.id_imovel,
            dt_avaliacao=data_inicio,
            valor_avaliado=imovel.valor_imovel
        ))

        apolices.append(Apolice(
            id_imovel=imovel.id_imovel,
            dt_inicio=data_inicio,
            dt_termino=fake.date_between(data_inicio, date(2050, 12, 31))
        ))

    session.add_all(avaliacoes)
    session.add_all(apolices)
    session.flush()

    return apolices


async def insert_sinistros_pagamentos_apolices_coberturas(session, apolices, imoveis, coberturas):
    sinistros = []
    pagamentos = []
    apolice_coberturas = []

    i = 0
    for apolice in apolices:
        coberturas_inseridas = random.sample(coberturas, randint_min(1, 10))
        await insert_apolice_coberturas(apolice, apolice_coberturas, coberturas_inseridas, imoveis, i)
        await asyncio.gather(
            insert_sinistros(apolice, sinistros, coberturas_inseridas),
            insert_pagamentos(apolice, pagamentos))
        i += 1

    session.add_all(sinistros)
    session.add_all(pagamentos)
    session.execute(apolice_cobertura.insert(), apolice_coberturas)
    session.flush()

    return sinistros, pagamentos, apolice_coberturas


async def insert_apolice_coberturas(apolice, apolice_coberturas, coberturas_inseridas, imoveis, i):
    valor_apolice = 0.0

    for cobertura in coberturas_inseridas:
        apolice_coberturas.append({
            'id_apolice': apolice.id_apolice,
            'id_cobertura': cobertura.id_cobertura
        })
        valor_apolice += float(cobertura.valor)

    apolice.valor_apolice = (valor_apolice * (imoveis[i].valor_imovel * 0.0001)) / 2


async def insert_sinistros(apolice, sinistros, coberturas_inseridas):
    for _ in range(randint_min(0, 4)):
        cobertura_utilizada = random.choice(coberturas_inseridas)
        sinistros.append(Sinistro(
            id_apolice=apolice.id_apolice,
            dt_sinistro=fake.date_between(apolice.dt_inicio, apolice.dt_termino),
            descricao=cobertura_utilizada.descricao,
            valor_sinistro=cobertura_utilizada.valor
        ))


async def insert_pagamentos(apolice, pagamentos):
    dt_pagamento = apolice.dt_inicio
    while dt_pagamento <= today:
        pagamentos.append(Pagamento(
            id_apolice=apolice.id_apolice,
            dt_pagamento=dt_pagamento,
            valor_pagamento=apolice.valor_apolice
        ))
        dt_pagamento = date(dt_pagamento.year + (dt_pagamento.month // 12), (dt_pagamento.month % 12) + 1, 5)


def randint_min(a, b):
    return min(random.randint(a, b), random.randint(a, b))


def log(message):
    print(f"{datetime.now()} - {message}")


async def main():
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
        }
    )

    engine = create_engine(connection_url)
    Session = sessionmaker(bind=engine)

    Base.metadata.create_all(engine)

    log("Conectado!")

    tipos_imovel = [
        "Casa",
        "Apartamento"
    ]

    coberturas = [
        Cobertura(descricao="Incêndio", valor=500.00),
        Cobertura(descricao="Roubo", valor=100.00),
        Cobertura(descricao="Danos Elétricos", valor=100.00),
        Cobertura(descricao="Vendaval", valor=200.00),
        Cobertura(descricao="Desmoronamento", valor=300.00),
        Cobertura(descricao="Responsabilidade Civil", valor=200.00),
        Cobertura(descricao="Perda de Aluguel", valor=10.00),
        Cobertura(descricao="Inundação", valor=350.00),
        Cobertura(descricao="Furto", valor=80.00),
        Cobertura(descricao="Desastres Naturais", valor=400.00),
    ]

    session = Session()

    for cobertura in coberturas:
        existing_cobertura = session.query(Cobertura).filter_by(descricao=cobertura.descricao).first()
        if not existing_cobertura:
            session.add(cobertura)

    session.commit()
    log(f"Coberturas inseridas.")

    total_clients = 100

    clientes = insert_clientes(session, total_clients)
    log(f"{len(clientes)} clientes inseridos.")

    imoveis = insert_imoveis(session, clientes, tipos_imovel)
    log(f"{len(imoveis)} imóveis inseridos.")

    apolices = insert_avaliacoes_apolices(session, imoveis)
    log(f"{len(apolices)} apólices e avaliações inseridas.")

    sinistros, pagamentos, apolice_coberturas = await insert_sinistros_pagamentos_apolices_coberturas(session, apolices, imoveis, coberturas)
    log(f"{len(sinistros)} sinistros, {len(pagamentos)} pagamentos, {len(apolice_coberturas)} apolice_coberturas inseridos.")

    session.commit()
    log("Todos registros salvos.")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
