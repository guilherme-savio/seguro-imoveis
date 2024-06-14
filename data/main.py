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

print("Conectado!")

def insert_cliente(coberturas, tipos_imovel):
    name = fake.name()
    address = fake.address()

    cliente = Cliente(
        nome=name,
        dt_nasc=fake.date_of_birth(minimum_age = 18),
        endereco=address,
        telefone=fake.phone_number(),
        email=name.replace(" ", "").lower() + "@" + fake.domain_name()
    )

    session.add(cliente)
    session.commit()

    for i in range(random.randint(1,5)):
        valor_random = random.random()
        valor_imovel = valor_random * 1000000
        ano_construcao = fake.year()

        imovel = Imovel(
            id_proprietario=cliente.id_cliente,
            id_inquilino=cliente.id_cliente,
            endereco=address if i == 0 else fake.address(),
            tipo_imovel=get_random(tipos_imovel),
            valor_imovel=valor_imovel,
            area_imovel=random.random() * valor_random * 2500,
            ano_construcao=ano_construcao
        )

        session.add(imovel)
        session.commit()

        data_inicio = fake.date_between(date(int(ano_construcao), 1, 1), date.today())
        data_termino = fake.date_between(data_inicio, date(2050, 12, 31))

        avaliacao = Avaliacao(
            id_imovel=imovel.id_imovel,
            dt_avaliacao=data_inicio,
            valor_avaliado=valor_imovel
        )

        session.add(avaliacao)
        session.commit()

        apolice = Apolice(
            id_imovel=imovel.id_imovel,
            dt_inicio=data_inicio,
            dt_termino=data_termino
        )

        session.add(apolice)
        session.commit()

        valor_apolice = 0.0
        coberturas_inseridas = []
        for i in range(random.randint(1, 10)):
            cobertura = get_random(coberturas)

            while coberturas_inseridas.__contains__(cobertura):
                cobertura = get_random(coberturas)

            session.execute(
                apolice_cobertura.insert().values(id_apolice=apolice.id_apolice, id_cobertura=cobertura.id_cobertura)
            )
            session.commit()

            coberturas_inseridas.append(cobertura)
            valor_apolice += float(cobertura.valor)
        
        apolice.valor_apolice = (valor_apolice * (valor_imovel * 0.01)) / 2
        session.commit()

        for i in range(random.randint(0,5)):
            cobertura_utilizada = get_random(coberturas_inseridas)

            sinistro = Sinistro(
                id_apolice=apolice.id_apolice,
                dt_sinistro=fake.date_between(data_inicio, data_termino),
                descricao=cobertura_utilizada.descricao,
                valor_sinistro=cobertura_utilizada.valor
            )

            session.add(sinistro)
            session.commit()

        dt_pagamento = data_inicio
        while dt_pagamento <= date.today():
            pagamento = Pagamento(
                id_apolice=apolice.id_apolice,
                dt_pagamento=dt_pagamento,
                valor_pagamento=valor_apolice
            )

            session.add(pagamento)
            session.commit()

            if (dt_pagamento.month == 12):
                dt_pagamento = date(dt_pagamento.year + 1, 1, 5)
            else:
                dt_pagamento = date(dt_pagamento.year, dt_pagamento.month + 1, 5)

def get_random(list):
    return list[random.randint(0, len(list) - 1)]

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

    total = 100 + 1
    for i in range(total):
        insert_cliente(coberturas, tipos_imovel)
        print("Cliente " + str(i + 1) + "/" + str(total - 1))