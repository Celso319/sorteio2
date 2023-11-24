import psycopg2
import random

def sortear_pessoas(conexao, numero_sorteios):
    # Dicionário para armazenar o número de participantes por organização
    participantes_por_organizacao = {}

    # Lista para armazenar as pessoas sorteadas
    pessoas_sorteadas = []

    # Cria uma conexão com o banco de dados
    with psycopg2.connect(conexao) as conn:
        with conn.cursor() as cursor:
            # Consulta para obter os dados necessários
            cursor.execute("SELECT cod_municipio_cadunico, nome_cadunico FROM goodcheck")

            # Processa os resultados da consulta
            for organizacao, nome in cursor.fetchall():
                participantes_por_organizacao[organizacao] = participantes_por_organizacao.get(organizacao, []) + [nome]

    # Calcula a probabilidade de escolher uma pessoa de cada organização
    probabilidades = {org: len(participantes) / sum(len(participantes) for participantes in participantes_por_organizacao.values()) for org, participantes in participantes_por_organizacao.items()}

    # Cria uma conexão com o banco de dados novamente
    with psycopg2.connect(conexao) as conn:
        with conn.cursor() as cursor:
            # Realiza os sorteios
            for _ in range(numero_sorteios):
                # Escolhe aleatoriamente uma organização com base nas probabilidades
                organizacao_sorteada = random.choices(list(probabilidades.keys()), list(probabilidades.values()))[0]

                # Filtra as pessoas para incluir apenas participantes da organização sorteada que ainda não foram sorteados
                pessoas_organizacao = participantes_por_organizacao[organizacao_sorteada]
                pessoas_nao_sorteadas = [pessoa for pessoa in pessoas_organizacao if pessoa not in pessoas_sorteadas]

                # Se todas as pessoas já foram sorteadas, escolhe aleatoriamente entre todas as pessoas da organização
                if not pessoas_nao_sorteadas:
                    pessoas_nao_sorteadas = pessoas_organizacao

                # Escolhe aleatoriamente uma pessoa da organização sorteada
                pessoa_sorteada = random.choice(pessoas_nao_sorteadas)

                # Adiciona a pessoa sorteada à lista de sorteados
                pessoas_sorteadas.append(pessoa_sorteada)

    return pessoas_sorteadas

# Substitua os valores abaixo pelos dados do seu banco de dados
conexao_banco = "dbname=cadunico user=sagaroso password=sucesso host=192.168.100.104 port=5432"

# Número de sorteios desejado (3000 no seu caso)
numero_sorteios = 3000

# Chama a função para realizar os sorteios
pessoas_sorteadas = sortear_pessoas(conexao_banco, numero_sorteios)

# Exibe as pessoas sorteadas
for i, pessoa in enumerate(pessoas_sorteadas, start=1):
    print(f'Sorteio {i}: {pessoa}')
