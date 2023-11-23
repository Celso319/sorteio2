import psycopg2
import random

def sortear_participantes(conexao, numero_sorteios):
    # Dicionário para armazenar o número de participantes por organização
    participantes_por_organizacao = {}

    # Lista para armazenar os participantes sorteados
    participantes_sorteados = []

    # Cria uma conexão com o banco de dados
    with psycopg2.connect(conexao) as conn:
        with conn.cursor() as cursor:
            # Consulta para obter os dados necessários
            cursor.execute("SELECT nis_cadunico, nome_cadunico, cod_municipio_cadunico FROM goodcheck")

            # Obtém todos os participantes
            participantes = cursor.fetchall()

            # Conta o número de participantes por organização
            for _, _, organizacao in participantes:
                participantes_por_organizacao[organizacao] = participantes_por_organizacao.get(organizacao, []) + [(_, _, organizacao)]

            # Realiza os sorteios
            for _ in range(numero_sorteios):
                # Escolhe aleatoriamente uma organização com base nas probabilidades
                organizacao_sorteada = random.choices(list(participantes_por_organizacao.keys()), list(map(len, participantes_por_organizacao.values())))[0]

                # Se todos os participantes da organização já foram sorteados, reinicializa a lista de participantes dessa organização
                if not participantes_por_organizacao[organizacao_sorteada]:
                    participantes_por_organizacao[organizacao_sorteada] = [(_, _, organizacao_sorteada) for _, _, org in participantes if org == organizacao_sorteada]

                # Escolhe aleatoriamente um participante da organização sorteada
                participante_sorteado = random.choice(participantes_por_organizacao[organizacao_sorteada])

                # Adiciona o participante sorteado à lista de sorteados
                participantes_sorteados.append(participante_sorteado)

                # Remove o participante sorteado da lista original
                participantes_por_organizacao[organizacao_sorteada].remove(participante_sorteado)

    return participantes_sorteados

# Substitua os valores abaixo pelos dados do seu banco de dados
conexao_banco = "dbname=cadunico user=sagaroso password=sucesso host=192.168.100.104 port=5432"

# Número de sorteios desejado (3000 no seu caso)
numero_sorteios = 3000

# Chama a função para realizar os sorteios
participantes_sorteados = sortear_participantes(conexao_banco, numero_sorteios)

# Exibe os participantes sorteados
for i, (codigo, nome, organizacao) in enumerate(participantes_sorteados, start=1):
    print(f'Sorteio {i}: Código: {codigo}, Nome: {nome}, Organização: {organizacao}')
