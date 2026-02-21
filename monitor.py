import time
import requests
from datetime import datetime

import redis
from pymongo import MongoClient
from cassandra.cluster import Cluster
from neo4j import GraphDatabase

REDIS_HOST = "localhost"
REDIS_PORT = 6379
MONGO_URI = "mongodb://localhost:27018/"
CASSANDRA_HOSTS = ["127.0.0.1"]
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "senha123"

SYMBOLS = [
    ("BTCUSDT", "BTC", "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"),
    ("ETHUSDT", "ETH", "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT"),
]
REDIS_TTL = 8
INTERVALO_LOOP = 10
INVESTIDORES = ["Paulo", "Ana", "João"]


def conectar_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def conectar_mongo():
    return MongoClient(MONGO_URI)


def conectar_cassandra():
    cluster = Cluster(CASSANDRA_HOSTS, protocol_version=4)
    return cluster.connect()


def conectar_neo4j():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def setup_cassandra(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS cotacoes
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace("cotacoes")
    session.execute("""
        CREATE TABLE IF NOT EXISTS historico_precos (
            moeda text,
            momento timestamp,
            valor decimal,
            PRIMARY KEY (moeda, momento)
        ) WITH CLUSTERING ORDER BY (momento DESC)
    """)


def setup_neo4j(driver):
    with driver.session() as session:
        for _, codigo, _ in SYMBOLS:
            session.run("MERGE (m:Moeda {symbol: $s})", s=codigo)
        for nome in INVESTIDORES:
            session.run("MERGE (i:Investidor {nome: $n})", n=nome)
        for nome in INVESTIDORES:
            for _, codigo, _ in SYMBOLS:
                session.run("""
                    MATCH (i:Investidor {nome: $nome}), (m:Moeda {symbol: $symbol})
                    MERGE (i)-[:ACOMPANHA]->(m)
                """, nome=nome, symbol=codigo)


def buscar_preco_api(url):
    try:
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"   Erro na API: {e}")
        return None


def indicador_variacao(preco_ant, preco_novo):
    if preco_ant is None:
        return "⚪", ""
    if preco_novo > preco_ant:
        return "🟢", "(Subiu)"
    if preco_novo < preco_ant:
        return "🔴", "(Caiu)"
    return "⚪", "(Igual)"


def ciclo_monitoramento(redis_client, mongo_db, cassandra_session, neo4j_driver, ultimo_preco):
    for symbol_key, codigo, url in SYMBOLS:
        redis_key = f"cotacao:{symbol_key}"
        valor_cache = redis_client.get(redis_key)

        print(f"\n{codigo} ({symbol_key})...")

        if valor_cache is not None:
            preco = float(valor_cache)
            payload = {"symbol": symbol_key, "price": valor_cache}
            print(f"   Preço veio do cache.")
        else:
            payload = buscar_preco_api(url)
            if not payload:
                continue
            preco = float(payload["price"])
            redis_client.setex(redis_key, REDIS_TTL, payload["price"])
            print(f"   Cache vazio, buscou na API e atualizou (TTL {REDIS_TTL}s).")

        preco_anterior = ultimo_preco.get(codigo)
        emoji, texto = indicador_variacao(preco_anterior, preco)
        ultimo_preco[codigo] = preco
        print(f"   {codigo}: $ {preco:,.2f} {emoji} {texto}")

        try:
            doc = {**payload, "data_coleta": datetime.now()}
            mongo_db.datalake.insert_one(doc)
            print(f"   Mongo: payload bruto salvo.")
        except Exception as e:
            print(f"   Mongo: {e}")

        try:
            cassandra_session.execute(
                "INSERT INTO cotacoes.historico_precos (moeda, momento, valor) VALUES (%s, %s, %s)",
                (codigo, datetime.now(), preco)
            )
            print(f"   Cassandra: preço na série temporal.")
        except Exception as e:
            print(f"   Cassandra: {e}")

        try:
            with neo4j_driver.session() as session:
                result = session.run("""
                    MATCH (i:Investidor)-[:ACOMPANHA]->(m:Moeda {symbol: $symbol})
                    RETURN i.nome AS nome
                """, symbol=codigo)
                nomes = [record["nome"] for record in result]
            if nomes:
                print(f"   Neo4j: notificando {', '.join(nomes)}.")
            else:
                print(f"   Neo4j: ninguém acompanha esta moeda.")
        except Exception as e:
            print(f"   Neo4j: {e}")


def main():
    print("Plataforma Inteligência de Mercado (Opção B - Cripto)\n")

    try:
        redis_client = conectar_redis()
        redis_client.ping()
        print("Redis: conectado.")
    except Exception as e:
        print(f"Redis não disponível: {e}. Rode: docker-compose up -d")
        return

    try:
        mongo_client = conectar_mongo()
        mongo_client.admin.command("ping")
        mongo_db = mongo_client.cotacoes
        print("MongoDB: conectado.")
    except Exception as e:
        print(f"MongoDB não disponível: {e}")
        return

    try:
        cassandra_session = conectar_cassandra()
        setup_cassandra(cassandra_session)
        print("Cassandra: conectado, tabela historico_precos ok.")
    except Exception as e:
        print(f"Cassandra não disponível: {e}")
        return

    try:
        neo4j_driver = conectar_neo4j()
        neo4j_driver.verify_connectivity()
        setup_neo4j(neo4j_driver)
        print("Neo4j: conectado, grafo de investidores/moedas pronto.")
    except Exception as e:
        print(f"Neo4j não disponível: {e}")
        return

    print("\nMonitorando (Ctrl+C para sair).\n")
    ultimo_preco = {}
    try:
        while True:
            ciclo_monitoramento(redis_client, mongo_db, cassandra_session, neo4j_driver, ultimo_preco)
            time.sleep(INTERVALO_LOOP)
    except KeyboardInterrupt:
        print("\nEncerrado.")


if __name__ == "__main__":
    main()
