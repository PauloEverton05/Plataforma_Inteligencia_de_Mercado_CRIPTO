# Plataforma de Inteligência de Mercado (Cripto)

Backend de monitoramento de cotações em tempo real usando quatro bancos NoSQL: Redis (cache), MongoDB (data lake), Cassandra (série temporal) e Neo4j (rede de investidores e alertas). Cotações via API Binance (BTC e ETH).

## Pré-requisitos

- [Docker](https://www.docker.com/get-started) e Docker Compose
- Python 3.11
- Acesso à internet (API Binance)

## Uso rápido

### 1. Subir os bancos de dados

Na pasta do projeto:

```bash
docker-compose up -d
```

Aguarde cerca de 1 minuto para o Cassandra ficar pronto. Confira com:

```bash
docker ps
```

Todos os quatro containers devem estar com status "Up" (redis-cotacoes, mongo-datalake, cassandra-series, neo4j-alertas).

### 2. Ambiente Python e dependências

Crie e ative uma venv (opcional, mas recomendado):

```bash
# Windows (PowerShell / cmd)
py -m venv .venv
.venv\Scripts\activate

# Linux / macOS
python3 -m venv .venv
source .venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

### 3. Rodar o monitor

```bash
python monitor.py
```

O script conecta nos quatro bancos, cria keyspace/tabela no Cassandra e nós/relacionamentos no Neo4j (se ainda não existirem) e entra em loop: a cada 10 segundos consulta cache (Redis), busca preço na API quando necessário, grava no MongoDB, insere no Cassandra e lista no terminal os investidores a notificar (Neo4j). Use **Ctrl+C** para encerrar.

## Portas e credenciais (Docker)

| Serviço   | Porta no host | Observação              |
|----------|----------------|-------------------------|
| Redis    | 6379           | Sem autenticação        |
| MongoDB  | 27018          | URI: `mongodb://localhost:27018/` |
| Cassandra| 9042           | Sem autenticação        |
| Neo4j    | 7474 (HTTP), 7687 (Bolt) | Usuário: `neo4j`, senha: `senha123` |

## Conexões no DBCode / DBeaver (opcional)

- **Redis:** host `localhost`, porta `6379`.
- **MongoDB:** `mongodb://localhost:27018` (ou host `localhost`, porta `27018`).
- **Cassandra:** host `127.0.0.1`, porta `9042`; keyspace do projeto: `cotacoes`.
- **Neo4j:** host `localhost`, porta `7687`, protocolo `bolt://`, usuário `neo4j`, senha `senha123`.

## Parar os containers

```bash
docker-compose down
```

## Estrutura do repositório

```
.
├── docker-compose.yml   # Redis, MongoDB, Cassandra, Neo4j
├── monitor.py           # Script principal
├── requirements.txt     # Dependências Python
├── trabalho_da_disciplina.md
└── README.md
```

## Disciplina

Banco de Dados NoSQL — Trabalho Final (persistência poliglota).
