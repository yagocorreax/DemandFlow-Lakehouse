# DemandFlow Lakehouse

Projeto de Engenharia de Dados criado para **simular localmente uma arquitetura Databricks + AWS**

## Objetivo

Construir um pipeline completo de dados utilizando ferramentas gratuitas rodando na infraesstrutura do Docker.

O projeto simulará:

- **Databricks:** Apache Spark, Delta Lake, arquitetura Medalhão, workflows e processamento incremental;
- **AWS:** S3, IAM, Secrets Manager e outros serviços por meio do LocalStack.

## Arquitetura

```text
PostgreSQL
    ↓
Debezium
    ↓
Apache Kafka
    ↓
Apache Spark
    ↓
LocalStack S3
    ↓
Raw → Bronze → Silver → Gold
    ↓
Trino
    ↓
Apache Superset
```

## Tecnologias

- Python
- PostgreSQL
- Docker
- LocalStack (AWS enviroment)
- Apache Kafka
- Debezium
- Apache Spark
- Delta Lake
- Apache Airflow
- Trino
- Apache Superset
- Pytest
- GitHub Actions

## Contexto

A aplicação representará uma empresa fictícia com dados de:

- produtos;
- vendas;
- pedidos;
- estoque;
- lojas;
- promoções;
- previsões de demanda.clear

## Status

## Status

- [x] PostgreSQL transacional
- [x] Gerador de dados
- [x] LocalStack com S3 persistente
- [x] Spark + Delta Lake
- [x] CDC com Kafka e Debezium
- [x] Camada Raw imutável
- [x] Bronze Events e Bronze Current
- [x] Silver + Data Quality + Quarantine
- [x] Gold Analytics
- [x] Hive Metastore + Trino
- [x] Dashboard analítico com Apache Superset
- [ ] Orquestração end-to-end com Apache Airflow

**Em desenvolvimento**