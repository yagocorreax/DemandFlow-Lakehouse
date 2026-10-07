# DemandFlow Lakehouse

Projeto de Engenharia de Dados criado para **simular localmente uma arquitetura Databricks + AWS**.

## Objetivo

Construir um pipeline completo de dados utilizando ferramentas gratuitas rodando na infraestrutura do Docker.

O projeto simulará:

- **Databricks:** Apache Spark, Delta Lake, arquitetura Medalhão, workflows e processamento incremental;
- **Armazenamento S3:** MinIO AIStor Free em nó único, com volume local persistente e identidade própria para o pipeline. Não emula os demais serviços AWS.

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
MinIO AIStor (S3)
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
- MinIO AIStor Free (armazenamento compatível com S3)
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
- previsões de demanda.

## Status

- [x] PostgreSQL transacional
- [x] Gerador de dados
- [x] Configuração estática do AIStor com volume persistente
- [x] Licença, autenticação, menor privilégio e persistência do AIStor validados isoladamente
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

## Armazenamento local

Consulte [a migração para AIStor](docs/aistor-migration.md) para a configuração
de licença e credenciais externas, os seis buckets, as validações por blocos
e os limites desta etapa. após a etapa, identidade, política e buckets foram
confirmados após reinício sem repetir o bootstrap. Os scripts antigos de
bootstrap do LocalStack foram substituídos por `scripts/bootstrap-s3.ps1`.

