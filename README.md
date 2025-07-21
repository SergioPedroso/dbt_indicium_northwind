# Desafio Técnico Indicium
### Autor: Sérgio Pedroso
**Pipeline Analítico & Predição de Churn (Northwind)**

> **Link do Dashboard Looker:** _[https://lookerstudio.google.com/embed/u/0/reporting/b8ca9748-c90c-4739-a745-b1f79d26c94c/page/rxdRF]_

## 🔍 Visão Geral
Este repositório demonstra um pipeline de dados de ponta a ponta, englobando:

- **Ingestão** de dados Northwind (CSV → BigQuery)  
- **Transformações ELT** em **dbt** (camadas `stg`, `int`, `dim`, `fact`)  
- **Modelagem preditiva** de churn em **Python**  
- **Visualização executiva** no Looker Studio  

Objetivos de negócio:
1. Identificar clientes com alto risco de churn.  
2. Sugerir estratégias para elevar o ticket médio.

## ⚙️ Stack
| Camada | Ferramenta |
|--------|------------|
| Armazenamento | Google Cloud Storage |
| Data Warehouse | BigQuery |
| ELT | dbt‑core + dbt‑bigquery |
| Machine Learning | Python (scikit‑learn, xgboost) |
| Visualização | Looker Studio |

