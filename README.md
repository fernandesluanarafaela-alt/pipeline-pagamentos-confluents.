# Pipeline de pagamentos em tempo real — Confluent Cloud

Pipeline ponta a ponta: **Neon (Postgres) → CDC (Debezium) → tópicos Avro → Flink SQL (enriquecimento + fraude) → consumo**.
Reproduzível com `./setup.sh` e removível com `./teardown.sh`.

## Status da entrega (transparência)

Este repositório contém **todo o código, a configuração e os roteiros** do pipeline. **Ainda não foi executado contra o Neon nem o Confluent Cloud**: não há prints, saídas de comando, métricas ou custos reais aqui, e nenhum foi inventado.

| Camada | Código/config | Executada? | Evidência |
|---|---|---|---|
| 1. Fundação | `setup.sh` | Não | pendente |
| 2. Contrato | `schemas/` | Não | pendente |
| 3. Ingestão (CDC) | `db/`, `connectors/cdc.json` | Não | pendente |
| 4. Processamento | `sql/` | Não | pendente |
| 5. Operação | seção abaixo | Não | pendente |

Os arquivos `evidencias/capturar.sh` e `teardown.sh` existem para gerar as evidências reais quando o pipeline for executado; este README será atualizado com as saídas. Até lá, o texto abaixo descreve **o que cada camada fará e o que deve ser observado**, não resultados obtidos.

## Como subir do zero

1. Crie um projeto no [Neon](https://neon.tech) (região `aws-us-east-1`) e uma conta no Confluent Cloud.
2. `cp .env.example .env` e preencha (host, usuário admin, senhas). O `.env` está no `.gitignore`.
3. Instale: `confluent` CLI (logado), `psql`, `envsubst` (pacote `gettext`), `python3`.
4. `./setup.sh` — cria ambiente, cluster, identidades, schema/seed no Postgres e o conector CDC.
5. Aplique os SQLs do Flink, em ordem, no workspace SQL do Confluent (ou via `confluent flink statement create`):
   `sql/01_tabelas.sql` → `sql/02_enriquecimento.sql` → `sql/03_fraude_velocity.sql`.
6. `./evidencias/capturar.sh` — roda os testes e salva as saídas em `evidencias/`.
7. **No fim:** `./teardown.sh` (derruba conector, statements, cluster, ambiente e o slot de replicação).

## Por que cada peça

| Peça | Escolha | Por quê |
|---|---|---|
| Postgres | Neon | Gratuito, tem replicação lógica, sobe em minutos. |
| Ingestão | CDC (Debezium) e não batch | Captura insert/update/delete na hora, com o estado anterior; batch perderia deletes e traria latência de minutos. |
| Formato | Avro + Schema Registry | Contrato explícito, evolução controlada, mensagens menores que JSON. |
| Valor monetário | `bytes` + `decimal(15,2)` | Evita erro de ponto flutuante em dinheiro. |
| Processamento | Flink SQL gerenciado | Join temporal e detecção de padrão sem operar cluster. |
| Cluster | Basic, aws/us-east-1 | Menor custo suficiente para o volume (2.000 transações). |

## Camada 1 — Fundação

- Environment `desafio-final` e cluster Basic `desafio-basic` (aws/us-east-1).
- Identidades: `desafio-producer` (WRITE/DESCRIBE/CREATE em `desafio*`), `desafio-consumer` (READ em `desafio*` + grupo `desafio*`). A conta pessoal só é usada em setup/teardown.

**Evidência:** pendente — será gerada em `evidencias/01-iam-acls.txt`.

## Camada 2 — Contrato de dados

Schemas em `schemas/`, namespace `com.bootcamp.payments.v1`, dinheiro como `bytes` + `decimal(15,2)`. Compatibilidade **BACKWARD** testada nos dois sentidos:

- `transaction-v2-compativel.avsc`: campo novo **com default** → deve ser aceito.
- `transaction-v3-incompativel.avsc`: campo obrigatório **sem default** → deve ser rejeitado.

**Evidência (aceita):** pendente — será gerada em `evidencias/02-compat-aceita.txt`.
**Evidência (rejeita):** pendente — será gerada em `evidencias/02-compat-rejeita.txt`.

## Camada 3 — Ingestão (CDC)

Conector `PostgresCdcSourceV2` (`connectors/cdc.json`) com `cdc_user` (só SELECT + REPLICATION) e `heartbeat.interval.ms=60000`. Segredos entram por variáveis de ambiente via `envsubst`; o JSON versionado não contém senhas.

Seed determinístico (`db/02_seed.sql`, derivado de `md5`, sem `random()`): customers 200 · accounts 240 · cards 320 · merchants 60 · transactions 2000.

**Evidência (contagens):** pendente — será gerada em `evidencias/03-seed-contagens.txt`.

Os três tipos de evento (`db/04_eventos_cdc.sql`): `op=c` (before=null), `op=u` (before preenchido graças a `REPLICA IDENTITY FULL`), `op=d` (after=null + tombstone).

**Evidência (envelopes):** pendente — será gerada em `evidencias/01-envelopes-cdc.json`.

## Camada 4 — Processamento (Flink SQL)

- `sql/02_enriquecimento.sql`: join temporal transações ⋈ contas (PK nos dois lados; `sql.tables.scan.idle-timeout=5s`).
- `sql/03_fraude_velocity.sql`: `RULE_VELOCITY` com `MATCH_RECOGNIZE` (`PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND`, `PARTITION BY card_id`).
- Para disparar: `db/03_gatilho_fraude.sql` insere 3 transações no mesmo cartão em ~20 s.

**Evidência:** pendente — será gerada em `evidencias/04-alerta-fraude.txt`.

## Camada 5 — Operação: as quatro respostas

| Pilar | Resposta |
|---|---|
| ① Observabilidade | Pico de `received_bytes` e lag por grupo, medidos pela Metrics API. pendente (`evidencias/05-metricas.json`) |
| ② Segurança | Tabela abaixo. |
| ③ Custos | pendente (`evidencias/05-custos.txt`) + 2 alavancas (abaixo). |
| ④ Confiabilidade | Tabela abaixo. |

**② Segurança — SA → ACLs → porquê**

| Identidade | ACLs | Por quê |
|---|---|---|
| `desafio-producer` | WRITE, DESCRIBE, CREATE em tópicos `desafio*` (prefixo) | O conector só precisa criar e escrever os tópicos do pipeline. |
| `desafio-consumer` | READ, DESCRIBE em tópicos `desafio*`; READ no grupo `desafio*` | Só lê; não escreve, não administra. |
| conta pessoal | administrador | Usada apenas em setup/teardown. |
| `cdc_user` (Postgres) | SELECT + REPLICATION | Menor privilégio no banco; sem escrita nas tabelas de negócio. |

**③ Custos — 2 alavancas:** (1) rodar o cluster Basic só durante o teste e derrubar com `teardown.sh`; (2) retenção curta (`kafka.retention.time = 1 d`) nos tópicos derivados do Flink e um único task de conector.

**④ Confiabilidade — trecho a trecho**

| Trecho | Garantia | Risco / mitigação |
|---|---|---|
| Postgres → conector | Slot de replicação + heartbeat | Slot parado acumula WAL; heartbeat de 60 s e remoção do slot no teardown. |
| Conector → tópico | At-least-once | Duplicatas possíveis; chaves = PK da linha. |
| Tópico → Flink | Checkpoints do serviço gerenciado | Join temporal exige watermark; idle-timeout evita travar. |
| Flink → alerta | Gravação no tópico | Regra determinística por janela. |
| **Elo fraco: consumidor → efeito** | At-least-once | **Idempotência é código do consumidor**: deduplicar por `card_id + last_ts` antes de agir. |

## Custo total e teardown

Custo real: **não medido** (pipeline ainda não executado).

Ao executar o pipeline, rode `./teardown.sh` no final; a verificação de listas vazias (`confluent connect cluster list` / `confluent flink statement list` → `None found.`) fica em `evidencias/06-teardown.txt`:

```bash
./teardown.sh 2>&1 | tee evidencias/06-teardown.txt
```

## Estrutura

```
setup.sh · teardown.sh · .env.example
db/          schema, seed determinístico, gatilho de fraude, eventos CDC
schemas/     Avro (base, v2 compatível, v3 incompatível)
connectors/  cdc.json (sem secrets)
sql/         Flink SQL (tabelas, enriquecimento, fraude)
evidencias/  capturar.sh + saídas reais
```
