# Migração do armazenamento para MinIO AIStor Free

## Configuração

| Item | Configuração |
| --- | --- |
| Serviço / container | `minio` / `demandflow-minio` |
| Imagem | `quay.io/minio/aistor/minio:RELEASE.2026-09-19T17-05-25Z` |
| API no host | `http://127.0.0.1:9000` |
| Console no host | `http://127.0.0.1:9001` |
| Endpoint entre containers | `S3_INTERNAL_ENDPOINT=http://minio:9000` |
| Dados | volume `demandflow_minio_data` em `/mnt/data` |
| Licença | secret `minio_license`, somente leitura em `/run/secrets/minio_license` |
| Prontidão | `mc ready local`; HTTP `/minio/health/ready` |
| Orçamento inicial | 1 GiB RAM, 1 CPU; medir e ajustar após o teste isolado |

Em nó único, o volume conserva dados entre reinícios,
mas não oferece alta disponibilidade nem substitui backup. O transporte aqui permanece HTTP na rede local
do Docker, com portas do host limitadas ao loopback.

## Licença e credenciais fora do projeto

No `.env` local, estão configurados apenas o caminho e o endpoint novos:

```dotenv
DEMANDFLOW_SECRETS_DIR=C:/Users/jcpre/.demandflow-secrets
S3_INTERNAL_ENDPOINT=http://minio:9000
```

Conteúdo esperado do diretório externo:

```text
.demandflow-secrets/
  minio.license          
  minio-root-user        
  minio-root-password    
  s3.env                
```

`scripts/initialize-storage-secrets.ps1` prepara os três últimos arquivos
quando for executado na etapa autorizada de preparação. Usa aleatoriedade
criptográfica; não imprime valores, não sobrescreve arquivos existentes e
interrompe se encontrar um conjunto parcial. O script não inicia containers.
A licença é somente verificada quanto à existência, nunca lida pelo script.
Sua aceitação e validade só serão confirmadas pelo AIStor em execução.

O administrador é passado ao AIStor por secrets com `MINIO_ROOT_USER_FILE`
e `MINIO_ROOT_PASSWORD_FILE`. Spark, Hive, Trino e Airflow recebem somente
`AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY` da conta do pipeline via
`env_file`, sem usar as antigas credenciais `test`.

No Docker Compose local, secrets baseados em arquivo são montagens de leitura:
isso não criptografa os arquivos no host. Proteja esse diretório com as permissões
do Windows. As credenciais de aplicação continuam disponíveis ao administrador
do Docker via inspeção de ambiente; não publique `docker inspect` ou
`docker compose config` completos. No Airflow, `private_environment` evita
incluí-las no campo de ambiente renderizado da tarefa, mas não é um cofre.

## Buckets e permissões

Os seis nomes e os caminhos existentes `s3://` / `s3a://` foram preservados:

- `demandflow-raw`
- `demandflow-bronze`
- `demandflow-silver`
- `demandflow-gold`
- `demandflow-quarantine`
- `demandflow-checkpoints`

`scripts/bootstrap-s3.ps1` chama `infra/minio/bootstrap.sh` dentro do
próprio container AIStor, usando o cliente `mc`. Cria os buckets de forma
idempotente, provisiona a conta de aplicação e aplica
`infra/minio/lakehouse-policy.json`. Não usa STS nem um container auxiliar.
A política permite listar os seis buckets conhecidos, obter sua região e
ler/gravar/excluir seus objetos, com operações multipart necessárias ao S3A.
Não concede administração, criação/exclusão de buckets ou acesso a outros
buckets. A exclusão de objetos é necessária para regravações e checkpoints.
Não habilita acesso anônimo nem versionamento.

## Integrações adaptadas

- Os nove jobs/validadores Spark usam `S3_INTERNAL_ENDPOINT`. O smoke não tem
  fallback de credenciais. A imagem `demandflow-spark:3.5.9` deve ser reconstruída
  pelo Dockerfile existente antes de executar DAGs, pois o Airflow usa o código
  copiado na imagem.
- Hive recebe endpoint/região pelo ambiente e usa o provider de credenciais
  AWS do ambiente; não guarda chaves em `core-site.xml`. Aguarda também o init
  do seu volume antes de iniciar.
- Trino recebe endpoint/região/credenciais pelo ambiente. O início do serviço
  inclui a dependência do metastore.
- Airflow encaminha endpoint e credenciais ao Spark e verifica a prontidão S3
  por HTTP, em vez de apenas verificar a antiga porta TCP.
- Scripts PowerShell usam `start-storage.ps1`, que inicia apenas o AIStor
  e espera até 120 segundos pelo healthcheck. O parâmetro `-Bootstrap`
  prepara os buckets nos fluxos de escrita; validadores não recriam buckets.

## Validação estática (sem Docker Engine)

```powershell
docker compose config --no-env-resolution --quiet
python -B -m unittest discover -s tests -p test_storage_migration.py -v
powershell -NoProfile -File scripts/validate-static-syntax.ps1
git diff --check
```

`--no-env-resolution` permite validar a estrutura mesmo antes da criação de
`s3.env`. O teste verifica os vínculos entre consumidores, secrets, política
e buckets e analisa sintaxe Python/XML. Não autentica no S3 nem valida a
licença. A validação completa de configuração (`config --quiet`) deve ocorrer
depois de preparar as credenciais.

## Resultado da validação em 06/10/2026

- Oito testes de consistência estática passaram.
- Os 19 scripts PowerShell passaram pelo parser, sem execução de seus comandos.
- O bootstrap shell passou em `sh -n` (somente sintaxe).
- Compose validado com perfis processing/query/orchestration e
  `--no-env-resolution`; `git diff --check` aprovado.
- Confirmado que licença e arquivos de credenciais estão cobertos pelo
  `.gitignore`; adicionadas também exclusões no contexto de build.
- `s3.env` ainda não existe: o gerador foi preparado, mas não executado.
  Portanto não houve criação de credenciais, usuários, buckets ou volume
  Docker nesta etapa. O arquivo de licença existente não foi alterado.

O Git informou um aviso preexistente ao acessar o link
`airflow/logs/dag_processor/latest`; esse log não participa das alterações.

## Próximos testes, sempre por blocos autorizados

1. Preparar os arquivos externos de credenciais; validar o Compose completo.
2. Baixar a imagem fixada e iniciar apenas AIStor. Verificar licença sem expor
   seu conteúdo, healthcheck, portas e consumo.
3. Executar bootstrap, criar um pequeno objeto de teste, ler seu conteúdo,
   parar/reiniciar o mesmo container e comparar buckets e objeto. Validar
   também que a conta do pipeline não acessa outros buckets nem administra
   usuários. Parar AIStor ao encerrar o bloco.
4. Com nova autorização, reconstruir a imagem Spark e testar escrita, leitura,
   MERGE e histórico Delta com AIStor como dependência. Parar o processamento.
5. Com nova autorização, validar Hive/Trino; depois Airflow em bloco próprio.

Os scripts de pipeline completo não devem ser usados como teste isolado:
eles ainda coordenam múltiplos serviços. Não executar `up` da stack inteira
nem remover volumes durante essa validação.

## Dados antigos e retorno

O serviço LocalStack saiu do Compose, mas a declaração de seu volume permanece.
Nenhum container, imagem ou volume antigo foi excluído. O novo volume é separado:
os formatos internos de LocalStack e AIStor não são intercambiáveis. Esta etapa
não migra objetos nem prova que todos os dados antigos estão vazios.

Em caso de falha, pare AIStor e revise a causa antes de continuar. A configuração
anterior está no histórico do Git; voltar requer restaurar conjuntamente Compose,
jobs, scripts e catálogo (com aprovação), e reaplicar as variáveis antigas.
Não reutilize o volume AIStor como volume LocalStack nem rode `down -v`.
O container LocalStack antigo pode aparecer como órfão; não removê-lo
automaticamente. Suas antigas variáveis no `.env` foram preservadas e não são
consumidas pela nova configuração.

## Fontes oficiais

- [AIStor em container e licença Free](https://docs.min.io/aistor/installation/container/install/)
- [Configuração por arquivos de credenciais](https://docs.min.io/aistor/reference/aistor-server/settings/)
- [Identidades locais e políticas](https://docs.min.io/aistor/administration/iam/identity/built-in-identity/)
- [Expansão de ambiente no Hadoop](https://hadoop.apache.org/docs/r3.3.6/api/org/apache/hadoop/conf/Configuration.html)
- [Validação do Docker Compose](https://docs.docker.com/reference/cli/docker/compose/config/)
