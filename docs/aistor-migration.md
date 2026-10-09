# Migração do armazenamento para MinIO AIStor Free

## Resumo do estado

| Item | Estado comprovado |
| --- | --- |
| Imagem | `quay.io/minio/aistor/minio:RELEASE.2026-09-19T17-05-25Z` |
| Serviço / contêiner | `minio` / `demandflow-minio` |
| Licença no log | `MinIO Community License`, sem erro de carregamento |
| API / console no host | `http://127.0.0.1:9000` / `http://127.0.0.1:9001` |
| Endpoint entre contêineres | `S3_INTERNAL_ENDPOINT=http://minio:9000` |
| Persistência | volume `demandflow_minio_data` montado em `/mnt/data` |
| Prontidão | `mc ready local` e HTTP `/minio/health/ready` |
| Limite local | 1 CPU e 1 GiB de RAM |
| Bootstrap | seis buckets, identidade separada e política restrita |
| Reinício | buckets, identidade e política persistiram sem reprovisionamento |

O nó único mantém dados entre reinícios, mas não fornece alta disponibilidade
nem substitui backup. O tráfego permanece HTTP dentro da rede Docker local; as
duas portas publicadas estão vinculadas somente ao loopback do host.

## Licença e credenciais externas

O `.env` local contém o caminho externo e o endpoint interno:

```dotenv
DEMANDFLOW_SECRETS_DIR=C:/Users/jcpre/.demandflow-secrets
S3_INTERNAL_ENDPOINT=http://minio:9000
```

Arquivos esperados fora do projeto e fora do OneDrive:

```text
C:/Users/jcpre/.demandflow-secrets/
  minio.license
  minio-root-user
  minio-root-password
  s3.env
```

`scripts/initialize-storage-secrets.ps1` gera credenciais com aleatoriedade
criptográfica, não mostra valores, não sobrescreve um conjunto existente e
interrompe diante de arquivos parciais ou vazios. A licença é preservada.

Na validação, o diretório externo estava acessível somente à conta local,
`SYSTEM` e administradores. Ainda assim, secrets baseados em arquivos no
Compose são montagens de leitura, não um cofre criptografado. Um administrador
do host ou do Docker continua capaz de inspecionar processos e ambientes.

O AIStor recebe o administrador por `MINIO_ROOT_USER_FILE` e
`MINIO_ROOT_PASSWORD_FILE`. Spark, Hive, Trino e Airflow recebem somente a
identidade de aplicação de `s3.env`; eles não recebem a licença nem as
credenciais administrativas.

Não execute `docker compose config` de forma verbosa em logs compartilhados.
Depois da criação de `s3.env`, prefira `docker compose config --quiet`, pois a
renderização completa pode incluir variáveis originadas de `env_file`.

## Buckets e política de menor privilégio

Os nomes utilizados por `s3://` e `s3a://` são exatamente:

- `demandflow-raw`
- `demandflow-bronze`
- `demandflow-silver`
- `demandflow-gold`
- `demandflow-quarantine`
- `demandflow-checkpoints`

Não existe o marcador `dev` nos nomes.

`scripts/bootstrap-s3.ps1` executa `infra/minio/bootstrap.sh` dentro do próprio
AIStor. O bootstrap é idempotente e:

1. valida os arquivos de credenciais sem imprimi-los;
2. cria os seis buckets;
3. cria uma identidade exclusiva para o pipeline;
4. aplica `infra/minio/lakehouse-policy.json`;
5. confirma o acesso da identidade a todos os buckets.

A política permite obter região, listar bucket, ler, gravar e excluir objetos e
executar as operações multipart necessárias ao S3A. Ela não permite administrar
usuários, criar ou excluir buckets, acessar buckets diferentes ou habilitar
acesso anônimo.

### Correção de compatibilidade do bootstrap

A primeira execução de runtime parou antes de criar buckets porque a imagem
minimalista do AIStor não contém `sed`. O parser foi substituído por
`read`/`case` do shell POSIX, sem instalar pacotes ou criar outra imagem.

O parser atual rejeita campos desconhecidos, duplicados, ausentes e valores
fora do formato gerado. Ele não executa `s3.env` como código. Um teste de
regressão impede o retorno do parser externo ou do carregamento inseguro do
arquivo.

## Evidências dos testes de runtime

Os testes foram feitos com somente o AIStor em execução:

- contêiner alcançou `running/healthy`;
- API e console responderam HTTP `200`;
- portas `9000` e `9001` ficaram em `127.0.0.1`;
- arquivo de licença foi montado com o tamanho esperado;
- log identificou `MinIO Community License` sem erro;
- foram encontrados exatamente os seis buckets documentados;
- a identidade do pipeline gravou, leu e excluiu um objeto pequeno;
- administração e criação de bucket foram negadas à identidade do pipeline;
- o objeto e os scripts temporários de teste foram excluídos;
- após parar e iniciar o mesmo contêiner, tudo foi revalidado sem executar o
  bootstrap: identidade, política e seis buckets persistiram;
- ao final, o AIStor foi parado normalmente e nenhum contêiner ficou ativo.

Os testes comprovam o caminho local exercitado. Eles não equivalem a teste de
carga, backup, recuperação de desastre ou alta disponibilidade.

## Recursos e desempenho local

O limite permanece em **1 CPU e 1 GiB** para proteger a máquina local. Na
amostra após a inicialização, o AIStor utilizou aproximadamente 79 MiB e 0,39%
de CPU, sem pressão de memória.

O AIStor alertou que recomenda oito CPUs para comportamento ideal; o contêiner
estava com `GOMAXPROCS=1`. Isso é um trade-off consciente de desenvolvimento:
mais CPU pode aumentar throughput e paralelismo, mas iniciar vários serviços já
causou sobrecarga nesta máquina.

Não há evidência que justifique elevar memória agora. Se Spark demonstrar
gargalo de armazenamento, testar primeiro 2 CPUs somente com AIStor e Spark,
comparar tempo/CPU/memória e voltar ao limite anterior se o host degradar. Não
subir diretamente para oito CPUs nem iniciar a stack inteira para esse teste.

## Integrações configuradas

- Os nove jobs e validadores Spark usam `S3_INTERNAL_ENDPOINT`; o smoke test não
  possui fallback para credenciais de teste.
- A imagem Spark é `demandflow-spark:3.5.9`, construída por
  `infra/spark/Dockerfile` sobre a variante oficial Python sem R, fixada por
  digest.
- A imagem e os bind mounts do Spark incluem somente `src/spark` e
  `config/tables.json`. Generator e Debezium permanecem no projeto e nos seus
  próprios fluxos, mas não ficam acessíveis dentro do contêiner Spark.
- Hive usa endpoint e região do ambiente e
  `EnvironmentVariableCredentialsProvider`; não grava chaves em XML.
- Trino recebe endpoint, região e credenciais pelo ambiente e depende do Hive.
- Airflow encaminha as chaves ao operador por `private_environment` e verifica
  a prontidão HTTP do armazenamento.
- Os consumidores aguardam o healthcheck do AIStor quando aplicável.

Essas integrações passaram por consistência estática. A validação funcional de
cada consumidor continua separada para não sobrecarregar a infraestrutura.

### Endurecimento da imagem Spark em 07/10/2026

- a base com R foi substituída por
  `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` fixada pelo digest OCI
  `sha256:f3d6eaa8bab8ec2e38f3c3918a5b2f8b253c95bcb19f9e87ad0e0cdacf9df2d5`;
- o conteúdo comprimido caiu de aproximadamente 879,6 MB para 659,0 MB, redução
  de cerca de 220,6 MB (25%);
- o contexto enviado à build caiu de 81,86 KB para 935 bytes;
- Java 17, diretório de trabalho e entrada do Spark foram preservados;
- a imagem final continua executando como usuário não privilegiado `spark`;
- nenhuma aplicação ou contêiner foi iniciado durante a reconstrução.

### Endurecimento estático do registro Debezium em 07/10/2026

- a senha literal foi removida do JSON versionado sem ser exibida;
- o template contém um marcador inválido até o momento da injeção, impedindo o
  registro acidental do conector sem uma credencial fornecida em runtime;
- o script exige `DEBEZIUM_POSTGRES_PASSWORD`, credencial dedicada ao CDC, e
  não reutiliza a senha administrativa `POSTGRES_PASSWORD`;
- usuário, senha e banco são lidos do `.env` ignorado pelo Git, validados contra
  ausência, duplicidade e valor vazio e inseridos somente no objeto em memória;
- uma regressão verifica o marcador, a ordem da injeção, a separação da senha
  administrativa e a ausência de impressão direta da credencial;
- nenhum contêiner foi iniciado para esta correção estática.

Essa correção elimina o segredo do estado atual da árvore, mas não o apaga do
histórico do Git. A credencial anterior deve ser tratada como exposta até ser
rotacionada no PostgreSQL e no `.env`. Reescrever o histórico não substitui a
rotação e não faz parte deste bloco.

### Estado da rotação da credencial CDC em 07/10/2026

`scripts/rotate-debezium-password.ps1` foi criado para executar uma rotação
isolada: exige todos os demais serviços parados, inicia somente o PostgreSQL,
gera 256 bits aleatórios e preserva exclusivamente em memória o verificador
SCRAM anterior. Credenciais são codificadas em memória e enviadas dentro de um
script pela entrada padrão; não aparecem em argumentos ou na saída.

A autenticação é testada pelo hostname de rede `postgres`, que exercita
`scram-sha-256`, e não pelo loopback coberto por uma regra local `trust`. A troca
do `.env` usa `File.Replace` com backup explícito, ACL preservada e limpeza dos
temporários. O verificador anterior é restaurado e comparado byte a byte em
qualquer falha ocorrida antes da confirmação final.

A rotação foi concluída em 07/10/2026:

- o verificador SCRAM original foi preservado antes do `ALTER ROLE`;
- a nova credencial autenticou e a credencial anterior foi rejeitada;
- o `.env` foi substituído atomicamente e sua credencial foi revalidada;
- após parar e iniciar novamente o PostgreSQL, a nova credencial continuou
  autenticando e uma senha aleatória foi rejeitada;
- nenhum valor de senha ou verificador foi exibido;
- somente o PostgreSQL foi iniciado e ele foi parado ao final.

Os falsos negativos observados nas primeiras tentativas vinham da serialização
multilinha entre Windows PowerShell e o shell Linux e, depois, do teste via
loopback `trust`. Eles não demonstravam dessincronização do volume. As tentativas
que alcançaram `ALTER ROLE` exercitaram o rollback SCRAM com restauração exata
antes da execução final bem-sucedida.

### Endurecimento da configuração PostgreSQL CDC em 07/10/2026

`scripts/configure-postgres-cdc.ps1` deixou de entregar `cdc_password` ao
`psql` por argumento de processo. O SQL agora carrega usuário, banco e senha
com `\getenv`; os valores são codificados somente em memória e enviados ao
shell do contêiner pela entrada padrão. O comando visível do `docker exec`
contém apenas `sh`, e o valor sensível não é escrito na saída.

O fluxo também foi tornado fail-closed para a infraestrutura local:

- exige que nenhum serviço do projeto esteja ativo antes de começar;
- usa `docker compose up -d --no-deps postgres` e confirma que somente
  `postgres` está rodando;
- falha explicitamente se o healthcheck não ficar saudável no prazo;
- valida `wal_level=logical`, LOGIN, REPLICATION, CONNECT, USAGE, SELECT nas
  tabelas e sequências atuais, privilégio padrão de SELECT e a publicação para
  todas as tabelas;
- autentica pelo hostname `postgres`, exercitando SCRAM, e confirma a rejeição
  de uma senha aleatória incorreta;
- reaplica a configuração no mesmo bloco para comprovar repetibilidade;
- sempre para o PostgreSQL no `finally` e confirma que não restou serviço
  ativo.

Na primeira tentativa, a consulta de validação permitia ao otimizador avaliar
`has_sequence_privilege` sobre um índice antes do filtro de tipo da relação. A
consulta foi protegida com `CASE`, garantindo que as funções de privilégio
recebam somente objetos compatíveis. A execução corrigida passou integralmente,
sem alterar a credencial rotacionada e sem iniciar Kafka ou Debezium.

O segredo ainda existe temporariamente no ambiente do processo `psql` dentro
do contêiner, necessário para `\getenv`, e um administrador do host/Docker pode
inspecionar processos. A melhoria elimina a exposição em argumentos e saída,
mas não transforma o ambiente local em um cofre de segredos.

### Validação funcional CDC em 08/10/2026

`scripts/validate-cdc.ps1` executou o caminho PostgreSQL → Debezium → Kafka e
Generator de forma isolada. PostgreSQL, Kafka e Debezium foram iniciados nessa
ordem, sempre aguardando o healthcheck anterior; MinIO, Spark, Hive, Trino,
Superset e Airflow permaneceram parados.

O teste comprovou:

- conector e task Debezium em `RUNNING`, com a senha mascarada nos logs;
- eventos de criação, atualização e exclusão, na ordem `c,u,d`, correlacionados
  pela chave primária e contendo LSN de origem;
- leitura dos dois formatos aceitos pela Bronze: envelope direto e o formato
  versionado com `schema`/`payload` encontrado no tópico persistente;
- contrato de `REPLICA IDENTITY DEFAULT`: o estado novo do `UPDATE` é completo,
  mas a pré-imagem não é obrigatória; o `DELETE` preserva a chave necessária;
- parada somente do Debezium, escrita no PostgreSQL durante a indisponibilidade
  e retomada posterior sem repetir snapshot;
- slot lógico inativo durante a parada e ativo após a retomada;
- avanço do tópico interno `demandflow-connect-offsets` e do LSN confirmado;
- uma venda determinística pelo Generator refletida em `orders`, `order_items`,
  `inventory` e `inventory_movements`.

O Generator ganhou `--action` para selecionar uma operação e `--seed` para
reproduzir sua aleatoriedade sem alterar o comportamento ponderado padrão. Uma
venda sintética válida permaneceu no banco para alimentar o próximo teste Raw;
os registros temporários usados para `c/u/d` e reinício foram excluídos e suas
exclusões chegaram ao Kafka.

Na máquina local, a recuperação dos tópicos internos do Kafka Connect chegou a
124 segundos sem OOM. A janela foi ajustada para 180 segundos, sem aumentar CPU
ou memória. O acesso do host foi canonicalizado para `127.0.0.1`, coerente com
o bind IPv4 do Compose e sem a resolução intermitente de `localhost` para
`::1` no Windows.

`run-raw-ingestion.ps1` e `run-lakehouse-rebuild.ps1` também foram reordenados:
o armazenamento é parado sem remover o volume, a configuração PostgreSQL roda
isolada e somente então MinIO e CDC são iniciados. Isso elimina chamadas que
violavam o isolamento introduzido no bloco anterior.

Riscos residuais conhecidos:

- o log do Debezium recomenda `heartbeat.action.query`; sem uma escrita de
  heartbeat no banco, workloads de baixa atividade podem reter WAL por mais
  tempo. A correção exige tabela/consulta dedicada e deve ser medida para não
  gerar escrita desnecessária nesta máquina;
- Kafka Connect precisa persistir a configuração do conector no volume Kafka.
  Um administrador do host/Docker continua capaz de acessar material sensível;
  um `ConfigProvider` baseado em arquivo externo reduziria esse risco;
- o tráfego Kafka/Connect continua sem TLS dentro da rede Docker local. As
  portas publicadas permanecem vinculadas ao loopback, não à rede externa.

Ao final, Debezium, Kafka e PostgreSQL foram parados individualmente e nenhum
serviço do projeto permaneceu ativo.

O Docker Scout está instalado, mas recusou a análise sem login no Docker ID.
Trivy, Grype e OSV Scanner não estão instalados. Portanto, a ausência de um
inventário de CVEs continua sendo uma limitação conhecida; não deve ser
interpretada como ausência de vulnerabilidades.

### Smoke Spark + Delta em 07/10/2026

O smoke foi executado com somente AIStor e um contêiner Spark temporário. A
tabela `s3a://demandflow-bronze/smoke/products_delta` comprovou:

- escrita Delta inicial na versão 0;
- `MERGE`/UPSERT na versão 1;
- leitura e validação dos quatro registros resultantes;
- histórico das operações `WRITE` e `MERGE`;
- Change Data Feed com pré-imagem, pós-imagem e inserção;
- Time Travel para a versão anterior ao `MERGE`.

O processo terminou com código 0. O contêiner Spark criado com `--rm` foi
removido automaticamente e o AIStor foi parado. A tabela de smoke permanece no
prefixo isolado `smoke/`, permitindo inspeção posterior e repetição idempotente
por sobrescrita.

No pico observado, Spark utilizou aproximadamente 1,92 GiB dos 3 GiB permitidos
e saturou o limite de 2 CPUs; AIStor utilizou aproximadamente 447 MiB de 1 GiB e
menos de 1% de CPU. Não ocorreu OOM nem sinal de sobrecarga fora dos limites.

A primeira execução baixou seis artefatos Maven, cerca de 283 MB, para
`/tmp/.ivy2` dentro do contêiner temporário. Esse cache é descartado por `--rm`,
então execuções futuras repetem rede e escrita em disco. Persistir um cache Ivy
dedicado pode reduzir tempo e tráfego, mas precisa de um bloco próprio para
definir permissões, escopo e limpeza sem compartilhar credenciais.

## Operação isolada

Preparar credenciais uma única vez:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/initialize-storage-secrets.ps1
```

Iniciar apenas armazenamento e aguardar o healthcheck:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/start-storage.ps1
```

Executar bootstrap quando necessário:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/bootstrap-s3.ps1
```

Ou iniciar e executar o bootstrap no mesmo fluxo:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/start-storage.ps1 -Bootstrap
```

Parar somente o armazenamento, preservando contêiner e volume:

```powershell
docker compose stop -t 30 minio
```

Não usar `docker compose down -v`: isso excluiria volumes persistentes.

## Validação estática

```powershell
docker compose --profile cdc --profile processing --profile query --profile orchestration config --quiet
.venv\Scripts\python.exe -B -m unittest discover -s tests -p test_storage_migration.py -v
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/validate-static-syntax.ps1
git diff --check
```

Resultado atual:

- treze testes de consistência passaram, incluindo isolamento da imagem Spark,
  preservação dos componentes Generator/CDC, injeção segura e configuração
  PostgreSQL sem senha em argumentos e o contrato do validador CDC;
- 21 scripts PowerShell passaram pelo parser sem execução operacional;
- scripts shell passaram em `sh -n`;
- Compose completo e `git diff --check` passaram;
- arquivos de licença e credenciais estão ignorados pelo Git e pelo contexto de
  build.

O Git pode emitir um aviso preexistente ao acessar
`airflow/logs/dag_processor/latest`; esse link de logs não participa da migração.

## Legado e rollback

O serviço LocalStack não existe mais no Compose. O contêiner órfão
`demandflow-localstack` foi removido em 06/10/2026 com autorização explícita,
sem `-v`. O volume `demandflow_localstack_data` continua declarado e o volume
Docker físico legado foi preservado para rollback.

O volume do AIStor é separado. Formatos internos de LocalStack e AIStor não são
intercambiáveis, e esta etapa não migrou objetos entre eles.

Um rollback exige restaurar em conjunto Compose, scripts, jobs e catálogos a
partir do histórico do Git, recriar o contêiner antigo e validar os dados antes
de qualquer escrita. Não montar o volume AIStor no LocalStack nem o volume
LocalStack no AIStor. Variáveis antigas presentes no `.env` estão preservadas,
mas não são consumidas pela configuração atual.

## Próximos blocos

Cada bloco exige nova autorização e termina com os serviços parados:

1. avaliar o cache Ivy e validar Raw → Bronze → Silver → Gold, dividindo o
   processamento em dois blocos se a pressão local exigir;
2. validar Hive Metastore, Trino e Superset sobre os dados Gold;
3. validar o DAG Airflow ponta a ponta, repetibilidade e retomada;
4. executar a auditoria final de segurança/desempenho, incluindo heartbeat e
   `ConfigProvider` do CDC, inventário de CVEs na
   medida em que uma ferramenta estiver disponível, documentação e checklist
   de entrega.

Esses são quatro blocos principais restantes. Pelas limitações da máquina, o
primeiro pode virar dois ou três blocos operacionais; portanto, a projeção é de
quatro blocos macro ou cinco a seis execuções controladas após este ponto.

## Referências oficiais

- [AIStor em contêiner e licença Free](https://docs.min.io/aistor/installation/container/install/)
- [Configuração do servidor AIStor](https://docs.min.io/aistor/reference/aistor-server/settings/)
- [Identidades locais e políticas](https://docs.min.io/aistor/administration/iam/identity/built-in-identity/)
- [Expansão de ambiente no Hadoop](https://hadoop.apache.org/docs/r3.3.6/api/org/apache/hadoop/conf/Configuration.html)
- [Validação do Docker Compose](https://docs.docker.com/reference/cli/docker/compose/config/)
- [PostgreSQL 16: restauração de senhas criptografadas de papéis](https://www.postgresql.org/docs/16/sql-createrole.html)
- [PostgreSQL 16: `psql`, `\getenv` e interpolação segura](https://www.postgresql.org/docs/16/app-psql.html)
- [PostgreSQL 16: funções de consulta de privilégios](https://www.postgresql.org/docs/16/functions-info.html)
