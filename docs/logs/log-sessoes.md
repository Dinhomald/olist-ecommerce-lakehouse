# Projeto olist-ecommerce-lakehouse — Log de Sessão 01

**Data:** 17-18/08/2026
**Objetivo do projeto:** Pipeline end-to-end (API → ADF → ADLS Gen2 → Databricks) usando dataset Olist, construído como portfólio para conversa técnica com Matheus (contato DB/Randoncorp). Matheus não sabe da existência do projeto.

---

## 1. Decisões de arquitetura

- **Stack escolhida:** FastAPI (fonte simulada) → Azure Data Factory (orquestração) → ADLS Gen2 (Bronze/Silver/Gold) → Databricks (processamento) → Power BI/SQL Warehouse (consumo, ainda não construído).
- **ADF em vez de só Airflow:** decisão deliberada de aprendizado — Ronaldo já domina Airflow em produção na Expert; ADF é ganho de portfólio para vagas Microsoft-stack.
- **Sem camada "Stage" separada da Bronze:** decisão consciente. Stage só se justificaria com múltiplas fontes heterogêneas convergindo (cenário da própria DB, não o nosso). ADF escreve direto na Bronze.
- **Dataset:** Olist Brazilian E-commerce (Kaggle), 9 tabelas relacionadas — orders, order_items, customers, products, sellers, payments, reviews, geolocation, category_translation.
- **SCD2 simulado:** decisão pendente de qual dimensão vai carregar a simulação — bateu-se o martelo em **Dim Produto** (mudança de categoria), mais simples de defender que Dim Cliente (evita confusão com Dim Geografia).
- **Nomenclatura de repos:** `olist-fake-api` (API simulada, repo próprio) e `olist-ecommerce-lakehouse` (ADF/Databricks/docs, repo próprio) — separados propositalmente, pois a API representa uma "fonte externa" que não deveria estar dentro do projeto de engenharia.

---

## 2. API Fake (`olist-fake-api`)

**Repo:** `github.com/Dinhomald/olist-fake-api`
**Deploy:** Render (free tier), URL: `https://olist-fake-api.onrender.com`
**Stack:** FastAPI + pandas + uvicorn

### Endpoints (9, todos paginados)
`/orders`, `/order_items`, `/customers`, `/products`, `/sellers`, `/payments`, `/reviews`, `/geolocation`, `/category_translation`, mais `/` (health check).

### Estrutura de resposta paginada
```json
{
  "page": 1,
  "page_size": 100,
  "total_records": 99441,
  "total_pages": 995,
  "has_next": true,
  "next_page_url": "https://olist-fake-api.onrender.com/orders?page=2",
  "data": [...]
}
```

### Bugs resolvidos nesta sessão
1. **Segfault do Uvicorn ao usar `parse_dates` do pandas** — causa raiz: incompatibilidade binária entre `pandas==2.2.2` (fixado) e `numpy 2.5.2` (dependência transitiva não fixada, puxada pelo pip). Além disso, Python 3.14 (muito recente) não tinha wheels pré-compiladas para a combinação antiga. Solução: `pandas==2.3.3` + `numpy>=2.1` (ambos com wheel para cp314).
2. **Valores nulos serializados como string `"NaT"` em vez de `null`** — causa: pandas preserva dtype `datetime64` mesmo ao tentar substituir por `None`. Solução: `.astype(object).where(pd.notnull(subset), None)` antes de `.to_dict()`.
3. **Processo Uvicorn "fantasma"** mascarando correções de código — processo antigo ficava preso na porta 8000, servindo versão desatualizada mesmo após editar o código. Prática adotada: sempre `netstat -ano | findstr :8000` antes de subir o servidor.
4. **Deploy no Render não atualizava** — causa: `git push` não tinha sido efetivado corretamente numa sessão anterior (trabalhando de outro computador). Resolvido com Manual Deploy → Deploy latest commit após confirmar que o commit estava no GitHub.
5. **`next_page_url` adicionado** — campo calculado no backend (não no cliente) porque a Pagination Rule do ADF (`AbsoluteUrl`/Body) só suporta extrair valor JSONPath pronto, não expressões condicionais computadas.

### Estado atual
API 100% funcional em produção, todos os 9 endpoints testados, paginação dinâmica funcionando corretamente (testado manualmente via browser/curl, página 1 e 2 confirmadas).

---

## 3. Infraestrutura Azure

**Subscription:** Pay-As-You-Go (não Free Trial — free trial trava Databricks por limite de 4 vCPUs; Pay-As-You-Go não tem esse limite). Crédito promocional de $200/30 dias confirmado ativo (aparece só na home simplificada do portal, não nas telas de Cost Management/Billing tradicionais — modelo de conta MCA).

**Budget Alert configurado** na subscription como proteção contra gasto excessivo.

**Grupo de recursos:** `rg-olist-lakehouse` (East US — todos os recursos na mesma região, propositalmente, para evitar latência/custo de transferência entre regiões).

### Recursos criados
- **Databricks workspace:** `estudos` (tier Trial Premium — 14 dias DBUs grátis, Unity Catalog incluso). Cluster mínimo testado e confirmado subindo sem erro de quota.
- **Storage Account (ADLS Gen2):** `adlsolistdatalake`. Standard, LRS, Hierarchical Namespace habilitado desde a criação (confirmado — não é Blob Storage comum). Continha uma tentativa fracassada de criar `adlsolistlakehouse` primeiro (erro `ResourceNotFound` por conflito de nome recém-excluído — resolvido criando com nome diferente).
- **Containers:** `bronze`, `silver`, `gold` — todos privados (sem acesso anônimo).
- **Azure Data Factory:** `adf-olist-lakehouse`.
  - Linked Service `ls_rest_olist_fake_api` (REST, Anonymous, aponta para a API no Render) — testado OK.
  - Linked Service `ls_adls_olist_lakehouse` (ADLS Gen2, autenticação via Managed Identity do sistema, role "Contribuinte de Dados do Armazenamento de Blobs" atribuída ao ADF sobre a storage account) — testado OK.
  - Dataset `ds_rest_orders` (REST, endpoint `/orders`).
  - Dataset `ds_adls_bronze_orders` (ADLS Gen2, JSON, `bronze/orders/`, sem schema importado — decisão correta para Bronze).
  - Pipeline `pl_ingest_orders_bronze`.

---

## 4. Bloqueio ativo — Paginação do ADF não avança

**Sintoma:** o Copy Activity sempre lê e grava **apenas 1 objeto** (a resposta inteira da API tratada como 1 registro, não como coleção), e a paginação **nunca avança além da página 1**, independentemente da configuração testada. Resultado idêntico em toda tentativa: 65.335 KB lidos, 1 objeto, 37.71 KB escritos, ~10-13s de duração.

### O que já foi testado, sem sucesso
- Pagination Rule `AbsoluteUrl` / `Body` / `$.next_page_url` — sintaxe confirmada correta pela documentação oficial e por tutoriais.
- Variação de sintaxe: `$.next_page_url` vs `$['next_page_url']` (colchetes).
- API confirmada 100% funcional de forma isolada — `page=2` testado direto no browser, retorna dados diferentes e `next_page_url` correto.
- Testado em modo Debug **e** via Trigger publicado (execução real) — mesmo resultado.
- Tentativa de configurar "Referência da recolha" (`Collection Reference`) como `$['data']` no mapeamento — gerou erro `"Um acessor de matriz como [0] não é suportado no sink de mapeamento de esquemas"` ao tentar mapear colunas individuais (`$['data'][0]['order_id']`).
- Mapeamento limpo (Collection Reference `$['data']` sem colunas mapeadas) — validou sem erro, mas resultado idêntico (1 objeto).
- Certificado SSL desativado no Linked Service (hipótese de falha silenciosa por validação de certificado) — sem efeito.
- Dataset REST recriado do zero.
- Copy Activity recriado do zero.
- Logging habilitado na atividade (`Ativar registo`) — arquivo gerado só com cabeçalho CSV, sem linhas de dado (não forneceu diagnóstico adicional).

### Conclusão
A causa raiz não foi identificada. É provável que seja uma interação específica entre o REST connector do ADF e a API custom hospedada no Render, não documentada e não reproduzida nos tutoriais/fóruns pesquisados.

### Próximo passo combinado (não finalizado nesta sessão)
Abandonar `AbsoluteUrl`/Body, pivotar para mecanismo `QueryParameters` + `RANGE` (documentado oficialmente como alternativa robusta, não depende de interpretar o corpo da resposta):

1. **Web Activity** (`Web1`) — já criada e conectada ao Copy Activity no canvas. Chama `GET /orders?page=1` para obter `total_pages` da resposta.
2. **Variável de pipeline** `totalPages` (tipo Cadeia/String) — já criada, mas o valor ainda está com um placeholder incompleto (`@activity('NomeDaWebActi...`), precisa ser corrigido para `@activity('Web1').output.total_pages`.
3. **Set Variable** — atividade ainda **não criada**. Precisa ser inserida entre a Web Activity e o Copy Activity, atribuindo o valor de `total_pages` à variável `totalPages`.
4. **Pagination Rule nova no Copy Activity** — trocar de `AbsoluteUrl` para `QueryParameters.page`, valor:
   ```
   @concat('RANGE:1:', string(variables('totalPages')), ':1')
   ```

Isso preserva parcialmente o dinamismo (não hardcoda o número de páginas), sem depender do mecanismo que está falhando.

---

## 5. Decisões de nomenclatura e padrões adotados

- Sequência de nomes: `ls_` (linked service), `ds_` (dataset), `pl_` (pipeline).
- Partição de ingestão planejada para a Bronze: `{entidade}/ano=YYYY/mes=MM/dia=DD/` (ainda não implementada — o Copy Activity atual grava sem particionamento por data).
- Padrão de conteúdo gerado por Ronaldo (LinkedIn, mensagens): frases curtas, sem enrolação, contrações informais, zero em-dash, ancorado em caso técnico real.

---

## 6. Notas para continuidade em outro chat (fim da Sessão 01)

- Não repetir os testes já listados na seção 4 — todos já foram tentados sem sucesso.
- Verificar se o cluster Databricks ficou ligado sem necessidade entre sessões (checar antes de continuar, risco de custo).
- Verificar se o Copy Activity atual ainda está configurado com `AbsoluteUrl` (precisa ser trocado) antes de continuar a implementação do `RANGE`.
- Depois de resolver a paginação de `/orders`, replicar o padrão (dataset + pagination) para os outros 8 endpoints — candidato natural para um **ForEach Activity parametrizado**, ainda não implementado.
- Etapa de SCD2 simulado (Dim Produto, mudança de categoria) ainda não iniciada.
- Unity Catalog ainda não configurado dentro do Databricks (só confirmado que está disponível no tier Premium).

---

# Sessão 02 (18/08/2026) — Resolução da paginação e ingestão completa dos 9 endpoints

## 7. Causa raiz real do bloqueio da Sessão 01

O bloqueio descrito na Seção 4 **não foi causado pela API nem pela arquitetura `AbsoluteUrl`** — foi causado por dois erros de configuração de UI no ADF, descobertos só depois de inspecionar o JSON de **Entrada** real da activity (aba "Entrada" no painel de execução, não a visualização da tela de configuração):

1. **Dropdown "Nome" da Pagination Rule em "Nenhum"**: a UI de Regras de Paginação tem dois campos separados — um dropdown (tipo da regra: `AbsoluteUrl`, `Headers`, `QueryParameters`) e um campo de texto livre ao lado. O texto `QueryParameters.page` (ou depois `AbsoluteUrl`) tinha sido digitado no campo de texto, mas o dropdown continuava em **"Nenhum"** — a regra nunca era vinculada de fato, e o Copy Activity sempre batia na URL base do dataset sem nenhuma paginação real aplicada.
2. **Path JSONPath duplicado**: ao corrigir o dropdown para `AbsoluteUrl`, uma edição de campo resultou em `$.$.next_page_url` (prefixo `$.` duplicado) em vez de `$.next_page_url`. JSONPath inválido = paginação nunca resolve o campo = loop para na primeira chamada.

**Lição travada**: a única forma confiável de diagnosticar isso foi abrir a aba **Entrada** (Input) da activity após a execução, que mostra o JSON de configuração *realmente enviado* ao motor de execução — não o que a caixa de texto exibe visualmente. Essa aba deveria ser o primeiro lugar a checar sempre que "a configuração parece certa na tela mas o resultado não bate".

## 8. Configuração final que funciona (referência para replicar)

**Pagination Rule (Copy Activity → aba Origem → Regras de paginação):**
| Campo | Valor |
|---|---|
| Nome (dropdown) | `AbsoluteUrl` |
| Campo de texto ao lado do dropdown | vazio |
| Seletor do Valor | `Body` |
| Valor | `$.next_page_url` (dot notation, sem colchetes/aspas, sem `data` ou índice no meio) |

**Sink:** dataset JSON puro, **sem** Collection Reference nem mapeamento tabular de colunas. Cada linha do arquivo de saída é um envelope de página completo (`page`, `page_size`, `total_records`, `total_pages`, `has_next`, `next_page_url`, `data: [...]`). Decisão consciente: o achatamento do array `data[]` (1 registro por linha) fica para o Spark na camada Silver, via `explode()` — tentar fazer esse flatten dentro do Copy Activity do ADF (testado exaustivamente com Collection Reference `$['data']` + mapeamento de colunas) nunca funcionou de forma confiável nesta combinação REST+Parquet/tabular, mesmo com preview mostrando o array correto.

**Fim de paginação:** automático — o conector para quando `next_page_url` (ou o campo mapeado em `AbsoluteUrl`) vem `null`, sem precisar saber `total_pages` de antemão. Isso tornou desnecessário todo o mecanismo de `Web Activity` + `Set Variable` + `RANGE` planejado no fim da Sessão 01 — **essa abordagem foi abandonada** em favor da paginação nativa via `next_page_url`.

## 9. Método de verificação (crítico — não confiar em "Efetuado com êxito")

Descoberto na prática: o resumo de execução do ADF ("Efetuado com êxito", contagem de "Objetos lidos") **não garante que a paginação percorreu todas as páginas**. Duas vezes nesta sessão, a execução reportou sucesso com contagem de objetos aparentemente correta (995), mas a inspeção do arquivo revelou que todas as "995 páginas" eram a página 1 repetida 995 vezes.

**Processo de verificação usado, deve ser reaplicado sempre:**
1. Calcular o número de páginas esperado: `total_pages_esperado = ceil(total_records / page_size)`.
2. Comparar contra "Objetos lidos" no resumo do ADF Studio — primeiro filtro, rápido, sem precisar baixar arquivo.
3. Para entidades críticas (ou qualquer uma com resultado suspeito), baixar o arquivo gerado no ADLS e rodar um script simples que confirma: (a) contagem de valores **distintos** de `page` bate com `total_pages`; (b) não há lacuna nem duplicata na sequência 1..N; (c) contagem de registros únicos (chave primária/composta) bate com `total_records`.

## 10. Bug adicional encontrado durante a replicação: `order_items`

Ao clonar o Copy Activity de `orders` para criar os outros 8 pipelines, `pl_ingest_order_items_bronze` foi publicado **sem a Pagination Rule configurada** (esquecida na clonagem). Resultado: reportou sucesso, "Objetos lidos: 1", 19s de duração — sintoma que só foi pego porque a duração (19s) era baixa demais proporcionalmente ao volume esperado de `order_items` (~1.127 páginas, mais que `orders`). Corrigido adicionando a Pagination Rule idêntica à de `orders`. Validado por arquivo completo depois da correção: 1.127 páginas únicas, 112.650 registros únicos por `(order_id, order_item_id)` — bate exato com `total_records` da API.

## 11. Estado final da ingestão Bronze — todos os 9 endpoints

Todos os 9 endpoints do Olist ingeridos com sucesso e verificados contra a contagem de páginas esperada (calculada, não assumida):

| Entidade | Registros totais | Páginas | Status |
|---|---|---|---|
| `category_translation` | 71 | 1 | ✅ |
| `sellers` | 3.095 | 31 | ✅ |
| `products` | 32.951 | 330 | ✅ |
| `reviews` | 99.224 | 993 | ✅ |
| `orders` | 99.441 | 995 | ✅ validado por arquivo (page-by-page, sem lacuna) |
| `customers` | 99.441 | 995 | ✅ |
| `payments` | 103.886 | 1.039 | ✅ |
| `order_items` | 112.650 | 1.127 | ✅ validado por arquivo (bug de clonagem corrigido, ver Seção 10) |
| `geolocation` | 1.000.163 | 10.002 | ✅ bateu exato com projeção matemática, isolado do restante do fluxo |

## 12. Arquitetura de orquestração — pipeline mestre

Criado `pl_ingest_bronze_master`, orquestrando os 9 pipelines filho (1 por entidade, cada um com dataset origem/destino próprio, sem parametrização por enquanto — decisão consciente de manter separado para facilitar debug enquanto o domínio de ADF ainda está em construção) via activities **Execute Pipeline**.

**Estrutura de dependência (decisão deliberada, não default do ADF):**
- 7 entidades pequenas/médias (`category_translation`, `customers`, `order_items`, `orders`, `payments`, `products`, `reviews`, `sellers`) encadeadas em sequência (seta verde de sucesso), retry configurado (2 tentativas, 30s de intervalo, para absorver cold start/rate limit do Render free tier).
- `geolocation` **isolado**, com ponto de início próprio no canvas (não encadeado com os outros) — por volume ~10x maior (10.002 páginas vs ~1.000 dos demais), rodando em paralelo com a cadeia dos outros 7, para que falha ou demora nele não trave a conclusão dos demais.
- Retry configurado no nível do Execute Pipeline (aba Geral: "Repetir novamente" = 2, "Intervalo de repetição" = 30s). Limitação identificada: retry reroda o pipeline filho inteiro do zero, sem checkpoint parcial — caro para `geolocation` especificamente se falhar tarde na execução.

**Ainda não implementado (adiado deliberadamente):** ForEach parametrizado com dataset genérico (`pEntityName`) para substituir os 9 pares de dataset fixos. Avaliado como próxima evolução, mas adiado até o time ter mais repertório de debug de erro dentro de iteração de loop (mais opaco que Copy Activity isolado).

## 13. Decisões e reflexões técnicas registradas

- **ADF vs Airflow**: usado como comparação consciente durante a sessão. Pontos de continuidade real (DAG visual = pipeline visual, task isolada = activity isolada, TriggerDagRunOperator = Execute Pipeline). Diferenças relevantes para entrevista: ADF é low-code/JSON-em-serviço (bug de configuração não aparece em `git diff`, como apareceria em DAG Python versionado); Airflow permite teste local sem custo, ADF Debug consome DIU-hora por execução; Airflow tem reprocessamento granular por task, ADF por padrão reroda o pipeline inteiro.
- **Migração ADF → Microsoft Fabric**: pesquisado durante a sessão. Constatação: é o Fabric que recebe os recursos novos hoje (mirroring, copy jobs, Copilot), não o ADF clássico — a lógica de "ficar no ADF preserva acesso a recursos novos" está invertida. Sem prazo de fim de vida anunciado para o ADF. Decisão tomada: não migrar agora, tratar como decisão separada do debug em andamento; ADF clássico é suficiente e mais simples para o escopo atual do portfólio.
- **Motivo da Microsoft empurrar Fabric**: modelo de capacidade reservada (SKU F2-F2048) favorece receita recorrente previsível sobre consumo variável; lock-in via OneLake; consolidação reduz custo de manutenção interna da Microsoft. Ganho técnico real existe (menos gestão de Integration Runtime), mas é também o que aprofunda o lock-in.

## 14. Notas para continuidade em outro chat (fim da Sessão 02)

- Etapa 1 (ingestão Bronze completa, 9 endpoints) está **concluída e verificada**, não apenas "rodando verde" — não repetir o debug de paginação.
- Próxima etapa: Databricks/Spark para construção da Silver — `explode()` do array `data[]` de cada entidade, schema real aplicado por entidade (a Bronze é intencionalmente crua/aninhada, esse é o primeiro tratamento estrutural real do pipeline).
- ForEach parametrizado para os 9 datasets continua pendente — considerar só depois de ter mais prática de debug em iteração de loop no ADF.
- SCD2 simulado (Dim Produto) e Unity Catalog ainda não iniciados — seguem pendentes desde a Sessão 01.
- Verificar se o cluster Databricks ficou ligado sem necessidade entre sessões (mesmo alerta da Sessão 01, ainda válido).

---

# Sessão 03 (18/08/2026) — Bug de CEP truncado, Unity Catalog do zero, Silver completa (9 entidades)

## 15. Bug de qualidade de dado encontrado antes da Silver: CEP truncado

Ao investigar o layout físico dos arquivos Bronze (confirmado: **JSON Lines**, 1 envelope de página por linha, sem `multiline` necessário no Spark), inspeção manual do arquivo de `geolocation` revelou `"geolocation_zip_code_prefix":1037` — número JSON sem zero à esquerda, em vez do `"01037"` esperado.

**Investigação, não assumida de imediato:**
- Hipótese inicial (rejeitada após checagem): "o dataset original do Kaggle já vem sem zero à esquerda". **Falsa** — checado o CSV original (`olist_geolocation_dataset.csv`), confirmado zero à esquerda presente (`01037`, `01046`, etc.), coluna como string entre aspas no CSV.
- **Causa raiz real:** `pd.read_csv()` no backend da API (`main.py`), sem `dtype` explícito, infere a coluna de CEP como `int64`, comendo o zero à esquerda na conversão — antes mesmo da serialização para JSON (diferente do bug de `NaT` da Sessão 01, que era na serialização, não na leitura).
- **Escopo do bug:** replicado em `geolocation_zip_code_prefix`, `customer_zip_code_prefix`, `sellers_zip_code_prefix` — confirmado por inspeção direta dos 3 CSVs (`grep`/`head`) antes de aplicar qualquer fix, evitando corrigir só o caso relatado e deixar os outros dois passar.

**Fix aplicado em `olist-fake-api/main.py`:**
```python
customers_df = pd.read_csv("data/olist_customers_dataset.csv", dtype={"customer_zip_code_prefix": str})
sellers_df = pd.read_csv("data/olist_sellers_dataset.csv", dtype={"seller_zip_code_prefix": str})
geolocation_df = pd.read_csv("data/olist_geolocation_dataset.csv", dtype={"geolocation_zip_code_prefix": str})
```

**Processo de correção e revalidação (não só "corrigiu e seguiu"):**
1. Fix aplicado, redeploy no Render.
2. Endpoint `/geolocation` testado isoladamente — zero à esquerda confirmado no JSON de resposta.
3. Re-execução dos 3 pipelines afetados na Bronze (`customers`, `sellers`, `geolocation`) — os outros 6 endpoints **não** foram re-ingeridos, por não terem campo de CEP.
4. Revalidação por arquivo, pós re-ingestão: contagem de linhas batendo com páginas esperadas nos 3 (995, 31, 10.002) **e** confirmação de casos reais com zero à esquerda presentes no JSON final (`"01151"`, `"04195"`, `"01037"`), não só verificação de tipo.

**Lição travada:** não assumir que uma característica "estranha" do dado vem da fonte original sem checar a fonte de verdade primeiro — a hipótese inicial (dataset original já vem truncado) foi proposta e descartada por checagem direta, evitando encerrar a investigação com conclusão errada.

## 16. Unity Catalog configurado do zero — cadeia de acesso Azure ↔ Databricks

Workspace `estudos` não tinha nenhuma credencial/location configurada. Cadeia completa montada nesta sessão:

**Sequência (Access Connector → Storage Credential → External Location → Catalog/Schema):**
1. **Access Connector for Azure Databricks** criado via portal Azure — recurso `ac-databricks-olist`, `rg-olist-lakehouse`, East US, com Identidade Gerida Atribuída pelo Sistema ativada (sem User Assigned).
2. **Role assignment no storage:** `ac-databricks-olist` recebeu **Storage Blob Data Contributor** em `adlsolistdatalake` — mesma role que o ADF já tinha, agora replicada para a identidade do Access Connector.
3. **Storage Credential** criada no Databricks (Azure Managed Identity), referenciando o **Resource ID completo** do Access Connector (não o Object ID da identidade — são dois IDs diferentes, achado durante a sessão: o campo "ID do conector de acesso" na tela de criação da External Location espera o Resource ID do recurso Access Connector, não o Object ID da managed identity, que vai no campo separado "opcional" de User Assigned, que ficou vazio).
4. **Três External Locations criadas** (`ext-loc-bronze`, `ext-loc-silver`, `ext-loc-gold`), uma por container, mesma credencial nas três:
   - `abfss://bronze@adlsolistdatalake.dfs.core.windows.net/`
   - `abfss://silver@adlsolistdatalake.dfs.core.windows.net/`
   - `abfss://gold@adlsolistdatalake.dfs.core.windows.net/`
   - Aviso de "eventos de ficheiro" (file events, Event Grid + Storage Queue) falhou nas três — role atribuída (Storage Blob Data Contributor) não cobre `EventGrid EventSubscription Contributor` / `Storage Queue Data Contributor`. **Ignorado deliberadamente** ("Forçar criação do local") — file events é otimização de Auto Loader/ingestão incremental via streaming, não usado neste projeto (ingestão é batch). Pendente futuro caso Auto Loader venha a ser implementado.

**Bug de configuração do `CREATE CATALOG`:**
- Tentativa inicial de `CREATE CATALOG olist_lakehouse` **sem** `MANAGED LOCATION` falhou: `INVALID_STATE: Metastore storage root URL does not exist`. Causa: metastore do workspace Trial não tem Default Storage funcional provisionado.
- **Fix:** `MANAGED LOCATION` explícita no `CREATE CATALOG`, reaproveitando o path de `bronze` como raiz do catálogo (decisão pragmática para contornar o erro, não escolha de design limpa — catálogo inteiro tecnicamente depende do container `bronze` existir, mesmo hospedando também `silver`/`gold`). Documentado como trade-off aceito, não ideal.
- Cada schema (`silver`, `gold`) declara sua própria `MANAGED LOCATION` explícita, sobrescrevendo a herdada do catálogo.
- **Schema `bronze` mantido, porém vazio e sem uso funcional** — decisão consciente, não descuido: a Bronze é lida via `spark.read.json()` direto por path absoluto (arquivo cru escrito pelo ADF, fora do controle do UC), nunca como tabela gerenciada. `olist_lakehouse.bronze` existe no catálogo só como referência lógica da camada, sem tabela dentro. Se perguntado em entrevista: resposta pronta é "documenta a existência lógica da camada, mesmo sem gestão de tabela pelo UC, já que a Bronze é propositalmente externa ao Unity Catalog".

**Erro de leitura descoberto e explicado (não é bug, é comportamento esperado do UC):**
- Tentativa de `spark.read.json()` na **raiz exata** do container `bronze` (`abfss://bronze@.../`) gerou `INVALID_PARAMETER_VALUE.LOCATION_OVERLAP` — colide com o path exato reservado pela `MANAGED LOCATION` do schema.
- Leitura por **sub-path de entidade** (`abfss://bronze@.../orders/`) funciona sem conflito — Unity Catalog não bloqueia acesso a sub-caminhos específicos dentro de um container gerenciado, só ao path raiz que coincide exatamente com a location declarada.

## 17. Camada Silver construída — piloto (`orders`) + replicação (8 entidades)

**Padrão de notebook fixado no piloto (`silver_orders`), replicado igual nos demais 8:**
1. Leitura direta do Bronze via `spark.read.json()` por sub-path de entidade (JSON Lines, sem `multiline`).
2. Schema explícito por `StructType`/`StructField` — nunca inferência automática em produção.
3. Explode defensivo: `explode(col("data"))` → `from_json(to_json(...), schema_explicito)` → `.select("alias.*")`. Reaplicação de tipo forçada logo após o explode, não herdando schema inferido da leitura original — proteção contra schema drift silencioso entre execuções.
4. Conversão de campos de data string → `to_timestamp()`, campo a campo (decisão consciente: assume timezone de sessão, UTC padrão do Databricks, já que o JSON original não carrega timezone explícito — aceitável para escopo de portfólio, documentado como suposição, não esquecido).
5. Validação por `assert` **antes** de escrever: contagem total batendo com `total_records` da API, e checagem de unicidade de chave (primária ou composta) — nunca confiar em "sem erro" como prova de integridade, mesmo princípio da Bronze (Seção 9).
6. Escrita como tabela Delta gerenciada (`olist_lakehouse.silver.<entidade>`), `mode("overwrite")` + `overwriteSchema=true` — full load, não incremental (decisão consciente, ver Seção 18).
7. Revalidação via SQL contra a tabela já persistida, não só contra o DataFrame em memória — confirma que a escrita em si não introduziu truncamento silencioso.

**Nomenclatura fixada:** notebooks `silver_<entidade>` (prefixo de camada, minúsculo, snake_case — mesma lógica dos prefixos `ls_`/`ds_`/`pl_` do ADF), organizados em pasta `notebooks/silver/` no repo. Tabela Delta sem repetir o prefixo de camada (`olist_lakehouse.silver.orders`, não `...silver.silver_orders`) — schema já carrega o "silver" no caminho.

### Erros reais cometidos e corrigidos durante a replicação (registrar para não repetir)

1. **Confusão de tipo `product_id` em `order_items`**: inicialmente declarado como `LongType()` — errado, é string hash (`"4244733e06e7ecb4970a6e2683c13e61"`). Mesma categoria de erro quase cometida com `order_id`: campo-ID que parece número não é número. Verificado programaticamente antes do fix: `product_id` **nunca nulo** em nenhum dos 112.650 registros (script Python percorrendo todas as 1.127 páginas), confirmando `nullable=False` correto depois da correção de tipo.
2. **Import de tipo faltando** (`LongType`, `DoubleType` usados sem import) — erro de execução simples, resolvido adicionando ao import.
3. **Estado de notebook dessincronizado**: célula reexecutada fora de ordem gerou `UNRESOLVED_COLUMN` (`data` não encontrada) porque uma variável (`df_exploded`) já não tinha mais a coluna `data` de uma execução anterior, mas a célula seguinte ainda tentava fazer `explode(col("data"))` em cima dela. Lição: Databricks mantém estado entre células como notebook Jupyter — editar célula de trás sem re-rodar as de frente na ordem correta dessincroniza o estado em memória do que está escrito na tela.

### Achados de qualidade de dado identificados por validação numérica (não por erro de execução)

**`reviews` — `review_id` NÃO é chave primária isolada.** Contagem total (99.224) batia, mas `COUNT(DISTINCT review_id)` deu 98.410 — 814 valores repetidos. Investigado antes de assumir bug de pipeline: casos duplicados têm mesmo `review_id`, mesma data, mesma nota, porém **`order_id` diferentes** — comportamento conhecido do dataset Olist original (review vinculado a múltiplos pedidos do mesmo checkout/carrinho, ou artefato de exportação/anonimização da fonte). Confirmado: chave real é composta, `(review_id, order_id)` — `COUNT(DISTINCT CONCAT(review_id,'-',order_id))` bate exato com 99.224. **Implicação para a Gold:** junção com `orders` assumindo `review_id` como 1:1 geraria fan-out incorreto; granularidade correta a ser usada na modelagem é a chave composta.

**Cardinalidades N:1 esperadas e confirmadas (não são bug):**
- `payments`: 103.886 linhas, 99.440 `order_id` distintos — normal (múltiplas formas de pagamento por pedido: parcelamento, voucher + cartão).
- `geolocation`: 1.000.163 linhas, 19.015 prefixos de CEP distintos — normal (múltiplas coordenadas lat/lng por prefixo de CEP no dataset original).

### Estado final da Silver — todas as 9 entidades validadas

| Tabela | Total | Chave validada | Observação |
|---|---|---|---|
| `category_translation` | 71 | `product_category_name` única | — |
| `customers` | 99.441 | `customer_id` única | — |
| `sellers` | 3.095 | `seller_id` única | — |
| `products` | 32.951 | `product_id` única | — |
| `order_items` | 112.650 | `(order_id, order_item_id)` única | `product_id` corrigido de `LongType` para `StringType`, 0 nulos confirmados |
| `orders` | 99.441 | `order_id` única | piloto, validado ponta a ponta primeiro |
| `payments` | 103.886 | N:1 com `order_id` (esperado) | — |
| `geolocation` | 1.000.163 | N:1 com CEP (esperado) | CEP com zero à esquerda confirmado pós-fix |
| `reviews` | 99.224 | `(review_id, order_id)` composta única | `review_id` isolado NÃO é chave, ver acima |

## 18. Decisão registrada: full load, não incremental (por enquanto)

Discutido e decidido conscientemente manter `mode("overwrite")` (full load) em vez de `MERGE INTO` incremental nesta etapa. Motivos documentados, não esquecimento:
1. Bronze não tem particionamento por data de ingestão implementado ainda (`{entidade}/ano=YYYY/mes=MM/dia=DD/`, pendente desde a Sessão 01) — pré-requisito estrutural para incremental eficiente.
2. Fonte (API fake sobre dataset Olist estático) não expõe conceito de "delta" — não tem `updated_at` nem endpoint de mudança incremental; toda execução da API retorna o mesmo dataset histórico fechado.
3. Implementar `MERGE` sem esses dois pré-requisitos adicionaria complexidade sem ganho comprovável, e sem forma real de testar que o incremental está funcionando (a fonte nunca muda de fato).

Ordem futura, se o projeto evoluir para simular execução recorrente real: (1) particionamento de data na Bronze via ADF, (2) só então `MERGE INTO` incremental na Silver.

## 19. Notas para continuidade em outro chat (fim da Sessão 03)

- Unity Catalog operacional: catálogo `olist_lakehouse`, schemas `bronze` (vazio, referência lógica), `silver` (9 tabelas, todas validadas), `gold` (vazio, próxima etapa).
- Bug de CEP truncado corrigido na fonte (API), não é workaround na Silver — `geolocation_zip_code_prefix`, `customer_zip_code_prefix`, `seller_zip_code_prefix` chegam como string com zero à esquerda preservado em toda a cadeia.
- Achado de `review_id` não sendo chave isolada precisa ser respeitado na modelagem da Gold — usar `(review_id, order_id)` como granularidade se `reviews` virar fato ou entrar em junção.
- Próxima etapa: modelagem dimensional da Gold (Star Schema Kimball — fato(s) + dimensões, granularidade de cada uma, surrogate keys) **no papel antes do código**.
- SCD2 simulado em Dim Produto (mudança de categoria) acontece na transição Silver → Gold — ainda não iniciado.
- ForEach parametrizado no ADF continua pendente/adiado — sem mudança de status nesta sessão.
- Particionamento de data na Bronze continua pendente — decisão consciente de não implementar agora (ver Seção 18).
- Verificar se o cluster Databricks ficou ligado sem necessidade entre sessões (mesmo alerta recorrente, ainda válido).

---

# Sessão 04 (19/08/2026) — Gold completa (Star Schema Kimball, SCD2 real) + início da camada de consumo

## 20. Modelagem decidida antes do código

3 fatos (não 1), respeitando grão distinto de cada processo de negócio — regra de Kimball de nunca misturar grãos diferentes no mesmo fato:

| Fato | Grão | Motivo de ser separado |
|---|---|---|
| `fato_vendas` | 1 item de pedido | — |
| `fato_pagamentos` | 1 pagamento de pedido | Pagamento é do pedido inteiro, não do item — misturar geraria fan-out arbitrário |
| `fato_reviews` | `(review_id, order_id)` | Review é do pedido, não do item; grão composto por causa do achado da Sessão 03 |

Dimensões: `dim_data` (calendário construído), `dim_geografia` (compartilhada entre cliente/vendedor via CEP), `dim_produto` (com SCD2), `dim_cliente`, `dim_vendedor`.

## 21. `dim_data` — calendário

Gerada via `sequence()` de datas (2016-01-01 a 2019-12-31), sem dependência de nenhuma outra tabela. `sk_data` no formato `YYYYMMDD` (convenção Kimball — permite filtro por range numérico sem join). 1.461 dias gerados (4 anos, incluindo 2016 bissexto), validado sem duplicata, escrito com sucesso.

Nota travada: `weekofyear()` do Spark segue ISO 8601 — primeiros dias de janeiro podem cair na semana 53 do ano anterior, dependendo do dia da semana em que o ano começa. Não é bug, é convenção.

## 22. `dim_geografia` — deduplicação de coordenadas por CEP

`geolocation` tem 1.000.163 linhas para 19.015 prefixos de CEP distintos — precisa de 1 linha por chave antes de virar dimensão. Decisão: **primeira ocorrência**, com critério de desempate **determinístico** via `row_number()` particionado por `zip_code_prefix`, ordenado por lat/lng (não há timestamp de captura na fonte para ordenar de forma mais significativa). Escolhida deliberadamente sobre "média de lat/lng" por simplicidade — documentado como trade-off consciente, não limitação não percebida.

Validado: 19.015 linhas, sem duplicata de `zip_code_prefix`, escrito com sucesso.

**Erro de infraestrutura do Unity Catalog descoberto nesta etapa:** tentativa de `spark.read.json()` na raiz exata de um container gerenciado (`abfss://bronze@.../`) colide com a `MANAGED LOCATION` do schema (`INVALID_PARAMETER_VALUE.LOCATION_OVERLAP`). Leitura por sub-path de entidade (`.../orders/`) funciona sem conflito — Unity Catalog só bloqueia o path raiz exato, não sub-caminhos.

## 23. `dim_produto` — SCD2 real, simulado sobre snapshot único

A fonte não tem histórico real de mudança de atributo — para provar que o mecanismo de versionamento funciona de verdade (não só estrutura de tabela vazia), uma mudança de categoria foi simulada em 5 produtos reais.

**Sequência implementada e validada:**
1. Carga inicial: 32.951 produtos, todos `flag_vigente = true`, `sk_produto` via `monotonically_increasing_id()`.
2. Simulação: 5 produtos reais (`product_id` extraídos da própria tabela) tiveram a categoria alterada artificialmente para `"eletronicos"` num "novo snapshot" — categorias originais confirmadas como diferentes (`perfumaria`, `artes`, `esporte_lazer`, `bebes`, `utilidades_domesticas`) antes de assumir que a simulação gerou mudança real.
3. Detecção de mudança: join entre novo snapshot e versão vigente atual, filtrando por categoria diferente — confirmado exatamente os 5 produtos simulados.
4. `MERGE` (passo 1): fecha a versão antiga (`flag_vigente = false`, `data_fim_vigencia = current_date()`).
5. Insert (passo 2): nova versão vigente, nova `sk_produto` continuando a partir do maior SK existente (`max_sk + 1`, para não colidir).

**Validação final (3 `assert`):** 32.956 versões totais (32.951 + 5 novas), 32.951 `product_id` únicos (nenhum produto novo criado, só versionado), 32.951 versões vigentes (exatamente 1 por produto, sem duplicidade). Todos passaram.

**Incidente de execução (não é bug de lógica):** estado do notebook se perdeu entre execuções (variáveis `produtos_para_mudar` e `df_mudancas` desapareceram da sessão serverless) depois do `MERGE` já ter rodado com sucesso e persistido na tabela. Recuperado reconstruindo as variáveis a partir do próprio estado já persistido (`flag_vigente == false` identifica os produtos já processados), sem precisar refazer o `MERGE`. Lição: sessões serverless podem cair silenciosamente; sempre inspecionar o notebook renderizado (`.ipynb`) para confirmar o que de fato rodou antes de assumir perda de trabalho.

**Notebook final limpo:** removidas as células de recuperação de estado (não fazem parte da lógica de negócio do SCD2) — versão final tem 8 células no fluxo lógico direto: carga inicial → SK → validação/escrita → simulação → detecção → fecha versão antiga → insere nova versão → validação final.

## 24. `dim_cliente` — grão de pessoa, não de pedido

**Achado de modelagem importante, decisão revisada em tempo real:** o dataset Olist tem `customer_id` (muda a cada pedido — é quase um ID de transação) e `customer_unique_id` (estável, identifica a pessoa real). Decisão inicial equivocada foi cogitar `customer_id` como grão da dimensão ("registros separados... fica melhor pra análise futura") — corrigida após entender que isso infla contagem de clientes reais e impede análise de recorrência/LTV. Grão correto: `customer_unique_id`. `customer_id` continua disponível como degenerate dimension no fato de vendas.

**Problema real encontrado:** 252 clientes (de ~99.441 pedidos) têm atributos de endereço inconsistentes entre pedidos diferentes (mudaram de CEP/cidade). Resolvido com **join com `orders`, pegando o endereço do pedido mais recente** (`row_number()` ordenado por `order_purchase_timestamp` desc) — critério diferente do usado em `dim_geografia` (lá não havia dado temporal disponível; aqui há, e representa de fato "onde o cliente mora agora", mais correto que "primeira ocorrência").

Validado: 96.096 clientes únicos, bate exato com `customer_unique_id` distintos na Silver. Mantém `cidade`/`estado`/`zip_code_prefix` embutidos na própria dimensão (redundante com `dim_geografia`, mas evita join extra para consultas rápidas — prática padrão Kimball).

## 25. `dim_vendedor` — simples, sem complicação de grão

`seller_id` já é chave única de verdade (confirmado antes de assumir, mesma disciplina do resto do projeto: 3.095 = 3.095). Sem necessidade de resolver inconsistência de endereço nem grão duplo. Carga direta, 3.095 vendedores, validado e escrito.

## 26. Os 3 fatos

**`fato_vendas`** (grão: item de pedido, 112.650 linhas):
- Join com `dim_produto` respeitando o **intervalo de vigência SCD2** (`data_inicio_vigencia` / `data_fim_vigencia` ajustada com `coalesce` para `9999-12-31` quando nula) — não apenas "pega a versão `flag_vigente = true`". Validado: nenhum fan-out (contagem se manteve 112.650 após o join), zero itens sem versão de produto correspondente.
- Joins subsequentes com `dim_cliente` (via `customer_unique_id`, obtido através de join com `customers`), `dim_vendedor`, `dim_data` — todos validados sem fan-out e sem órfão.
- Chave composta `(order_id, order_item_id)` validada como única na tabela final.

**`fato_pagamentos`** (grão: pagamento de pedido, 103.886 linhas): join com `dim_cliente` e `dim_data` via `orders`. Chave composta `(order_id, payment_sequential)` validada única.

**`fato_reviews`** (grão: `(review_id, order_id)`, 99.224 linhas): join com `dim_cliente` e `dim_data`, usando `review_creation_date` (não a data do pedido) como data de referência. Chave composta validada única.

### Erro de execução recorrente identificado (padrão, não caso isolado)

Estado de notebook se perdendo entre sessões serverless aconteceu de novo ao construir `fato_pagamentos` e `fato_reviews` (cada um em notebook próprio, variáveis de apoio como `df_orders`/`df_customers_silver`/`df_dim_cliente` precisaram ser recriadas no início de cada notebook novo — comportamento esperado quando se usa 1 notebook por fato, não é bug). Lição fixada: sempre que abrir um notebook novo, recriar os imports e tabelas de apoio no topo antes de qualquer lógica — não assumir que variáveis de outro notebook estão disponíveis.

## 27. Validação cruzada final — integridade referencial da Gold inteira

Bateria de `LEFT JOIN` de cada fato contra todas as dimensões que referencia, contando registros órfãos (esperado: zero em tudo):

- `fato_vendas` × (`dim_produto`, `dim_cliente`, `dim_vendedor`, `dim_data`): **0, 0, 0, 0**.
- `fato_pagamentos` × (`dim_cliente`, `dim_data`): **0, 0**.
- `fato_reviews` × (`dim_cliente`, `dim_data`): **0, 0**.

Gold com 100% de integridade referencial confirmada, não assumida.

## 28. Camada Publish — views analíticas de consumo final

Decisão de escopo: queries SQL primeiro (provam que o modelo responde pergunta de negócio real), Power BI depois (conectado via SQL Warehouse, reaproveitando a mesma lógica).

5 perguntas de negócio respondidas e validadas com dado plausível (comparado contra padrões conhecidos publicamente do dataset Olist real — concentração de vendedores em SP, categorias líderes em beleza/relógios, sazonalidade de início/fim de dataset):

1. Receita, frete e ticket médio por mês.
2. Top categorias por receita.
3. Top vendedores por receita e estado.
4. Satisfação média (`review_score`) por categoria — **junta fato com fato** (`fato_reviews` × `fato_vendas` via `order_id`) para alcançar a categoria do produto, já que review não tem granularidade de item; fan-out é intencional/esperado aqui (review de pedido multi-categoria aparece uma vez por categoria).
5. Distribuição de forma de pagamento.

**Nota de terminologia — revisão em relação ao registro inicial desta sessão:** a camada foi inicialmente cogitada como "Diamond layer" (termo não verificável publicamente, descartado). Posteriormente, confirmado que Ronaldo já usa esse padrão de camada pós-Gold profissionalmente na Expert, e Matheus (contato DB/Randoncorp) confirmou que a DB aplica a mesma prática — chamada de **"Publish"**. Não é cópia de terminologia sem lastro, é padrão validado por experiência profissional direta e por confirmação externa do próprio contato técnico do processo seletivo.

**Decisão de implementação:** schema próprio no catálogo — `olist_lakehouse.publish` — separado de `gold`, não apenas views com prefixo dentro do schema `gold` (opção descartada). Motivo: reflete separação real de camada, não só nomenclatura.

- `CREATE SCHEMA olist_lakehouse.publish`, **sem `MANAGED LOCATION` própria** — decisão consciente e tecnicamente correta: views não armazenam dado físico (são definição SQL recalculada a cada consulta), então não há necessidade de container ADLS/External Location dedicada, diferente de `bronze`/`silver`/`gold`, que hospedam tabelas Delta com arquivo físico real.
- 5 views recriadas em `olist_lakehouse.publish` (mesmo SQL, apontando ainda para as tabelas físicas em `olist_lakehouse.gold`), confirmadas via `SHOW VIEWS IN olist_lakehouse.publish` — `isTemporary = false`, `isMaterialized = false` nas 5, persistidas corretamente no metastore.

**Justificativa arquitetural registrada (para uso em post/entrevista):** Publish não existe para reduzir custo de storage (é desprezível, view não duplica dado) nem creates custo de computação relevante além do que já existiria com acesso direto à Gold. O valor real é de **governança e desacoplamento**: (1) superfície de acesso menor — consumidores de BI recebem permissão só em `publish`, nunca em `gold` diretamente; (2) contrato de interface estável — mudanças internas na Gold não quebram dashboards, desde que a viewexterna mantenha a mesma forma; (3) vocabulário de negócio exposto ao consumidor, não vocabulário técnico de modelagem dimensional (chave composta, SCD2, surrogate key ficam invisíveis para quem consome `publish`).

**Achado de dado, sem necessidade de correção:** categoria `not_defined` em `payment_type` com 3 registros e valor zero — resíduo de qualidade de dado do CSV original do Olist, não introduzido pelo pipeline. Volume irrelevante (3 em ~104 mil).

## 28.1. Governança de acesso — planejada, não implementada (decisão de custo-benefício)

Ao conectar o Power BI via Personal Access Token do usuário administrador do
workspace, confirmado que a árvore do catálogo inteira fica visível
(`bronze`/`silver`/`gold`/`publish`) — a separação por schema, sozinha, é só
organização lógica, não é controle de acesso real sem `GRANT` restritivo aplicado.

Tentativa de provar isso com um segundo usuário de teste (Databricks
Personal Access Token com permissão restrita só em `publish`) esbarrou em: (1)
convite por e-mail Gmail não permitido pelo Azure AD/Entra ID do tenant (exige
conta Microsoft nativa, não aceita Gmail puro sem conversão), (2) mesmo padrão de
fricção de configuração de identidade já visto na Sessão 03 (Access Connector,
Storage Credential).

**Decisão consciente, não lacuna:** dado o esforço (nova conta Microsoft, novo
convite, novo token, reconexão do Power BI) versus o ganho real (a resposta verbal
"eu aplicaria `GRANT SELECT` restrito por schema" já é tecnicamente correta e
suficiente para sustentar a pergunta em entrevista — provar com usuário de teste
adiciona valor incremental pequeno, já que governança de acesso é conceito, não
técnica de difícil verificação verbal), decidido **não implementar** o usuário de
teste. Comando de referência documentado, caso seja implementado no futuro:

```sql
GRANT USAGE ON CATALOG olist_lakehouse TO `<principal_consumidor_bi>`;
GRANT USAGE ON SCHEMA olist_lakehouse.publish TO `<principal_consumidor_bi>`;
GRANT SELECT ON SCHEMA olist_lakehouse.publish TO `<principal_consumidor_bi>`;
-- Sem GRANT equivalente em bronze/silver/gold para esse principal.
```

Power BI conectado usando o próprio token de administrador — as 5 views de
`publish` selecionadas manualmente no Navigator, ignorando `bronze`/`silver`/`gold`
mesmo visíveis na árvore (disciplina de escopo, não restrição técnica).

## 28.2. Expansão da camada Publish — views granulares para cross-filtering

Ao montar o primeiro visual de teste (receita mensal), identificado que as 5 views
originais são **agregadas** (`GROUP BY` fechado dentro do SQL) — cada uma já
calculada no seu próprio grão, sem chave compartilhada entre si. Isso impede
qualquer relacionamento real no Power BI: `vw_top_categorias` não tem coluna de
data, `vw_receita_mensal` não tem categoria — não há como um filtro num visual
afetar o outro (sem cross-filtering).

**Opção descartada (avaliada e rejeitada, com motivo):** conectar o Power BI
diretamente às tabelas físicas de `olist_lakehouse.gold` (fato + dimensões, que já
têm as FKs corretas e validadas). Tecnicamente mais simples, mas **fura o
princípio de governança que motivou a criação da camada Publish** — se o
consumidor de BI acessa `gold` diretamente assim que esbarra em fricção técnica, a
separação de camadas deixa de fazer sentido.

**Solução adotada:** réplica da estrutura de Star Schema como views não agregadas,
**dentro do próprio schema `publish`** — preserva a governança (BI nunca toca
`gold`) e resolve o cross-filtering (as views agora carregam as chaves `sk_*`
necessárias para o Power BI relacionar).

Views granulares criadas, com seleção explícita de coluna (sem `SELECT *`) e
nomes traduzidos para vocabulário de negócio:

- `vw_dim_data`, `vw_dim_produto` (inclui colunas de controle SCD2 —
  `data_inicio_vigencia`/`data_fim_vigencia`/`flag_vigente` — tratadas como
  informação de negócio válida, não detalhe técnico a esconder), `vw_dim_cliente`,
  `vw_dim_vendedor`.
- `vw_fato_vendas`, `vw_fato_pagamentos`, `vw_fato_reviews`.

**Decisão registrada: as 5 views agregadas originais foram mantidas**, não
substituídas pelas granulares — Publish passa a ter duas sub-camadas de propósito
distinto:
1. **Agregadas** (`vw_receita_mensal`, `vw_top_categorias`, `vw_top_vendedores`,
   `vw_satisfacao_categoria`, `vw_forma_pagamento`) — resposta pronta para consumo
   direto (SQL puro, script simples), sem necessidade de montar relacionamento.
2. **Granulares** (`vw_dim_*`, `vw_fato_*`) — modelo relacional que alimenta
   ferramentas de BI com cross-filtering real, onde a agregação acontece
   dinamicamente via DAX/medida, não pré-calculada em SQL.

**Ponto de atenção documentado para o relacionamento no Power BI:** `dim_produto`
tem SCD2 — `product_id` não é chave única (um produto pode ter múltiplas versões).
O relacionamento entre `vw_fato_vendas` e `vw_dim_produto` deve usar sempre
`sk_produto` (chave de versão), nunca `product_id` (chave natural, duplicada entre
versões) — usar a chave errada geraria relação muitos-para-muitos silenciosa.

## 28.3. Power BI — dashboard montado com cross-filtering real

Conectado via Databricks SQL Warehouse (Server Hostname + HTTP Path, autenticação
por Personal Access Token do usuário administrador — testada tentativa de usuário
de teste com permissão restrita, descartada por custo-benefício, ver 28.1).

**Correção de tipo de dado na importação:** coluna `nota` (de `vw_fato_reviews`)
importada pelo Power BI como Texto, mesmo sendo numérica na origem (Silver/Gold) —
causou erro `AVERAGE não pode trabalhar com valores do tipo String` na medida de
satisfação. Corrigido no próprio Power BI (Ferramentas de coluna → Tipo de dados →
Número Inteiro), não na origem — confirmado que a query SQL original já funcionava
com `AVG()`, então o problema era só de inferência na importação, não do dado em
si.

**Relação Muitos-para-Muitos entre `vw_fato_vendas` e `vw_fato_reviews`:** para
calcular satisfação por categoria de produto, foi necessário criar relação direta
entre as duas tabelas de fato via `order_id` (cardinalidade M:N, já que nenhuma
das duas colunas é única — um pedido tem múltiplos itens e pode gerar múltiplas
linhas de review por características do dataset). Direção do filtro cruzado
definida como "Ambos", para que o filtro de `categoria` (vindo via
`vw_dim_produto` → `vw_fato_vendas`) se propague até `vw_fato_reviews`. Sem essa
relação, a medida de nota média retornava o mesmo valor (geral, sem filtro real)
para todas as categorias, com uma categoria "(Em branco)" espúria — sintoma de
ausência de propagação de filtro através da cadeia de 2 saltos.

**Medidas DAX criadas** (tabela dedicada `medidas`, sem dado próprio — organização
que separa métrica calculada de coluna física, decisão do próprio Ronaldo, não
sugerida):
```dax
Receita Total = SUM(vw_fato_vendas[valor_produto])
Receita Frete = SUM(vw_fato_vendas[valor_frete])
Ticket Médio = DIVIDE(SUM(vw_fato_vendas[valor_produto]), DISTINCTCOUNT(vw_fato_vendas[order_id]))
Nota Média Review = AVERAGE(vw_fato_reviews[nota])
Valor Pago Total = SUM(vw_fato_pagamentos[valor_pagamento])
Total Reviews = COUNT(vw_fato_reviews[nota])
```

**Achado estatístico corrigido durante a montagem:** o primeiro filtro "Top N" no
gráfico de satisfação por categoria usou `Nota Média Review` como critério de
ordenação — trouxe categorias de nicho com pouquíssimas avaliações
(`cds_dvds_musicais` com nota 5 baseada em poucos reviews), estatisticamente não
confiável (amostra pequena infla/deflaciona a média). Corrigido usando
`Total Reviews` como critério do Top N — traz as categorias de maior volume real
(`beleza_saude`, `esporte_lazer`, etc.), consistentes com as mesmas categorias que
lideram receita.

**Dashboard final** (`Olist E-commerce - Painel Analítico`): 6 visuais — receita
mensal (linha), top categorias (barras), receita por estado (barras), satisfação
por categoria (barras), forma de pagamento (pizza), receita por vendedor/estado —
com filtros interativos de Ano, Mês, Estado e Categoria no cabeçalho, cross-
filtering funcional entre todos os visuais graças ao modelo relacional completo em
`publish`.

**Item deixado deliberadamente sem ajuste fino:** escala do eixo Y do gráfico de
satisfação por categoria não foi customizada (permanece de 0 a 4, comprimindo
visualmente a diferença real entre notas de 3.9 a 4.6) — decisão consciente de
parar o refinamento visual nesse ponto, dado que o dado subjacente está correto e
o ganho de legibilidade adicional não justificava mais tempo de ajuste.

## 29.1. Nota sobre metodologia desta etapa

Diferente das etapas anteriores (Bronze/Silver/Gold, feitas com validação
numérica rigorosa a cada passo), a etapa de Power BI teve caráter mais
iterativo/visual — várias tentativas de ajuste de eixo, tipo de gráfico e
ordenação foram feitas por tentativa e erro visual, não por cálculo prévio. Isso é
apropriado para a natureza da tarefa (design de dashboard é mais subjetivo que
engenharia de pipeline), mas contrasta com o rigor das camadas anteriores — vale
ter isso em mente ao relatar o projeto: a força do projeto está na engenharia de
dados (Bronze→Silver→Gold→Publish), o BI é a "vitrine" construída sobre essa base,
não o ponto mais sofisticado tecnicamente.

## 29. README criado

Primeiro `README.md` do projeto — consolidação de arquitetura, decisões, estado atual e achados de qualidade de dado, para leitura rápida por terceiros (diferente do log de sessão, que é histórico de processo). Estrutura: visão geral, arquitetura em camadas, decisões justificadas, tabela de estado da Bronze, resumo de Silver, Gold com seção dedicada ao SCD2, achados de qualidade de dado, camada de consumo, stack, estado atual e itens conscientemente adiados.

## 30. Notas para continuidade em outro chat (fim da Sessão 04)

- Gold 100% completa e validada: 5 dimensões + 3 fatos, SCD2 demonstrado de ponta a ponta, integridade referencial confirmada.
- Camada Publish expandida: 5 views agregadas (consumo direto) + 7 views granulares espelhando fato/dimensões (para modelo relacional de BI) — 12 views totais, schema `olist_lakehouse.publish`.
- **Power BI concluído**: dashboard `Olist E-commerce - Painel Analítico` com 6 visuais, modelo relacional completo (todas as relações fato↔dimensão validadas, incluindo M:N entre os dois fatos onde necessário), filtros interativos, medidas DAX organizadas em tabela própria.
- README ainda **não atualizado** para refletir a expansão de Publish (views granulares) nem o Power BI — só tem a versão anterior, com as 5 views agregadas originais.
- Próximo passo combinado: CI/CD (Azure DevOps), depois teste de arquitetura sem consulta (Ronaldo explicando SCD2, grão de dim_cliente, chave composta de reviews de cabeça, sem abrir notebook/log).
- Padrão recorrente identificado: sessões serverless do Databricks podem perder estado de notebook entre execuções sem aviso — sempre confirmar variáveis em memória antes de assumir continuidade, especialmente em notebooks separados por fato/dimensão.
- Verificar se o cluster/serverless não ficou consumindo recurso à toa entre sessões (alerta recorrente, ainda válido).
