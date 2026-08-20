# Achados de qualidade de dado

Três problemas reais identificados por validação numérica (contagem, unicidade de
chave), não por erro de execução. Cada um documentado com causa raiz, correção e
revalidação — não apenas "corrigido e seguido em frente".

---

## 1. CEP truncado (perda de zero à esquerda)

**Sintoma:** `geolocation_zip_code_prefix` retornando `1037` em vez de `01037`.

**Investigação:** hipótese inicial descartada após checagem — não é característica
do dataset original (confirmado no CSV fonte: zero à esquerda presente,
`"01037"`). Causa raiz real: `pd.read_csv()` no backend da API, sem `dtype`
explícito, inferindo a coluna como `int64` e descartando o zero antes mesmo da
serialização para JSON.

**Escopo:** replicado em 3 colunas — `geolocation_zip_code_prefix`,
`customer_zip_code_prefix`, `seller_zip_code_prefix` — confirmado por inspeção
direta dos 3 CSVs antes de aplicar qualquer correção.

**Correção:**
```python
customers_df = pd.read_csv("data/olist_customers_dataset.csv", dtype={"customer_zip_code_prefix": str})
sellers_df = pd.read_csv("data/olist_sellers_dataset.csv", dtype={"seller_zip_code_prefix": str})
geolocation_df = pd.read_csv("data/olist_geolocation_dataset.csv", dtype={"geolocation_zip_code_prefix": str})
```

**Revalidação:** redeploy da API, teste isolado do endpoint, re-ingestão dos 3
pipelines Bronze afetados, confirmação de casos reais com zero à esquerda
presentes no arquivo final — não apenas verificação de tipo.

---

## 2. `review_id` não é chave primária isolada

**Sintoma:** contagem total de `reviews` batia (99.224), mas
`COUNT(DISTINCT review_id)` retornava 98.410 — 814 valores repetidos.

**Investigação:** casos duplicados analisados antes de assumir bug de pipeline —
mesmo `review_id`, mesma data, mesma nota, porém **`order_id` diferentes**. Não é
duplicata de linha idêntica, é o mesmo review associado a múltiplos pedidos —
comportamento real do dataset Olist original (não introduzido pelo pipeline).

**Implicação de modelagem:** a granularidade real da entidade é a chave composta
`(review_id, order_id)`, não `review_id` isolado. Confirmado:
`COUNT(DISTINCT CONCAT(review_id, '-', order_id))` bate exato com o total.

**Aplicado em:** `fato_reviews` usa `(review_id, order_id)` como chave composta.
No Power BI, a relação entre `fato_vendas` e `fato_reviews` precisou ser
modelada como Muitos-para-Muitos, refletindo essa característica real.

---

## 3. Tipo incorreto em `product_id` (order_items)

**Sintoma:** `product_id` declarado inicialmente como `LongType()` no schema
explícito da Silver.

**Causa:** confusão entre "campo-ID que parece número" e "campo numérico de
fato" — `product_id` é hash alfanumérico (`4244733e06e7ecb4970a6e2683c13e61`),
não número.

**Verificação antes da correção:** confirmado programaticamente, percorrendo
todas as 1.127 páginas do arquivo Bronze, que `product_id` nunca é nulo em
nenhum dos 112.650 registros — validando o `nullable=False` do schema corrigido.

**Correção:** schema ajustado para `StringType()`.

---

## Padrão comum aos três achados

Nenhum foi descoberto por acidente — todos emergiram de um princípio aplicado
consistentemente no projeto: **nunca confiar em "rodou sem erro" como prova de
integridade**. Cada camada (Bronze, Silver, Gold) tem validação de contagem e
unicidade de chave antes de qualquer escrita, e qualquer resultado inesperado foi
investigado até a causa raiz antes de ser corrigido.
