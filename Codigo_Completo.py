# ============================================================
# 0) Instalação das libs 
# ============================================================

# Instala biblioteca para leitura de arquivos Excel (.xlsx)
%pip install openpyxl

# Reinicia o interpretador Python dentro do Databricks para garantir que as novas libs fiquem disponíveis
dbutils.library.restartPython()

# ============================================================
# 1) Importação das bibliotecas
# ============================================================

import io                     # trabalha com arquivos em memória
import re                     # expressões regulares (buscar/substituir textos)
import unicodedata            # normalizar caracteres (ex.: remover acentos)
import requests               # chamadas HTTP (APIs)
import pandas as pd           # manipulação de dados (DataFrames)
from msal import ConfidentialClientApplication  # autenticação Azure AD

from pyspark.sql import SparkSession            # inicia a sessão Spark
from pyspark.sql import functions as F          # funções do PySpark (colunas, filtros, etc.)
from pyspark.sql.types import StringType        # tipo de dado texto no Spark


# ============================================================
# 2) Parâmetros de execução do notebook
# ============================================================
multiple_run_parameters = dbutils.notebook.entry_point.getCurrentBindings()
catalog = multiple_run_parameters.get("catalog") or "develop"
schema = "planejamento"


# ============================================================
# 3) Parâmetros de origem/arquivo
# ============================================================
HOST        = "nome_da_empresa.sharepoint.com"              # domínio do SharePoint
SEARCH_SITE = "cscgp"                                      # site onde está o arquivo
BASE_FOLDER = "02_Governança/02_Indicadores/2026/Analytics/OTC"  # pasta do arquivo
FILE_NAME   = "de---para categoria.xlsx"                   # nome do arquivo
SHEET_NAME  = "Planilha1"                                  # aba do Excel (usa a 1ª se não achar)


# ============================================================
# 4) Segredos (App Registration)
# ============================================================
TENANT_ID = dbutils.secrets.get(
    scope="CRIAR_SCOPE_SHAREPOINT",
    key="CRIAR_SENHA_TENANT_ID"
)

CLIENT_ID = dbutils.secrets.get(
    scope="CRIAR_SCOPE_O365_CONNECT",
    key="CRIAR_SENHA_CLIENT_ID"
)

CLIENT_SECRET = dbutils.secrets.get(
    scope="CRIAR_SCOPE_O365_CONNECT",
    key="CRIAR_SENHA_CLIENT_SECRET"
)

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"   # endpoint de login do Azure AD
SCOPES    = ["https://graph.microsoft.com/.default"]           # permissões para acessar o Graph API
GRAPH     = "https://graph.microsoft.com/v1.0"                 # endpoint base do Graph


# ============================================================
# 5) Helpers: autenticação no Graph e padronização de nomes de colunas
# ============================================================
def acquire_token():
    # Autentica no Azure AD e retorna o token do Graph API
    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )
    result = (
        app.acquire_token_silent(SCOPES, account=None)
        or app.acquire_token_for_client(scopes=SCOPES)
    )
    if "access_token" not in result:
        raise RuntimeError(f"[AUTH] Falha ao obter token Graph: {result}")
    return result["access_token"]


def norm_col(text: str) -> str:
    """
    Normaliza nomes de colunas: remove acentos/símbolos, junta palavras e deixa em minúsculas.
    Ex.: 'Razao Social' -> 'razaosocial'
    """
    txt = (text or "").strip()
    nfkd = unicodedata.normalize("NFKD", txt)
    no_acc = "".join(c for c in nfkd if not unicodedata.combining(c))
    no_sym = re.sub(r"[^A-Za-z0-9]", "", no_acc)
    return no_sym.lower()


TOKEN   = acquire_token()                         # gera token de acesso
HEADERS = {"Authorization": f"Bearer {TOKEN}"}    # cabeçalho HTTP p/ Graph


# ============================================================
# 6) Descobre o site_id por busca
# ============================================================
def find_site_id(host: str, search_term: str) -> str:
    r = requests.get(
        f"{GRAPH}/sites?search={search_term}",
        headers=HEADERS,
        timeout=30
    )
    r.raise_for_status()
    data = r.json().get("value", [])

    candidates = [
        s for s in data
        if s.get("siteCollection")
        and s.get("webUrl", "").startswith(f"https://{host}")
    ]

    if not candidates:
        raise RuntimeError(
            f"[SITE SEARCH] Nenhum site encontrado com termo '{search_term}' em {host}."
        )

    site = candidates[0]
    print(f"[SITE] Encontrado: {site.get('name')} | webUrl={site.get('webUrl')}")
    return site["id"]


SITE_ID = find_site_id(HOST, SEARCH_SITE)


# ============================================================
# 7) Descobre o drive do site
# ============================================================
def find_drive_id(site_id: str) -> str:
    r = requests.get(
        f"{GRAPH}/sites/{site_id}/drives",
        headers=HEADERS,
        timeout=30
    )
    r.raise_for_status()
    drives = r.json().get("value", [])

    if not drives:
        raise RuntimeError("[DRIVES] Nenhuma drive encontrada no site.")

    preferred = ["Shared Documents", "Documentos", "Documentos Compartilhados"]
    chosen = (
        next(
            (d for pref in preferred for d in drives if d.get("name", "").lower() == pref.lower()),
            None
        )
        or drives[0]
    )

    print(f"[DRIVE] Usando: name={chosen.get('name')} id={chosen.get('id')}")
    return chosen["id"]


DRIVE_ID = find_drive_id(SITE_ID)


# ============================================================
# 8) Baixa o arquivo por caminho
# ============================================================
def download_excel_by_path(site_id, drive_id, base_folder, file_name) -> bytes:
    rel_path = f"{base_folder.strip('/')}/{file_name}".strip("/")
    url = f"{GRAPH}/sites/{site_id}/drives/{drive_id}/root:/{rel_path}:/content"

    r = requests.get(url, headers=HEADERS, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(
            f"[DOWNLOAD] HTTP {r.status_code}: {r.text} | path: {rel_path}"
        )
    return r.content


content = download_excel_by_path(
    SITE_ID, DRIVE_ID, BASE_FOLDER, FILE_NAME
)


# ============================================================
# 9) Lê o Excel e converte para Spark
# ============================================================
try:
    pdf = pd.read_excel(
        io.BytesIO(content),
        sheet_name=SHEET_NAME,
        engine="openpyxl"
    )
except ValueError:
    xls = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
    first_sheet = xls.sheet_names[0]
    print(f"[SHEET] '{SHEET_NAME}' não encontrada. Usando '{first_sheet}'.")
    pdf = pd.read_excel(io.BytesIO(content), sheet_name=first_sheet, engine="openpyxl")

pdf = pdf.fillna("").astype(str)

spark = SparkSession.builder.appName(
    "GraphExcel_dim_de_para_categoria"
).getOrCreate()

sdf = spark.createDataFrame(pdf)


# ============================================================
# 10) Normalização de cabeçalhos
# ============================================================
for c in sdf.columns:
    newc = norm_col(c)
    if newc and newc != c:
        sdf = sdf.withColumnRenamed(c, newc)

rename_map = {
    "razao_social": "razaosocial",
    "informacao_pdd": "informacaopdd",
    "cnpj_": "cnpj",
}

for old, new in rename_map.items():
    if old in sdf.columns and new not in sdf.columns:
        sdf = sdf.withColumnRenamed(old, new)

for col in sdf.columns:
    if col.endswith("_") and col[:-1] not in sdf.columns:
        sdf = sdf.withColumnRenamed(col, col[:-1])


# ============================================================
# 11) Colunas derivadas
# ============================================================
if "conta" not in sdf.columns:
    raise RuntimeError("Coluna 'conta' não encontrada após normalização.")

sdf = sdf.withColumn("conta", F.col("conta").cast(StringType()))
sdf = sdf.withColumn("contagem", F.length(F.col("conta")))

sdf = sdf.withColumn(
    "conta_final",
    F.when(F.col("conta") == "BPRC04", F.col("conta"))
     .when(F.col("contagem") < 10, F.lpad(F.col("conta"), 10, "0"))
     .otherwise(F.col("conta"))
)

if "daingestao" not in [c.lower() for c in sdf.columns]:
    sdf = sdf.withColumn("daingestao", F.current_timestamp())


# ============================================================
# 12) Organização das colunas
# ============================================================
expected_cols = [
    "sap", "conta", "tipo", "cnpj", "razaosocial",
    "informacaopdd", "contagem", "conta_final", "daingestao"
]

ordered = (
    [c for c in expected_cols if c in sdf.columns]
    + [c for c in sdf.columns if c not in expected_cols]
)

sdf = sdf.select(*ordered)


# ============================================================
# 13) Salvando o DataFrame como Delta Table
# ============================================================
table = f"{catalog}.{schema}.dim_de_para_categoria"

(
    sdf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table)
)

print(f"✅ Tabela '{table}' atualizada com sucesso!")
