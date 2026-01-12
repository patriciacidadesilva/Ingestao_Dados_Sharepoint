# 📊 PySpark + SharePoint + Databricks  
## Exemplo prático de Engenharia de Dados (end-to-end)

---

## 🎯 Objetivo do Projeto

Este notebook foi desenvolvido com **finalidade demonstrativa**, com o objetivo de **mostrar como construir um pipeline em PySpark que consome dados diretamente do SharePoint**, realiza tratamentos e persiste os dados em uma **tabela Delta no Databricks (Lakehouse)**.

O foco não é apenas o dado em si, mas **o COMO fazer**:
- Como autenticar no SharePoint de forma segura
- Como acessar arquivos (Excel) via **Microsoft Graph API**
- Como integrar Pandas + Spark de forma eficiente
- Como aplicar boas práticas de **Engenharia de Dados em PySpark**

Este projeto serve como **referência técnica reutilizável** para cenários reais de ingestão de dados corporativos.

---

## 🧠 O que este código demonstra na prática

✔ Integração entre **SharePoint e Databricks**  
✔ Uso de **Microsoft Graph API** para leitura de arquivos  
✔ Conversão de dados externos em **Spark DataFrames**  
✔ Padronização e enriquecimento de dados  
✔ Aplicação de regras de negócio  
✔ Persistência governada em **Delta Lake**  

---

## 🏗️ Arquitetura do Fluxo

**SharePoint (Excel)**  
⬇  
**Microsoft Graph API**  
⬇  
**Pandas (leitura inicial)**  
⬇  
**PySpark (transformações)**  
⬇  
**Delta Table (Databricks Lakehouse)**

---

## 🔐 Autenticação e Segurança

O acesso ao SharePoint é feito via **Azure AD App Registration**, utilizando:
- `CLIENT_ID`
- `CLIENT_SECRET`
- `TENANT_ID`

As credenciais são armazenadas de forma segura em **Databricks Secrets**, seguindo boas práticas de segurança e governança.

---

## 🧩 Etapas do Pipeline (passo a passo)

### 1️⃣ Preparação do ambiente
- Instalação de dependências (`openpyxl`)
- Importação de bibliotecas necessárias
- Reinicialização do kernel Python

### 2️⃣ Autenticação no Microsoft Graph
- Geração de token OAuth2 com MSAL
- Configuração dos headers HTTP

### 3️⃣ Descoberta do SharePoint
- Localização do `site_id`
- Identificação do `drive_id` correto (Documentos / Shared Documents)

### 4️⃣ Leitura do arquivo Excel
- Download do arquivo diretamente do SharePoint
- Leitura via Pandas
- Fallback automático de abas, quando necessário

### 5️⃣ Tratamento e padronização
- Normalização de nomes de colunas
- Ajustes de nomenclatura para compatibilidade
- Criação de colunas derivadas
- Padronização de chaves (ex.: zero à esquerda)

### 6️⃣ Conversão para Spark
- Transformação de Pandas DataFrame → Spark DataFrame
- Garantia de tipos e estrutura

### 7️⃣ Persistência no Lakehouse
- Escrita em formato **Delta**
- Modo `overwrite` com `overwriteSchema`
- Atualização controlada da tabela final

---

## 📦 Output Final

📌 **Tabela Delta criada/atualizada:**
{catalog}.planejamento.dim_de_para_categoria

Essa tabela fica pronta para:
- Consumo analítico
- Dashboards (Power BI, Databricks SQL)
- Pipelines downstream
- Automação e IA

---

## 🧪 Por que este exemplo é importante?

Porque **SharePoint é uma fonte extremamente comum** em ambientes corporativos — e raramente bem tratada do ponto de vista de engenharia.

Este código mostra como:
- Sair do “download manual”
- Automatizar ingestões recorrentes
- Criar pipelines escaláveis e governados
- Transformar arquivos operacionais em **ativos de dados**

---

## 🔁 Reutilização

O padrão apresentado aqui pode ser facilmente adaptado para:
- Outros arquivos Excel ou CSV
- Outras bibliotecas do SharePoint
- Diferentes domínios de negócio
- Pipelines produtivos ou provas de conceito (PoC)

---

## 👩‍💻 Autoria & Contexto

Este projeto faz parte do meu **portfólio em Engenharia de Dados**, com foco em:
- Databricks
- PySpark
- Integrações corporativas
- Automação e IA aplicada a dados


## 🏗️ Diagrama de Arquitetura — SharePoint → Databricks (Lakehouse)
  ┌────────────────────┐
│   SharePoint       │
│  (Excel / Tabela)  │
└─────────┬──────────┘
          │
          │ Microsoft Graph API
          │ (OAuth2 + Azure AD)
          ▼
┌────────────────────┐
│   Databricks       │
│  Notebook PySpark  │
│                    │
│ • Autenticação     │
│ • Download Excel   │
│ • Pandas           │
│ • PySpark          │
└─────────┬──────────┘
          │
          │ Transformações
          │ Padronização
          ▼
┌────────────────────┐
│   Delta Lake       │
│ (Lakehouse Table)  │
│ dim_de_para_categoria │
└─────────┬──────────┘
          │
          │ Consumo Analítico
          ▼
┌────────────────────┐
│ BI / Analytics     │
│ Power BI / SQL     │
│ Automação / IA     │
└────────────────────┘


![Diagrama de Arquitetura](https://github.com/user-attachments/assets/b6f649f1-8722-4f69-afe3-113181a56daa)


