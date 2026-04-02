# Marketing Dashboard — Setup

## 1. Estrutura esperada do Google Sheets

Crie uma planilha com **4 abas** com os seguintes nomes e colunas:

### Aba "Ads"
| Date | Campaign Name | Ad Set Name | Ad Name | Spend | Impressions | Clicks |
|------|--------------|-------------|---------|-------|-------------|--------|
| 2024-03-01 | Campanha A | Conjunto 1 | Criativo Video | 500.00 | 50000 | 750 |

### Aba "Leads"
| Date | Email | Campaign | Ad Set | Ad Name |
|------|-------|----------|--------|---------|
| 2024-03-01 | lead@email.com | Campanha A | Conjunto 1 | Criativo Video |

### Aba "Appointments"
| Date | Email | Campaign |
|------|-------|----------|
| 2024-03-02 | lead@email.com | Campanha A |

### Aba "Sales"
| Date | Email | Campaign | Revenue |
|------|-------|----------|---------|
| 2024-03-05 | lead@email.com | Campanha A | 2000.00 |

> **Importante:** Os nomes das colunas podem variar (o sistema detecta automaticamente variações comuns em português e inglês). O Email é o campo de join entre as tabelas.

---

## 2. Configurar Google Service Account

1. Acesse [console.cloud.google.com](https://console.cloud.google.com)
2. Crie um projeto (ou use um existente)
3. Ative as APIs: **Google Sheets API** e **Google Drive API**
4. Vá em **IAM & Admin → Service Accounts → Create Service Account**
5. Crie a conta e clique em **Manage Keys → Add Key → JSON**
6. Baixe o arquivo JSON e salve como `credentials.json` na pasta do projeto
7. Copie o email da service account (ex: `dashboard@projeto.iam.gserviceaccount.com`)
8. **Compartilhe sua planilha com esse email** (permissão de leitura)

---

## 3. Configurar o projeto

```bash
# 1. Entre na pasta do projeto
cd marketing-dashboard

# 2. Crie o ambiente virtual
python -m venv venv

# Windows
venv\Scripts\activate
# Mac/Linux
source venv/bin/activate

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Copie e edite o .env
cp .env.example .env
```

Edite o arquivo `.env`:
```
GOOGLE_CREDENTIALS_FILE=credentials.json
SPREADSHEET_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms   # ID da sua planilha (na URL)
ADS_SHEET_NAME=Ads
LEADS_SHEET_NAME=Leads
APPOINTMENTS_SHEET_NAME=Appointments
SALES_SHEET_NAME=Sales
ANTHROPIC_API_KEY=sk-ant-...   # Opcional — para insights com IA
```

O **Spreadsheet ID** está na URL da planilha:
```
https://docs.google.com/spreadsheets/d/[SPREADSHEET_ID]/edit
```

---

## 4. Executar

```bash
python app.py
```

Abra: **http://localhost:5000**

---

## 5. Insights com IA (opcional)

Para habilitar os insights gerados pelo Claude:

1. Acesse [console.anthropic.com](https://console.anthropic.com) e gere uma API key
2. Adicione no `.env`: `ANTHROPIC_API_KEY=sk-ant-...`
3. Reinicie o servidor

Os insights são gerados a cada 5 minutos para evitar custos excessivos.

---

## 6. Nomes de colunas personalizados

Se suas colunas tiverem nomes diferentes, edite o arquivo `data_processor.py` nas seções `ADS_COLS`, `LEADS_COLS`, etc., adicionando o nome exato da sua coluna à lista de candidatos.

---

## 7. Solução de problemas

| Problema | Solução |
|----------|---------|
| "Sheet not found" | Verifique o nome da aba e o `SPREADSHEET_ID` no `.env` |
| "403 Permission denied" | Compartilhe a planilha com o email da service account |
| "Credentials file not found" | Coloque `credentials.json` na pasta raiz do projeto |
| Dashboard não atualiza | Verifique `SHEETS_REFRESH_INTERVAL` no `.env` |
| Insights não aparecem | Verifique `ANTHROPIC_API_KEY` no `.env` |
