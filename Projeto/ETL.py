import psycopg2
import polars as pl
import io
import csv
import requests
import time

# Conexão com o PostgreSQL
def carregar_tabela_postgres(nome_tabela):
    conn = psycopg2.connect(
        dbname="Transações",    # Nome do seu banco
        user="postgres",        # Usuário
        password="2022",        # Senha
        host="localhost",
        port="5432"
    )

    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {nome_tabela}")

    col_names = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    # Converter pra CSV em memória
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(col_names)
    writer.writerows(rows)
    csv_buffer.seek(0)

    cur.close()
    conn.close()
    return pl.read_csv(csv_buffer)

dim_pais = carregar_tabela_postgres("dim_pais")
dim_tempo = carregar_tabela_postgres("dim_tempo")
dim_produto = carregar_tabela_postgres("dim_produto")
dim_cambio = carregar_tabela_postgres("dim_cambio")
dim_transporte = carregar_tabela_postgres("dim_transporte")
dim_tipo_transacao = carregar_tabela_postgres("dim_tipo_transação")
fato_transacao = carregar_tabela_postgres("fato_transacao")

dim_transporte = dim_transporte.with_columns([
    pl.col("descricao_transporte").str.to_uppercase()
])

dim_produto = dim_produto.with_columns([
    pl.col("descrição_produto").str.to_uppercase(),
    pl.col("categoria_produto").str.to_uppercase()
])

dim_tipo_transacao = dim_tipo_transacao.with_columns([
    pl.col("descrição_tipo_transação").str.to_uppercase()
])

dim_pais = dim_pais.with_columns([
    pl.col("nome_pais").str.to_uppercase(),
    pl.col("bloco_economico").str.to_uppercase()
])

dim_cambio = dim_cambio.with_columns([
    pl.col("descrição_moeda_origem").str.to_uppercase(),
    pl.col("descrição_moeda_destino").str.to_uppercase()
])

# Conexão separada para atualizar o PostgreSQL
conn = psycopg2.connect(
    dbname="Transações",
    user="postgres",
    password="2022",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Iterar sobre a tabela dim_cambio carregada com Polars
for row in dim_cambio.iter_rows(named=True):
    sk = row['sk_cambio']
    data = row['data']
    moeda_origem = row['descrição_moeda_origem']
    moeda_destino = row['descrição_moeda_destino']
    taxa_banco = round(row['taxa_cambio'], 2)

    # Chamada da API Frankfurter
    url = f"https://api.frankfurter.app/{data}?from={moeda_origem}&to={moeda_destino}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            resultado = response.json()
            taxa_api = resultado["rates"].get(moeda_destino)
            if taxa_api is not None:
                taxa_api = round(taxa_api, 2)
                if taxa_api != taxa_banco:
                    #Para ter um controle de quais foram atualizadas e quais estavam corretas
                    print(f"[ATUALIZA] {data} - {moeda_origem}->{moeda_destino}: {taxa_banco} → {taxa_api}")
                    cur.execute("""
                        UPDATE dim_cambio
                        SET taxa_cambio = %s
                        WHERE sk_cambio = %s
                    """, (taxa_api, sk))
                    conn.commit()
                else:
                    print(f"[OK] {data} - {moeda_origem}->{moeda_destino}: {taxa_banco}")
            else:
                print(f"[ERRO] Taxa não encontrada para {moeda_destino} em {data}")
        else:
            print(f"[ERRO] Falha na API ({response.status_code}) para {data}")
    except Exception as e:
        print(f"[ERRO] {data} - Exceção: {str(e)}")

    time.sleep(1)
cur.close()
conn.close()
