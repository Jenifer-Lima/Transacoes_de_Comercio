import psycopg2
import polars as pl
import io
import csv
import matplotlib.pyplot as plt
import plotly.express as px
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

dim_produto = carregar_tabela_postgres("dim_produto")
fato_transacao = carregar_tabela_postgres("fato_transacao")

fato_transacao = fato_transacao.join(
    dim_produto,
    left_on="sk_produto",
    right_on="sk_produto")
print(fato_transacao)

prod_mais_comercializados = fato_transacao.group_by(
    "descrição_produto"
    ).agg(
    pl.len().alias("quantidade_transações")
    ).sort("quantidade_transações", descending=True)
print(prod_mais_comercializados)

df_produtos = prod_mais_comercializados.to_pandas()

#Gráfico

plt.figure(figsize=(12, 6))
plt.bar(df_produtos["descrição_produto"], df_produtos["quantidade_transações"], color="blue")
plt.title("Produtos mais comercializados")
plt.xlabel("Produto", fontsize=12)
plt.ylabel("Quantidade de Transações", fontsize=12)
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()


