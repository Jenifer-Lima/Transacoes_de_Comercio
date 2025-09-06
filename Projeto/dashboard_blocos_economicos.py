import psycopg2
import polars as pl
import io
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

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
fato_transacao = carregar_tabela_postgres("fato_transacao")

dados = fato_transacao.join(dim_pais, left_on="sk_pais_origem", right_on="sk_pais")\
    .join(dim_tempo, left_on="sk_data", right_on="sk_data")


dados_agrupados_bloco = dados.group_by(["ano","bloco_economico"]).agg(
    pl.sum("valor_monetario").alias("valor_total_bloco")
)
print(dados_agrupados_bloco)

def formatar_valores(x, pos):
    return f"${x:,.0f}"
df = dados_agrupados_bloco.to_pandas().pivot(index="ano", columns="bloco_economico",values="valor_total_bloco").fillna(0)
df = df.sort_index()
# Plot
plt.figure(figsize=(12, 6))
plt.stackplot(df.index, df.T.values, labels=df.columns, linewidth=0.5)
plt.title("Evolução do comércio por Bloco Econômico ao longo do tempo")
plt.xlabel("Ano", fontsize=12)
plt.ylabel("Valor Monetário (US$)", fontsize=12)
plt.gca().yaxis.set_major_formatter(FuncFormatter(formatar_valores))
plt.legend(loc='upper left')
plt.tight_layout()
plt.show()