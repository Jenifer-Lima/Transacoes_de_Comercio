import psycopg2
import polars as pl
import io
import csv
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


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
dim_tipo_transacao = carregar_tabela_postgres("dim_tipo_transação")
fato_transacao = carregar_tabela_postgres("fato_transacao")
print(dim_tipo_transacao)

fato_transacao = fato_transacao.join(
    dim_tipo_transacao,
    left_on="sk_tipo_transação",
    right_on="sk_tipo_transação").filter(pl.col("descrição_tipo_transação")=="EXPORT")


exportacoes = fato_transacao.join(
    dim_pais,
    left_on="sk_pais_origem",
    right_on="sk_pais"
)

print(fato_transacao)

#Agrupar e somar o valor
exportacoes_por_pais = exportacoes.group_by("nome_pais").agg(
    pl.col("valor_monetario").sum().alias("total_valor_monetario")
    ).sort("total_valor_monetario", descending=True)

print(exportacoes_por_pais)

df = exportacoes_por_pais.to_pandas()
df["percentual"] = 100 * df["total_valor_monetario"] / df["total_valor_monetario"].sum()
df["acumulado"] = df["percentual"].cumsum()

# --- Gráfico de Pareto ---
fig, ax1 = plt.subplots(figsize=(9, 6))

# Barras
bars = ax1.bar(df["nome_pais"], df["total_valor_monetario"], color="blue")
ax1.set_ylabel("Total Exportado (US$)")
ax1.tick_params(axis='x', rotation=0)
ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

# Adiciona os valores no topo de cada barra
for bar in bars:
    yval = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width() / 2, yval + yval*0.01, f"${yval:,.0f}",
             ha='center', va='bottom', fontsize=8)

# Linha acumulada (se quiser adicionar futuramente)

plt.title("Países com maior volume de exportação em valor monetário")
plt.tight_layout()
plt.show()