import psycopg2
import polars as pl
import io
import csv
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

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
dim_cambio = carregar_tabela_postgres("dim_cambio").sort("sk_cambio")
dim_transporte = carregar_tabela_postgres("dim_transporte")
dim_tipo_transacao = carregar_tabela_postgres("dim_tipo_transação")
fato_transacao = carregar_tabela_postgres("fato_transacao")

# Filtrar exportação e importação
exportacao = fato_transacao.join(
    dim_tipo_transacao,
    left_on="sk_tipo_transação",
    right_on="sk_tipo_transação"
).filter(pl.col("descrição_tipo_transação") == "EXPORT")

importacao = fato_transacao.join(
    dim_tipo_transacao,
    left_on="sk_tipo_transação",
    right_on="sk_tipo_transação"
).filter(pl.col("descrição_tipo_transação") == "IMPORT")

# Join com a tabela de tempo para obter o ano
exportacao_com_ano = exportacao.join(
    dim_tempo,
    left_on="sk_data",
    right_on="sk_data"
).select(["ano", "valor_monetario"])

importacao_com_ano = importacao.join(
    dim_tempo,
    left_on="sk_data",
    right_on="sk_data"
).select(["ano", "valor_monetario"])

# Agrupar por ano e somar os valores
exportacao_ano = exportacao_com_ano.group_by("ano").agg(
    pl.sum("valor_monetario").alias("valor_exportado")
)

importacao_ano = importacao_com_ano.group_by("ano").agg(
    pl.sum("valor_monetario").alias("valor_importado")
)

print(importacao_com_ano)
# Juntar as duas tabelas de exportação e importação por ano
df_valores_ano = exportacao_ano.join(importacao_ano, on="ano", how="full")
df_valores_ano = df_valores_ano.to_pandas()

# Gráfico de barras
fig, ax = plt.subplots(figsize=(10, 6))

# Plotando as barras para exportação e importação
bar1 = ax.bar(df_valores_ano["ano"] - 0.2, df_valores_ano["valor_exportado"], width=0.4, label="Exportação", color="blue")
bar2 = ax.bar(df_valores_ano["ano"] + 0.2, df_valores_ano["valor_importado"], width=0.4, label="Importação", color="red")

# Títulos e rótulos
ax.set_title("Valor Total Importado e Exportado por Ano", fontsize=14)
ax.set_xlabel("Ano", fontsize=12)
ax.set_ylabel("Valor Monetário (US$)", fontsize=12)
ax.legend()

# Formatar o eixo Y com separador de milhar
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{int(x):,}'.replace(",", ".")))


# Adiciona os valores nas barras
for bar in bar1:
    altura = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, altura, f'{int(altura):,}'.replace(",", "."),
            ha='center', va='bottom', fontsize=9, color="black")

for bar in bar2:
    altura = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, altura, f'{int(altura):,}'.replace(",", "."),
            ha='center', va='bottom', fontsize=9, color="black")


plt.tight_layout()
plt.show()

