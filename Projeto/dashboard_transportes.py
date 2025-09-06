import psycopg2
import polars as pl
import io
import csv
import matplotlib.pyplot as plt

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

dim_transporte = carregar_tabela_postgres("dim_transporte")
fato_transacao = carregar_tabela_postgres("fato_transacao")

dados_transportes = fato_transacao.join(
    dim_transporte,
    left_on="sk_transporte",
    right_on="sk_transporte"
)

transporte_agrupado = dados_transportes.group_by("descricao_transporte").agg(
    pl.len().alias("quantidade")
).sort("quantidade", descending=True)

# Converter para pandas
df_pizza = transporte_agrupado.to_pandas()
def formatar_label(pct, all_vals):
    valor_absoluto = int(round(pct / 100 * sum(all_vals)))
    return f'{pct:.1f}%\n({valor_absoluto})'

# Gráfico de Pizza
fig, ax = plt.subplots(figsize=(8, 8))
ax.pie(
    df_pizza["quantidade"],
    labels=df_pizza["descricao_transporte"],
    autopct=lambda pct: formatar_label(pct, df_pizza["quantidade"]),
    startangle=140
)
ax.set_title("Distribuição dos Meios de Transporte nas Transações", fontsize=14)

plt.tight_layout()
plt.show()