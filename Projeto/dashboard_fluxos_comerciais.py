import psycopg2
import polars as pl
import io
import csv
import seaborn as sns
import matplotlib.pyplot as plt

import plotly.graph_objects as go
import pandas as pd

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
fato_transacao = carregar_tabela_postgres("fato_transacao")
df_sankey = (
    fato_transacao
    .join(dim_pais, left_on="sk_pais_origem", right_on="sk_pais")
    .join(dim_pais, left_on="sk_pais_destino", right_on="sk_pais", suffix="_destino")
    .group_by(["nome_pais", "nome_pais_destino"])
    .agg(pl.col("valor_monetario").sum().alias("valor_total"))
    .rename({"nome_pais": "nome_pais_origem"})  # renomeia para facilitar
    .filter(pl.col("valor_total") > 0)
)

print(df_sankey)

# --- Converte para Pandas ---
df_sankey = df_sankey.to_pandas()

# --- Mapeia nomes dos países para índices (necessário para o Sankey) ---
all_nodes = pd.unique(df_sankey[["nome_pais_origem", "nome_pais_destino"]].values.ravel())
node_map = {name: idx for idx, name in enumerate(all_nodes)}

df_sankey["source"] = df_sankey["nome_pais_origem"].map(node_map)
df_sankey["target"] = df_sankey["nome_pais_destino"].map(node_map)

# --- Criação do Gráfico de Sankey ---
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=15,
        thickness=10,
        line=dict(color="black", width=0.5),
        label=list(node_map.keys())
    ),
    link=dict(
        source=df_sankey["source"],
        target=df_sankey["target"],
        value=df_sankey["valor_total"]
    )
)])

fig.update_layout(title_text="Fluxo de Comércio Internacional entre Países", font_size=10)
#fig.show()
fig.write_html("grafico_fluxo_comercial.html", auto_open=True)
'''df_heatmap = fato_transacao.join(dim_pais, left_on="sk_pais_origem", right_on="sk_pais") \
    .rename({"nome_pais": "pais_origem"}) \
    .join(dim_pais, left_on="sk_pais_destino", right_on="sk_pais") \
    .rename({"nome_pais": "pais_destino"}) \
    .group_by(["pais_origem", "pais_destino"]) \
    .agg(pl.sum("valor_monetario").alias("valor_total")) \
    .to_pandas()

# Pivotar a tabela para formar a matriz do heatmap
heatmap_data = df_heatmap.pivot(index="pais_origem", columns="pais_destino", values="valor_total").fillna(0)

# Plotar o heatmap
plt.figure(figsize=(14, 10))
sns.heatmap(heatmap_data, cmap="YlGnBu", linewidths=0.5)
plt.title("Parcerias Comerciais: Exportador x Importador (US$)")
plt.xlabel("País Destino (Importador)")
plt.ylabel("País Origem (Exportador)")
plt.tight_layout()
plt.show()'''