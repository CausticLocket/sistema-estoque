import json
import psycopg2
import os
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


# conexão banco
def conectar_bd():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# utilitário query
def query(sql, params=None, fetch=False):

    conn = conectar_bd()
    cursor = conn.cursor()

    cursor.execute(sql, params or ())

    if fetch:
        colunas = [d[0] for d in cursor.description]
        dados = [dict(zip(colunas,row)) for row in cursor.fetchall()]
    else:
        dados = None

    conn.commit()
    cursor.close()
    conn.close()

    return dados


# criar tabelas
def criar_tabelas():

    query("""
    CREATE TABLE IF NOT EXISTS produtos(
        id SERIAL PRIMARY KEY,
        codigo TEXT UNIQUE,
        nome TEXT,
        compra NUMERIC,
        venda NUMERIC,
        estoque INTEGER,
        minimo INTEGER
    )
    """)

    query("""
    CREATE TABLE IF NOT EXISTS historico_entradas(
        id SERIAL PRIMARY KEY,
        data TEXT,
        nf TEXT,
        codigo TEXT,
        nome TEXT,
        quantidade INTEGER,
        valor_unitario NUMERIC DEFAULT 0,
        valor_total NUMERIC DEFAULT 0
    )
    """)

    query("""
    CREATE TABLE IF NOT EXISTS historico_vendas(
        id SERIAL PRIMARY KEY,
        data TEXT,
        cliente TEXT,
        cpf TEXT,
        endereco TEXT,
        pagamento TEXT,
        parcelas INTEGER,
        total NUMERIC,
        itens_json TEXT,
        obs TEXT
    )
    """)

criar_tabelas()


@app.route("/")
def index():
    return render_template("index.html")


# produtos
@app.route("/produtos")
def produtos():

    rows = query("SELECT * FROM produtos ORDER BY nome", fetch=True)
    return jsonify(rows)


# cadastrar produto
@app.route("/cadastrar", methods=["POST"])
def cadastrar():

    d = request.json

    query("""
    INSERT INTO produtos(codigo,nome,compra,venda,estoque,minimo)
    VALUES(%s,%s,%s,%s,%s,%s)
    ON CONFLICT(codigo) DO UPDATE SET
    nome=EXCLUDED.nome,
    compra=EXCLUDED.compra,
    venda=EXCLUDED.venda,
    estoque=EXCLUDED.estoque,
    minimo=EXCLUDED.minimo
    """,(
        d["codigo"],
        d["nome"],
        d["compra"],
        d["venda"],
        d["estoque"],
        d["minimo"]
    ))

    return jsonify({"status":"sucesso"})


# registrar entrada
@app.route("/registrar-entrada", methods=["POST"])
def registrar_entrada():

    d = request.json

    conn = conectar_bd()
    cursor = conn.cursor()

    data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(
        "SELECT compra FROM produtos WHERE codigo=%s",
        (d["codigo"],)
    )

    prod = cursor.fetchone()

    valor_unit = prod[0] if prod else 0
    valor_total = valor_unit * int(d["quantidade"])

    cursor.execute(
        "UPDATE produtos SET estoque=estoque+%s WHERE codigo=%s",
        (d["quantidade"],d["codigo"])
    )

    cursor.execute("""
    INSERT INTO historico_entradas
    (data,nf,codigo,nome,quantidade,valor_unitario,valor_total)
    VALUES(%s,%s,%s,%s,%s,%s,%s)
    """,(
        data_atual,
        d["nf"],
        d["codigo"],
        d["nome"],
        d["quantidade"],
        valor_unit,
        valor_total
    ))

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"status":"sucesso"})


# vender
@app.route("/vender", methods=["POST"])
def vender():

    d = request.json

    conn = conectar_bd()
    cursor = conn.cursor()

    data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for item in d["itens"]:

        cursor.execute(
            "UPDATE produtos SET estoque=estoque-%s WHERE codigo=%s",
            (item["qtd"],item["codigo"])
        )

    cursor.execute("""
    INSERT INTO historico_vendas
    (data,cliente,total,itens_json,cpf,endereco,pagamento,parcelas,obs)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """,(
        data_atual,
        d.get("cliente","Consumidor"),
        d["total"],
        json.dumps(d["itens"]),
        d.get("cpf",""),
        d.get("endereco",""),
        d.get("pagamento","Dinheiro"),
        d.get("parcelas",1),
        d.get("obs","")
    ))

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"status":"sucesso"})

@app.route('/historico-entradas', methods=['GET'])
def hist_entradas():

    conn = conectar_bd()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM historico_entradas
        ORDER BY id DESC
    """)

    rows = cursor.fetchall()

    colunas = [desc[0] for desc in cursor.description]

    dados = [dict(zip(colunas, row)) for row in rows]

    cursor.close()
    conn.close()

    return jsonify(dados)

# historico vendas paginado
@app.route("/historico-vendas")
def historico_vendas():

    pagina = int(request.args.get("pagina",1))
    limite = int(request.args.get("limite",10))

    offset = (pagina-1)*limite

    conn = conectar_bd()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM historico_vendas")
    total = cursor.fetchone()[0]

    cursor.execute("""
    SELECT *
    FROM historico_vendas
    ORDER BY id DESC
    LIMIT %s OFFSET %s
    """,(limite,offset))

    rows = cursor.fetchall()

    colunas=[d[0] for d in cursor.description]

    vendas=[]

    for r in rows:

        venda=dict(zip(colunas,r))

        try:
            itens=json.loads(venda["itens_json"]) if venda["itens_json"] else []
        except:
            itens=[]

        venda["itens"]=itens

        vendas.append(venda)

    cursor.close()
    conn.close()

    return jsonify({
        "vendas":vendas,
        "total":total,
        "pagina":pagina,
        "limite":limite
    })

@app.route("/vendas-mes")
def vendas_mes():

    conn = conectar_bd()
    cursor = conn.cursor()

    mes = datetime.now().strftime("%Y-%m")

    cursor.execute("""
        SELECT id, data, cliente, total, itens_json, cpf, endereco, pagamento, parcelas, obs
        FROM historico_vendas
        WHERE to_char(data::date,'YYYY-MM') = %s
        ORDER BY id DESC
    """, (mes,))

    rows = cursor.fetchall()

    colunas = [desc[0] for desc in cursor.description]

    lista = []

    for row in rows:

        r = dict(zip(colunas, row))

        try:
            itens = json.loads(r["itens_json"]) if r["itens_json"] else []
        except:
            itens = []

        lista.append({
            "id": r["id"],
            "data": r["data"],
            "cliente": r["cliente"],
            "total": float(r["total"]),
            "itens": itens,
            "cpf": r["cpf"],
            "endereco": r["endereco"],
            "pagamento": r["pagamento"],
            "parcelas": r["parcelas"],
            "obs": r["obs"]
        })

    cursor.close()
    conn.close()

    return jsonify(lista)

@app.route("/comparativo-mensal")
def comparativo_mensal():

    conn = conectar_bd()
    cursor = conn.cursor()

    ano = datetime.now().year

    meses = [f"{ano}-{m:02d}" for m in range(1,13)]
    labels = [datetime.strptime(m,"%Y-%m").strftime("%b") for m in meses]

    vendas_db = {m:0 for m in meses}
    compras_db = {m:0 for m in meses}

    # VENDAS
    cursor.execute("""
        SELECT to_char(data::date,'YYYY-MM') as mes, SUM(total)
        FROM historico_vendas
        GROUP BY mes
    """)

    for row in cursor.fetchall():
        mes = row[0]
        valor = float(row[1] or 0)

        if mes in vendas_db:
            vendas_db[mes] = valor

    # COMPRAS
    cursor.execute("""
        SELECT to_char(data::date,'YYYY-MM') as mes, SUM(valor_total)
        FROM historico_entradas
        GROUP BY mes
    """)

    for row in cursor.fetchall():
        mes = row[0]
        valor = float(row[1] or 0)

        if mes in compras_db:
            compras_db[mes] = valor

    cursor.close()
    conn.close()

    return jsonify({
        "labels": labels,
        "vendas": [vendas_db[m] for m in meses],
        "compras": [compras_db[m] for m in meses]
    })

@app.route("/kpis-dashboard")
def kpis_dashboard():

    conn = conectar_bd()
    cursor = conn.cursor()

    mes_atual = datetime.now().strftime("%Y-%m")

    # faturamento mensal
    cursor.execute("""
        SELECT COALESCE(SUM(total),0)
        FROM historico_vendas
        WHERE substr(data,1,7)=%s
    """, (mes_atual,))
    faturamento = cursor.fetchone()[0]

    # total de vendas
    cursor.execute("""
        SELECT COUNT(*)
        FROM historico_vendas
        WHERE substr(data,1,7)=%s
    """, (mes_atual,))
    total_vendas = cursor.fetchone()[0]

    ticket_medio = round(faturamento / total_vendas, 2) if total_vendas else 0

    # margem média
    cursor.execute("""
        SELECT itens_json
        FROM historico_vendas
        WHERE substr(data,1,7)=%s
    """, (mes_atual,))
    vendas = cursor.fetchall()

    margem_total = 0
    qtd_total = 0

    for v in vendas:

        itens = json.loads(v[0]) if v[0] else []

        for item in itens:

            codigo = item["codigo"]
            qtd = item["qtd"]

            cursor.execute("""
                SELECT compra, venda
                FROM produtos
                WHERE codigo=%s
            """, (codigo,))

            prod = cursor.fetchone()

            if prod and prod[1] > 0:

                compra = prod[0]
                venda = prod[1]

                margem_item = (venda - compra) / venda * 100
                margem_total += margem_item * qtd
                qtd_total += qtd

    margem_media = round(margem_total / qtd_total, 2) if qtd_total else 0

    cursor.close()
    conn.close()

    return jsonify({
        "faturamento": round(float(faturamento),2),
        "total_vendas": total_vendas,
        "ticket_medio": ticket_medio,
        "margem_media": margem_media
    })


@app.route("/editar/<codigo>", methods=["PUT"])
def editar(codigo):

    d = request.json

    query("""
    UPDATE produtos
    SET nome=%s, compra=%s, venda=%s, estoque=%s, minimo=%s
    WHERE codigo=%s
    """,(
        d["nome"],
        d["compra"],
        d["venda"],
        d["estoque"],
        d["minimo"],
        codigo
    ))

    return jsonify({"status":"sucesso"})
    
# cancelar venda
@app.route("/cancelar-venda/<int:venda_id>", methods=["DELETE"])
def cancelar_venda(venda_id):

    conn = conectar_bd()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT itens_json FROM historico_vendas WHERE id=%s",
        (venda_id,)
    )

    venda=cursor.fetchone()

    if not venda:
        return jsonify({"erro":"Venda não encontrada"}),404

    itens=json.loads(venda[0]) if venda[0] else []

    for item in itens:

        cursor.execute(
            "UPDATE produtos SET estoque=estoque+%s WHERE codigo=%s",
            (item["qtd"],item["codigo"])
        )

    cursor.execute(
        "DELETE FROM historico_vendas WHERE id=%s",
        (venda_id,)
    )

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"status":"cancelada"})

@app.route("/formas-pagamento")
def formas_pagamento():

    rows = query("""
    SELECT pagamento, COUNT(*) as total
    FROM historico_vendas
    GROUP BY pagamento
    """, fetch=True)

    return jsonify({
        "labels":[r["pagamento"] for r in rows],
        "valores":[r["total"] for r in rows]
    })

@app.route("/top-produtos")
def top_produtos():

    rows = query("SELECT itens_json FROM historico_vendas", fetch=True)

    ranking = {}

    for r in rows:

        itens = json.loads(r["itens_json"]) if r["itens_json"] else []

        for item in itens:

            cod = item["codigo"]
            ranking[cod] = ranking.get(cod,0) + item["qtd"]

    top = sorted(ranking.items(), key=lambda x:x[1], reverse=True)[:5]

    return jsonify({
        "labels":[t[0] for t in top],
        "valores":[t[1] for t in top]
    })

@app.route("/compras-vs-vendas")
def compras_vs_vendas():

    conn = conectar_bd()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT to_char(data::date,'YYYY-MM') mes, SUM(total)
    FROM historico_vendas
    GROUP BY mes
    """)

    vendas_db = {r[0]:float(r[1]) for r in cursor.fetchall()}

    cursor.execute("""
    SELECT to_char(data::date,'YYYY-MM') mes, SUM(valor_total)
    FROM historico_entradas
    GROUP BY mes
    """)

    compras_db = {r[0]:float(r[1]) for r in cursor.fetchall()}

    cursor.close()
    conn.close()

    ano = datetime.now().year

    labels=[]
    vendas=[]
    compras=[]

    for m in range(1,13):

        chave=f"{ano}-{m:02d}"

        labels.append(datetime.strptime(chave,"%Y-%m").strftime("%b"))

        vendas.append(vendas_db.get(chave,0))
        compras.append(compras_db.get(chave,0))

    return jsonify({
        "labels":labels,
        "vendas":vendas,
        "compras":compras
    })

# faturamento mensal
@app.route("/faturamento-mensal")
def faturamento():

    rows=query("""
    SELECT substr(data,1,7) mes,
    SUM(total) total
    FROM historico_vendas
    GROUP BY mes
    ORDER BY mes
    """,fetch=True)

    return jsonify(rows)


# produtos mais vendidos
@app.route("/produtos-mais-vendidos")
def mais_vendidos():

    rows=query("SELECT itens_json FROM historico_vendas",fetch=True)

    ranking={}

    for r in rows:

        itens=json.loads(r["itens_json"]) if r["itens_json"] else []

        for item in itens:

            cod=item["codigo"]

            ranking[cod]=ranking.get(cod,0)+item["qtd"]

    dados=sorted(ranking.items(),key=lambda x:x[1],reverse=True)

    return jsonify({
        "labels":[d[0] for d in dados],
        "valores":[d[1] for d in dados]
    })

@app.route("/lucro-mensal")
def lucro_mensal():

    conn = conectar_bd()
    cursor = conn.cursor()

    cursor.execute("SELECT data,itens_json FROM historico_vendas")

    vendas = cursor.fetchall()

    lucro_db = {}

    for v in vendas:

        data = v[0]
        itens_json = v[1]

        mes = data[:7]

        if mes not in lucro_db:
            lucro_db[mes] = 0

        itens = json.loads(itens_json) if itens_json else []

        for item in itens:

            cursor.execute("""
            SELECT compra,venda
            FROM produtos
            WHERE codigo=%s
            """,(item["codigo"],))

            prod = cursor.fetchone()

            if prod:

                compra = prod[0]
                venda = prod[1]

                lucro_db[mes] += (venda-compra) * item["qtd"]

    cursor.close()
    conn.close()

    meses = sorted(lucro_db.keys())

    labels=[datetime.strptime(m,"%Y-%m").strftime("%b") for m in meses]
    valores=[lucro_db[m] for m in meses]

    return jsonify({
        "labels":labels,
        "valores":valores
    })

# deletar produto
@app.route("/deletar/<codigo>",methods=["DELETE"])
def deletar(codigo):

    query("DELETE FROM produtos WHERE codigo=%s",(codigo,))

    return jsonify({"status":"sucesso"})

@app.route("/graficos")
def pagina_graficos():
    return render_template("graficos.html")

@app.route("/debug-entradas")
def debug_entradas():

    rows = query("""
    SELECT data,codigo,nome,quantidade,valor_unitario,valor_total
    FROM historico_entradas
    ORDER BY data DESC
    """, fetch=True)

    return jsonify(rows)

if __name__=="__main__":

    port=int(os.environ.get("PORT",5000))

    app.run(host="0.0.0.0",port=port)
