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


# deletar produto
@app.route("/deletar/<codigo>",methods=["DELETE"])
def deletar(codigo):

    query("DELETE FROM produtos WHERE codigo=%s",(codigo,))

    return jsonify({"status":"sucesso"})


if __name__=="__main__":

    port=int(os.environ.get("PORT",5000))

    app.run(host="0.0.0.0",port=port)
