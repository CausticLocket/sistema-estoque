import json
import psycopg2
import os
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

from psycopg2 import pool

db_pool = pool.SimpleConnectionPool(
    1,
    10,
    os.environ["DATABASE_URL"]
)

def conectar_bd():
    return db_pool.getconn()

def query(sql, params=None, fetch=False):

    conn = conectar_bd()
    cursor = conn.cursor()

    cursor.execute(sql, params or ())

    if fetch:
        colunas = [desc[0] for desc in cursor.description]
        dados = [dict(zip(colunas,row)) for row in cursor.fetchall()]
    else:
        dados = None

    conn.commit()

    cursor.close()
    db_pool.putconn(conn)

    return dados


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
        valor_unitario NUMERIC,
        valor_total NUMERIC
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


@app.route("/produtos")
def produtos():

    rows = query("SELECT * FROM produtos ORDER BY nome", fetch=True)

    return jsonify(rows)


@app.route("/cadastrar", methods=["POST"])
def cadastrar():

    d = request.json

    try:

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
            d.get("codigo"),
            d.get("nome"),
            d.get("compra"),
            d.get("venda"),
            d.get("estoque"),
            d.get("minimo")
        ))

        return jsonify({"status":"sucesso"})

    except Exception as e:

        return jsonify({"status":"erro","mensagem":str(e)}),500


@app.route("/vender",methods=["POST"])
def vender():

    d=request.json

    data=datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for item in d["itens"]:

        query(
            "UPDATE produtos SET estoque=estoque-%s WHERE codigo=%s",
            (item["qtd"],item["codigo"])
        )

    query("""
    INSERT INTO historico_vendas
    (data,cliente,total,itens_json,cpf,endereco,pagamento,parcelas,obs)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """,(
        data,
        d.get("cliente","Consumidor"),
        d["total"],
        json.dumps(d["itens"]),
        d.get("cpf",""),
        d.get("endereco",""),
        d.get("pagamento","Dinheiro"),
        d.get("parcelas",1),
        d.get("obs","")
    ))

    return jsonify({"status":"sucesso"})


@app.route("/historico-vendas")
def historico_vendas():

    rows = query(
        "SELECT * FROM historico_vendas ORDER BY id DESC",
        fetch=True
    )

    vendas = []

    for r in rows:

        try:
            itens = json.loads(r["itens_json"]) if r["itens_json"] else []
        except:
            itens = []

        vendas.append({
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

    return jsonify(vendas)

@app.route("/historico-entradas")
def historico_entradas():

    rows=query(
        "SELECT * FROM historico_entradas ORDER BY id DESC",
        fetch=True
    )

    return jsonify(rows)

@app.route("/registrar-entrada", methods=["POST"])
def registrar_entrada():

    d = request.json

    conn = conectar_bd()
    cursor = conn.cursor()

    try:

        data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # buscar preço de compra
        cursor.execute(
            "SELECT compra FROM produtos WHERE codigo=%s",
            (d["codigo"],)
        )

        prod = cursor.fetchone()

        valor_unit = prod[0] if prod else 0
        valor_total = valor_unit * int(d["quantidade"])

        # atualizar estoque
        cursor.execute(
            "UPDATE produtos SET estoque = estoque + %s WHERE codigo = %s",
            (d["quantidade"], d["codigo"])
        )

        # salvar histórico
        cursor.execute("""
            INSERT INTO historico_entradas
            (data, nf, codigo, nome, quantidade, valor_unitario, valor_total)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            data_atual,
            d["nf"],
            d["codigo"],
            d["nome"],
            d["quantidade"],
            valor_unit,
            valor_total
        ))

        conn.commit()

        return jsonify({"status":"sucesso"})

    except Exception as e:

        return jsonify({"status":"erro","mensagem":str(e)}),500

    finally:

        cursor.close()
        conn.close()


@app.route("/deletar/<codigo>",methods=["DELETE"])
def deletar(codigo):

    query("DELETE FROM produtos WHERE codigo=%s",(codigo,))

    return jsonify({"status":"sucesso"})


@app.route("/faturamento-mensal")
def faturamento():

    rows=query("""
    SELECT LEFT(data,7) mes,
    SUM(total) total
    FROM historico_vendas
    GROUP BY mes
    ORDER BY mes
    """,fetch=True)

    return jsonify(rows)


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


if __name__=="__main__":

    port=int(os.environ.get("PORT",5000))

    app.run(host="0.0.0.0",port=port)
