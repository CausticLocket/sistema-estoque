import json
import psycopg2
import os
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Conectar ao banco
def conectar_bd():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    return conn

def atualizar_entradas_antigas():

    conn = conectar_bd()
    cursor = conn.cursor()

    entradas = cursor.execute(
        "SELECT id, codigo, quantidade FROM historico_entradas"
    ).fetchall()

    for e in entradas:
        entrada_id = e["id"]
        codigo = e["codigo"]
        quantidade = e["quantidade"]

        prod = cursor.execute(
            "SELECT compra FROM produtos WHERE codigo=?",
            (codigo,)
        ).fetchone()

        valor_unit = prod["compra"] if prod else 0
        valor_total = valor_unit * quantidade

        cursor.execute("""
            UPDATE historico_entradas
            SET valor_unitario=?, valor_total=?
            WHERE id=?
        """, (valor_unit, valor_total, entrada_id))

    conn.commit()
    conn.close()


def criar_tabelas():
    conn = conectar_bd()
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS produtos (
        id SERIAL PRIMARY KEY,
        codigo TEXT UNIQUE,
        nome TEXT,
        compra REAL,
        venda REAL,
        estoque INTEGER,
        minimo INTEGER
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS historico_entradas (
        id SERIAL PRIMARY KEY,
        data TEXT,
        nf TEXT,
        codigo TEXT,
        nome TEXT,
        quantidade INTEGER,
        valor_unitario REAL DEFAULT 0,
        valor_total REAL DEFAULT 0
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS historico_vendas (
        id SERIAL PRIMARY KEY,
        data TEXT,
        cliente TEXT,
        cpf TEXT,
        endereco TEXT,
        pagamento TEXT,
        parcelas INTEGER,
        total REAL,
        itens_json TEXT,
        obs TEXT
    )''')

    conn.commit()
    conn.close()

criar_tabelas()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/produtos', methods=['GET'])
def listar_produtos():
    conn = conectar_bd()
    prods = [dict(row) for row in conn.execute('SELECT * FROM produtos').fetchall()]
    conn.close()
    return jsonify(prods)

@app.route('/cadastrar', methods=['POST'])
def cadastrar():
    d = request.json
    conn = conectar_bd()
    try:
        conn.execute('''INSERT INTO produtos (codigo, nome, compra, venda, estoque, minimo)
            VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(codigo) DO UPDATE SET
            nome=excluded.nome, compra=excluded.compra, venda=excluded.venda,
            estoque=excluded.estoque, minimo=excluded.minimo''',
            (d['codigo'], d['nome'], float(d['compra']), float(d['venda']), int(d['estoque']), int(d['minimo'])))
        conn.commit()
        return jsonify({"status": "sucesso"})
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500
    finally:
        conn.close()

@app.route('/registrar-entrada', methods=['POST'])
def registrar_entrada():

    d = request.json
    conn = conectar_bd()

    try:
        data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # buscar preço de compra do produto
        prod = conn.execute("""
            SELECT compra
            FROM produtos
            WHERE codigo = ?
        """,(d["codigo"],)).fetchone()

        valor_unit = prod["compra"] if prod else 0
        valor_total = valor_unit * int(d["quantidade"])

        # atualizar estoque
        conn.execute(
            'UPDATE produtos SET estoque = estoque + ? WHERE codigo = ?',
            (d['quantidade'], d['codigo'])
        )

        # salvar entrada
        conn.execute("""
            INSERT INTO historico_entradas
            (data, nf, codigo, nome, quantidade, valor_unitario, valor_total)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            data_atual,
            d['nf'],
            d['codigo'],
            d['nome'],
            d['quantidade'],
            valor_unit,
            valor_total
        ))

        conn.commit()

        return jsonify({"status":"sucesso"})

    finally:
        conn.close()

@app.route('/vender', methods=['POST'])
def vender():
    d = request.json
    conn = conectar_bd()
    try:
        # horário do Brasil
        from datetime import datetime
        from zoneinfo import ZoneInfo
        data_atual = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")

        # baixa estoque
        for item in d['itens']:
            conn.execute(
                'UPDATE produtos SET estoque = estoque - ? WHERE codigo = ?',
                (item['qtd'], item['codigo'])
            )

        # salva venda
        conn.execute('''
            INSERT INTO historico_vendas
            (data, cliente, total, itens_json, cpf, endereco, pagamento, parcelas, obs)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data_atual,
            d.get('cliente', 'Consumidor'),
            d['total'],
            json.dumps(d['itens']),
            d.get('cpf', ''),
            d.get('endereco', ''),
            d.get('pagamento', 'Dinheiro'),
            d.get('parcelas', 1),
            d.get('obs', '')
        ))

        conn.commit()
        return jsonify({"status": "sucesso"})

    except Exception as e:
        print("ERRO AO VENDER:", e)  # <<< IMPORTANTE
        return jsonify({"erro": str(e)}), 500

    finally:
        conn.close()



@app.route('/historico-entradas', methods=['GET'])
def hist_entradas():
    conn = conectar_bd()
    rows = conn.execute('SELECT * FROM historico_entradas ORDER BY id DESC').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/historico-vendas')
def historico_vendas():
    pagina = int(request.args.get("pagina", 1))
    limite = int(request.args.get("limite", 10))
    offset = (pagina - 1) * limite

    conn = conectar_bd()
    cursor = conn.cursor()

    # total de registros
    total = cursor.execute(
        "SELECT COUNT(*) FROM historico_vendas"
    ).fetchone()[0]

    rows = cursor.execute("""
        SELECT *
        FROM historico_vendas
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (limite, offset)).fetchall()

    conn.close()

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
            "total": r["total"],
            "itens": itens,
            "cpf": r["cpf"],
            "endereco": r["endereco"],
            "pagamento": r["pagamento"],
            "parcelas": r["parcelas"],
            "obs": r["obs"]
        })

    return jsonify({
        "vendas": vendas,
        "total": total,
        "pagina": pagina,
        "limite": limite
    })

@app.route('/editar/<id>', methods=['PUT'])
def editar(id):
    d = request.json
    conn = conectar_bd()
    try:
        conn.execute('UPDATE produtos SET nome=?, compra=?, venda=?, estoque=?, minimo=? WHERE codigo=?',
                     (d['nome'], d['compra'], d['venda'], d['estoque'], d['minimo'], id))
        conn.commit()
        return jsonify({"status": "sucesso"})
    finally:
        conn.close()

@app.route("/cancelar-venda/<int:venda_id>", methods=["DELETE"])
def cancelar_venda(venda_id):

    conn = conectar_bd()
    cursor = conn.cursor()

    # buscar venda
    cursor.execute("""
        SELECT itens_json
        FROM historico_vendas
        WHERE id = ?
    """, (venda_id,))

    venda = cursor.fetchone()

    if not venda:
        conn.close()
        return jsonify({"erro": "Venda não encontrada"}), 404

    # carregar itens do JSON
    try:
        itens = json.loads(venda["itens_json"]) if venda["itens_json"] else []
    except:
        itens = []

    # devolver estoque
    for item in itens:
        cursor.execute("""
            UPDATE produtos
            SET estoque = estoque + ?
            WHERE codigo = ?
        """, (item["qtd"], item["codigo"]))

    # deletar venda
    cursor.execute("""
        DELETE FROM historico_vendas
        WHERE id = ?
    """, (venda_id,))

    conn.commit()
    conn.close()

    return jsonify({"status": "cancelada"})

@app.route('/deletar/<id>', methods=['DELETE'])
def deletar(id):
    conn = conectar_bd()
    conn.execute('DELETE FROM produtos WHERE codigo = ?', (id,))
    conn.commit()
    conn.close()
    return jsonify({"status": "sucesso"})

@app.route("/vendas-mes")
def vendas_mes():
    conn = conectar_bd()

    mes = datetime.now().strftime("%Y-%m")

    rows = conn.execute("""
        SELECT id, data, cliente, total, itens_json, cpf, endereco, pagamento, parcelas, obs
        FROM historico_vendas
        WHERE strftime('%Y-%m', datetime(data)) = ?
        ORDER BY id DESC
    """, (mes,)).fetchall()

    conn.close()

    lista = []

    for r in rows:
        try:
            itens = json.loads(r["itens_json"]) if r["itens_json"] else []
        except:
            itens = []

        lista.append({
            "id": r["id"],
            "data": r["data"],
            "cliente": r["cliente"],
            "total": r["total"],
            "itens": itens,
            "cpf": r["cpf"],
            "endereco": r["endereco"],
            "pagamento": r["pagamento"],
            "parcelas": r["parcelas"],
            "obs": r["obs"]
        })

    return jsonify(lista)

@app.route("/faturamento-mensal")
def faturamento_mensal():
    conn = conectar_bd()   # usa o caminho correto
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            substr(data, 1, 7) as mes,
            SUM(total) as total
        FROM historico_vendas
        GROUP BY mes
        ORDER BY mes
    """)

    dados = cursor.fetchall()
    conn.close()

    return jsonify([
        {"mes": d["mes"], "total": d["total"] or 0}
        for d in dados
    ])

@app.route("/graficos")
def pagina_graficos():
    return render_template("graficos.html")

@app.route("/kpis-dashboard")
def kpis_dashboard():

    conn = conectar_bd()
    cursor = conn.cursor()

    mes_atual = datetime.now().strftime("%Y-%m")

    # 1️⃣ FATURAMENTO MENSAL
    faturamento = cursor.execute("""
        SELECT SUM(total)
        FROM historico_vendas
        WHERE substr(data,1,7)=?
    """, (mes_atual,)).fetchone()[0] or 0

    # 2️⃣ TOTAL DE VENDAS
    total_vendas = cursor.execute("""
        SELECT COUNT(*)
        FROM historico_vendas
        WHERE substr(data,1,7)=?
    """, (mes_atual,)).fetchone()[0] or 0

    # 3️⃣ TICKET MÉDIO
    ticket_medio = round(faturamento / total_vendas, 2) if total_vendas > 0 else 0

    # 4️⃣ MARGEM MÉDIA
    vendas = cursor.execute("""
        SELECT itens_json
        FROM historico_vendas
        WHERE substr(data,1,7)=?
    """, (mes_atual,)).fetchall()

    margem_total = 0
    qtd_total = 0

    for v in vendas:
        itens = json.loads(v["itens_json"]) if v["itens_json"] else []
        for item in itens:
            codigo = item["codigo"]
            qtd = item["qtd"]

            prod = cursor.execute("""
                SELECT compra, venda
                FROM produtos
                WHERE codigo=?
            """, (codigo,)).fetchone()

            if prod and prod["venda"] > 0:
                margem_item = (prod["venda"] - prod["compra"]) / prod["venda"] * 100
                margem_total += margem_item * qtd  # pondera pela quantidade vendida
                qtd_total += qtd

    margem_media = round(margem_total / qtd_total, 2) if qtd_total > 0 else 0

    conn.close()

    return jsonify({
        "faturamento": round(faturamento, 2),
        "total_vendas": total_vendas,
        "ticket_medio": ticket_medio,
        "margem_media": margem_media
    })

@app.route("/produtos-mais-vendidos")
def produtos_mais_vendidos():

    conn = conectar_bd()
    rows = conn.execute("SELECT itens_json FROM historico_vendas").fetchall()
    conn.close()

    ranking = {}

    for r in rows:
        itens = json.loads(r["itens_json"]) if r["itens_json"] else []
        for item in itens:
            codigo = item["codigo"]
            qtd = item["qtd"]
            ranking[codigo] = ranking.get(codigo,0) + qtd

    dados = sorted(ranking.items(), key=lambda x:x[1], reverse=True)

    return jsonify({
        "labels":[d[0] for d in dados],
        "valores":[d[1] for d in dados]
    })

@app.route("/formas-pagamento")
def formas_pagamento():

    conn = conectar_bd()
    rows = conn.execute("""
        SELECT pagamento, COUNT(*) as total
        FROM historico_vendas
        GROUP BY pagamento
    """).fetchall()
    conn.close()

    return jsonify({
        "labels":[r["pagamento"] for r in rows],
        "valores":[r["total"] for r in rows]
    })

@app.route("/comparativo-mensal")
def comparativo_mensal():
    conn = conectar_bd()
    cursor = conn.cursor()

    ano = datetime.now().year
    meses = [f"{ano}-{m:02d}" for m in range(1, 13)]
    labels = [datetime.strptime(m, "%Y-%m").strftime("%b") for m in meses]

    vendas_db = {m: 0 for m in meses}
    compras_db = {m: 0 for m in meses}

    # Vendas por mês
    for row in cursor.execute("SELECT substr(data,1,7) as mes, SUM(total) as vendas FROM historico_vendas GROUP BY mes"):
        vendas_db[row["mes"]] = row["vendas"] or 0

    # Compras por mês
    for row in cursor.execute("SELECT substr(data,1,7) as mes, SUM(valor_total) as compras FROM historico_entradas GROUP BY mes"):
        compras_db[row["mes"]] = row["compras"] or 0

    conn.close()

    return jsonify({
        "labels": labels,
        "vendas": [vendas_db[m] for m in meses],
        "compras": [compras_db[m] for m in meses]
    })


@app.route("/top-produtos")
def top_produtos():

    conn = conectar_bd()
    rows = conn.execute("SELECT itens_json FROM historico_vendas").fetchall()
    conn.close()

    ranking = {}

    for r in rows:
        itens = json.loads(r["itens_json"]) if r["itens_json"] else []
        for item in itens:
            codigo = item["codigo"]
            qtd = item["qtd"]
            ranking[codigo] = ranking.get(codigo, 0) + qtd

    top = sorted(ranking.items(), key=lambda x: x[1], reverse=True)[:5]

    return jsonify({
        "labels": [t[0] for t in top],
        "valores": [t[1] for t in top]
    })

@app.route("/compras-vs-vendas")
def compras_vs_vendas():
    conn = conectar_bd()
    cursor = conn.cursor()

    # vendas por mês
    cursor.execute("""
        SELECT substr(data,1,7) as mes, SUM(total) as vendas
        FROM historico_vendas
        GROUP BY mes
    """)
    vendas_db = {row["mes"]: row["vendas"] for row in cursor.fetchall()}

    # compras por mês
    cursor.execute("""
        SELECT substr(data,1,7) as mes,
               SUM(COALESCE(valor_total, quantidade * p.compra)) as compras
        FROM historico_entradas e
        JOIN produtos p ON p.codigo = e.codigo
        GROUP BY mes
    """)
    compras_db = {row["mes"]: row["compras"] for row in cursor.fetchall()}

    conn.close()

    # 12 meses do ano atual
    ano = datetime.now().year
    labels = []
    vendas = []
    compras = []

    for mes in range(1,13):
        chave = f"{ano}-{mes:02d}"
        labels.append(datetime.strptime(chave,"%Y-%m").strftime("%b"))
        vendas.append(vendas_db.get(chave,0))
        compras.append(compras_db.get(chave,0))

    return jsonify({
        "labels": labels,
        "vendas": vendas,
        "compras": compras
    })

@app.route("/debug-tabelas")
def debug_tabelas():
    conn = sqlite3.connect("estoque.db")
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tabelas = cursor.fetchall()

    conn.close()
    return jsonify(tabelas)

@app.route("/debug-colunas")
def debug_colunas():
    conn = sqlite3.connect("estoque.db")
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(historico_vendas)")
    vendas = cursor.fetchall()

    cursor.execute("PRAGMA table_info(historico_entradas)")
    entradas = cursor.fetchall()

    conn.close()

    return jsonify({
        "historico_vendas": vendas,
        "historico_entradas": entradas
    })

@app.route("/debug-entradas")
def debug_entradas():

    conn = conectar_bd()

    rows = conn.execute("""
        SELECT
            data,
            codigo,
            nome,
            quantidade,
            valor_unitario,
            valor_total
        FROM historico_entradas
        ORDER BY data DESC
    """).fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/lucro-mensal")
def lucro_mensal():
    conn = conectar_bd()
    cursor = conn.cursor()

    ano = datetime.now().year
    meses = [f"{ano}-{m:02d}" for m in range(1, 13)]
    lucro_db = {m: 0 for m in meses}

    vendas = cursor.execute("SELECT data, itens_json FROM historico_vendas").fetchall()

    for v in vendas:
        mes = v["data"][:7]
        if not v["itens_json"]:
            continue
        itens = json.loads(v["itens_json"])
        for item in itens:
            prod = cursor.execute("SELECT compra, venda FROM produtos WHERE codigo=?", (item["codigo"],)).fetchone()
            if prod:
                lucro_db[mes] += (prod["venda"] - prod["compra"]) * item["qtd"]

    conn.close()

    labels = [datetime.strptime(m, "%Y-%m").strftime("%b") for m in meses]
    valores = [lucro_db[m] for m in meses]

    return jsonify({
        "labels": labels,
        "valores": valores
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
