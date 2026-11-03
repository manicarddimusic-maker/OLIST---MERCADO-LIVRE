"""Painel Olist (ERP / Tiny API v2) para o e-commerce do Mercado Livre.

O token da Olist fica SÓ em variável de ambiente (OLIST_TOKEN) - nunca vai pra página.
O painel é protegido por senha (DASH_PASSWORD).
"""
import datetime as dt
import hmac
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

BASE = "https://api.tiny.com.br/api2/"
TOKEN = os.environ.get("OLIST_TOKEN", "")
PASSWORD = os.environ.get("DASH_PASSWORD", "")
START_DATE = os.environ.get("START_DATE", "01/01/2026")  # início do histórico lido
TTL = int(os.environ.get("CACHE_SECONDS", "300"))
FINAL_STATES = {"entregue", "cancelado", "não entregue", "nao entregue"}

app = Flask(__name__, static_folder="static")
_cache: dict = {}
_lock = threading.Lock()
_gate = threading.Semaphore(3)  # no máximo 3 chamadas simultâneas à Olist


class OlistError(Exception):
    pass


def tiny(endpoint: str, ttl: int = TTL, **params) -> dict:
    """Chama a API v2 da Olist com cache e tentativa de novo se estourar o limite."""
    key = (endpoint, tuple(sorted(params.items())))
    now = time.time()
    with _lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    data = None
    for attempt in range(5):
        with _gate:
            r = requests.post(BASE + endpoint, data={"token": TOKEN, "formato": "json", **params}, timeout=30)
        ret = r.json().get("retorno", {})
        if ret.get("status") == "OK":
            data = ret
            break
        code = str(ret.get("codigo_erro", ""))
        msg = " ".join(e.get("erro", "") for e in ret.get("erros", []))
        if code == "20":  # "A consulta não retornou registros"
            data = {}
            break
        if re.search(r"excedido|bloquead|limite", msg, re.I):
            time.sleep(4 * (attempt + 1))
            continue
        raise OlistError(f"{endpoint}: {msg or ret}")
    if data is None:
        raise OlistError(f"{endpoint}: limite de acessos da Olist excedido, tente em 1 minuto")
    with _lock:
        _cache[key] = (now, data)
    return data


def paged(endpoint: str, listkey: str, **params) -> list:
    out, page, total = [], 1, 1
    while page <= total:
        d = tiny(endpoint, pagina=page, **params)
        total = int(d.get("numero_paginas", 1) or 1)
        out += d.get(listkey, [])
        page += 1
    return out


def iso(br: str) -> str:
    """dd/mm/aaaa -> aaaa-mm-dd"""
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", br or "")
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else ""


def num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def load_orders(today: str) -> list:
    rows = paged("pedidos.pesquisa.php", "pedidos", dataInicial=START_DATE, dataFinal=today)
    heads = [r["pedido"] for r in rows]

    def detail(h):
        ttl = 86400 if norm(h["situacao"]) in FINAL_STATES else TTL
        return h, tiny("pedido.obter.php", ttl=ttl, id=h["id"]).get("pedido", {})

    with ThreadPoolExecutor(max_workers=3) as ex:
        results = list(ex.map(detail, heads))

    orders = []
    for h, o in results:
        itens = [
            {
                "codigo": i["item"].get("codigo", ""),
                "descricao": (i["item"].get("descricao") or "").strip(),
                "qtd": num(i["item"].get("quantidade")),
                "unit": num(i["item"].get("valor_unitario")),
            }
            for i in o.get("itens", [])
        ]
        pagos = [num(p["pagamento_integrado"].get("valor")) for p in o.get("pagamentos_integrados", []) or []]
        cli = o.get("cliente", {}) or {}
        eco = o.get("ecommerce", {}) or {}
        orders.append(
            {
                "id": h["id"],
                "numero": h["numero"],
                "ecommerce": h.get("numero_ecommerce", ""),
                "data": iso(h["data_pedido"]),
                "situacao": h["situacao"],
                # só nome/cidade/UF - CPF e endereço nunca saem do servidor
                "cliente": h.get("nome", ""),
                "cidade": cli.get("cidade", ""),
                "uf": cli.get("uf", ""),
                "canal": eco.get("nomeEcommerce") or "—",
                "itens": itens,
                "total_produtos": num(o.get("total_produtos")),
                "frete": num(o.get("valor_frete")),
                "desconto": num(o.get("valor_desconto")),
                "total_pedido": num(o.get("total_pedido")) or num(h.get("valor")),
                "recebido": round(sum(pagos), 2),
                "tem_pagamento": bool(pagos),
                "nf": o.get("id_nota_fiscal", ""),
            }
        )
    orders.sort(key=lambda x: (x["data"], int(x["numero"] or 0)))
    return orders


def load_products(orders: list) -> list:
    rows = [r["produto"] for r in paged("produtos.pesquisa.php", "produtos")]
    rows = [p for p in rows if p.get("situacao", "A") == "A"]

    def stock(p):
        d = tiny("produto.obter.estoque.php", id=p["id"]).get("produto", {})
        return p, d

    with ThreadPoolExecutor(max_workers=3) as ex:
        results = list(ex.map(stock, rows))

    last_price = {}
    for o in orders:  # preço de venda mais recente, usado quando o cadastro está com preço 0
        for i in o["itens"]:
            last_price[i["codigo"]] = i["unit"]

    prods = []
    for p, d in results:
        saldo, reservado = num(d.get("saldo")), num(d.get("saldoReservado"))
        preco = num(p.get("preco")) or last_price.get(p.get("codigo"), 0.0)
        prods.append(
            {
                "id": p["id"],
                "codigo": p.get("codigo", ""),
                "nome": (p.get("nome") or "").strip(),
                "saldo": saldo,
                "reservado": reservado,
                "disponivel": saldo - reservado,
                "preco": preco,
                "custo": num(p.get("preco_custo")),
                "criado": iso(p.get("data_criacao", "")),
            }
        )
    prods.sort(key=lambda x: x["nome"])
    return prods


def load_payables(today: str, orders: list) -> list:
    rows = paged("contas.pagar.pesquisa.php", "contas", data_ini_emissao=START_DATE, data_fim_emissao=today)
    by_name = {}
    for o in orders:
        by_name.setdefault(norm(o["cliente"]), []).append(o)
    out = []
    for r in rows:
        c = r["conta"]
        hist = c.get("historico", "")
        m = re.search(r"NF\s*0*(\d+),\s*(.+)$", hist)
        pedido = None
        if m:
            cands = by_name.get(norm(m.group(2)), [])
            if len(cands) == 1:
                pedido = cands[0]["numero"]
            elif cands:  # mais de um pedido do mesmo cliente: pega o de data mais próxima da emissão
                emis = iso(c.get("data_emissao", ""))
                pedido = min(cands, key=lambda o: abs((dt.date.fromisoformat(o["data"]) - dt.date.fromisoformat(emis)).days))["numero"] if emis else None
        out.append(
            {
                "id": c["id"],
                "fornecedor": c.get("nome_cliente", ""),
                "historico": hist,
                "doc": c.get("numero_doc", ""),
                "vencimento": iso(c.get("data_vencimento", "")),
                "emissao": iso(c.get("data_emissao", "")),
                "valor": num(c.get("valor")),
                "saldo": num(c.get("saldo")),
                "situacao": (c.get("situacao") or "").lower(),
                "pedido": pedido,
            }
        )
    out.sort(key=lambda x: x["vencimento"])
    return out


def load_receivables(today: str) -> list:
    rows = paged("contas.receber.pesquisa.php", "contas", data_ini_emissao=START_DATE, data_fim_emissao=today)
    return [
        {
            "id": r["conta"]["id"],
            "cliente": r["conta"].get("nome_cliente", ""),
            "historico": r["conta"].get("historico", ""),
            "vencimento": iso(r["conta"].get("data_vencimento", "")),
            "valor": num(r["conta"].get("valor")),
            "saldo": num(r["conta"].get("saldo")),
            "situacao": (r["conta"].get("situacao") or "").lower(),
        }
        for r in rows
    ]


# ---------- proteção por senha ----------
def _authorized() -> bool:
    a = request.authorization
    return bool(a and PASSWORD and hmac.compare_digest(a.password or "", PASSWORD))


@app.before_request
def gate():
    if request.path == "/health":
        return None
    if not PASSWORD or not TOKEN:
        return Response("Configure OLIST_TOKEN e DASH_PASSWORD nas variáveis do servidor.", 503)
    if not _authorized():
        return Response("Senha necessária.", 401, {"WWW-Authenticate": 'Basic realm="Painel Olist"'})


@app.after_request
def no_store(resp):
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["X-Robots-Tag"] = "noindex"
    return resp


@app.route("/health")
def health():
    return "ok"


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/data")
def api_data():
    if request.args.get("refresh") == "1":
        with _lock:
            _cache.clear()
    today = (dt.datetime.utcnow() - dt.timedelta(hours=3)).strftime("%d/%m/%Y")  # horário de Brasília
    try:
        info = tiny("info.php", ttl=3600).get("conta", {})
        orders = load_orders(today)
        products = load_products(orders)
        payables = load_payables(today, orders)
        receivables = load_receivables(today)
    except (OlistError, requests.RequestException) as e:
        return jsonify({"erro": str(e)}), 502
    return jsonify(
        {
            "gerado_em": (dt.datetime.utcnow() - dt.timedelta(hours=3)).isoformat(timespec="seconds"),
            "conta": info.get("razao_social", ""),
            "pedidos": orders,
            "produtos": products,
            "pagar": payables,
            "receber": receivables,
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
