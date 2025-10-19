#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cosa fa in 3 passi:
  1) Legge il CSV.gz dell'estrattore (colonne: block_hash, tx_index, txid, vout_index, value_btc, opret_script_hex).
  2) Prima passata: prende TUTTI i block_hash unici e, per ognuno, chiede al nodo (getblockheader)
     time e height.
  3) Seconda passata: per ogni riga estrae il PAYLOAD dallo script OP_RETURN e prova 5 decodifiche
     (utf8, url_utf8, hex2_utf8, base64_utf8, base58_hex). Scrive una riga JSON (JSONL) con
     txid, vout, block_hash, block_time, block_height, hex (payload), le 5 decodifiche,
     decoded_any e first_success.
Ripartenza:
  - Se il JSONL esiste e ha righe, legge l’ULTIMA riga per ottenere last_key (txid:vout),
    apre l’output in append e scorre il CSV finché trova last_key; poi appende SOLO le righe nuove.
  - Se elimini il JSONL: riparte da zero e lo ricrea.
"""

# =============================
# SEZIONE 1 — Import e config
# =============================
import os, io, csv, gzip, json, argparse, re
from typing import Optional, Dict, Tuple
import sys
csv.field_size_limit(sys.maxsize)  # evita "field larger than field limit"

try:
    from bitcoinrpc.authproxy import AuthServiceProxy
except Exception:
    AuthServiceProxy = None

DEFAULT_IN  = "/var/lib/opreturns/op_returns_hex.csv.gz"
DEFAULT_OUT = "/var/lib/opreturns/op_returns_decoded.jsonl"
RPC_URL     = os.environ.get("RPC_URL", "http://bitcoinrpc:YOUR_PASS@127.0.0.1:8332")

# ======================================================
# SEZIONE 2 — Mini-utility per decodifiche (5 metodi)
# ======================================================
PRINTABLE = set(chr(c) for c in range(32,127)) | set("\n\r\t")
BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
HEX_RE = re.compile(r"^[0-9a-fA-F]+$")
PCT_RE = re.compile(r"%[0-9A-Fa-f]{2}")

def printable_ratio(s: str) -> float:
    if not s: return 0.0
    return sum(1 for ch in s if ch in PRINTABLE) / max(1, len(s))

def d_utf8(b: bytes) -> Optional[str]:
    try:
        s = b.decode('utf-8')
        return s if printable_ratio(s) >= 0.85 else None
    except Exception:
        return None

def d_url_utf8(b: bytes) -> Optional[str]:
    t = b.decode('latin-1', errors='ignore')
    if not PCT_RE.search(t):
        return None
    try:
        from urllib.parse import unquote_to_bytes
        raw = unquote_to_bytes(t)
        s = raw.decode('utf-8')
        return s if printable_ratio(s) >= 0.85 else None
    except Exception:
        return None

def d_hex2_utf8(b: bytes) -> Optional[str]:
    t = b.decode('latin-1', errors='ignore').strip()
    if len(t) % 2 != 0 or not HEX_RE.match(t):
        return None
    try:
        bb = bytes.fromhex(t)
        s = bb.decode('utf-8')
        return s if printable_ratio(s) >= 0.85 else None
    except Exception:
        return None

def d_base64_utf8(b: bytes) -> Optional[str]:
    import base64
    t = ''.join(ch for ch in b.decode('latin-1', errors='ignore') if not ch.isspace())
    if not t:
        return None
    ALPH = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=")
    if any(ch not in ALPH for ch in t):
        return None
    if (len(t) % 4) not in (0,2,3):
        return None
    try:
        pad = (-len(t)) % 4
        raw = base64.b64decode(t + ('=' * pad))
        s = raw.decode('utf-8')
        return s if printable_ratio(s) >= 0.85 else None
    except Exception:
        return None

def d_base58_hex(b: bytes) -> Optional[str]:
    t = b.decode('latin-1', errors='ignore').strip()
    if not t or any(ch not in BASE58 for ch in t):
        return None
    num = 0
    for ch in t:
        num = num * 58 + BASE58.index(ch)
    out = bytearray()
    while num > 0:
        num, rem = divmod(num, 256)
        out.append(rem)
    out = bytes(reversed(out))
    out = (b"\x00" * (len(t) - len(t.lstrip('1')))) + out
    return out.hex()

# ======================================================
# SEZIONE 3 — Estrarre il PAYLOAD dallo script OP_RETURN
# ======================================================
def extract_payload_from_script(script_hex: str) -> Optional[bytes]:
    try:
        b = bytes.fromhex(script_hex)
    except Exception:
        return None
    if not b or b[0] != 0x6A:  # OP_RETURN
        return None
    i, n = 1, len(b)
    out = bytearray()
    while i < n:
        op = b[i]; i += 1
        if 1 <= op <= 75:
            if i + op > n: break
            out.extend(b[i:i+op]); i += op
        elif op == 0x4C:
            if i + 1 > n: break
            ln = b[i]; i += 1
            if i + ln > n: break
            out.extend(b[i:i+ln]); i += ln
        elif op == 0x4D:
            if i + 2 > n: break
            ln = b[i] | (b[i+1] << 8); i += 2
            if i + ln > n: break
            out.extend(b[i:i+ln]); i += ln
        elif op == 0x4E:
            if i + 4 > n: break
            ln = b[i] | (b[i+1] << 8) | (b[i+2] << 16) | (b[i+3] << 24); i += 4
            if i + ln > n: break
            out.extend(b[i:i+ln]); i += ln
        else:
            break
    return bytes(out)

# ===============================================
# SEZIONE 4 — RPC Bitcoin: getblockheader
# ===============================================
def get_rpc():
    if AuthServiceProxy is None:
        raise SystemExit("Installa prima: pip install python-bitcoinrpc (o usa il venv)")
    return AuthServiceProxy(RPC_URL)

# ===============================================
# SEZIONE 5 — Main: 2 passate + ripartenza
# ===============================================
def main():
    p = argparse.ArgumentParser(description="OP_RETURN → JSONL (resume light)")
    p.add_argument('--in',  dest='in_path',  default=DEFAULT_IN)
    p.add_argument('--out', dest='out_path', default=DEFAULT_OUT)
    args = p.parse_args()

    in_path, out_path = args.in_path, args.out_path

    # RIPARTENZA: ultima riga del JSONL -> last_key; se non c'è, si parte da zero
    last_key = None   # "txid:vout"
    out_mode = 'w'
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        try:
            with open(out_path, 'rb') as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                back = min(size, 1024 * 1024)
                f.seek(-back, os.SEEK_END)
                tail = f.read().splitlines()
                while tail and not tail[-1].strip():
                    tail.pop()
                if tail:
                    last_obj = json.loads(tail[-1].decode('utf-8', errors='replace'))
                    last_txid = str(last_obj.get('txid', ''))
                    last_vout = int(last_obj.get('vout', 0))
                    if last_txid:
                        last_key = f"{last_txid}:{last_vout}"
                        out_mode = 'a'
        except Exception:
            last_key = None
            out_mode = 'w'

    # PASSATA 1 — blocchi unici
    block_hashes = set()
    with gzip.open(in_path, 'rb') as gz:
        reader = csv.DictReader(io.TextIOWrapper(gz, encoding='utf-8', newline=''))
        for row in reader:
            bh = row.get('block_hash')
            if bh: block_hashes.add(bh)

    # RPC header una volta per blocco
    rpc = get_rpc()
    hdr: Dict[str, Tuple[int,int]] = {}
    for bh in block_hashes:
        try:
            h = rpc.getblockheader(bh)
            hdr[bh] = (int(h.get('time', 0)), int(h.get('height', -1)))
        except Exception:
            hdr[bh] = (0, -1)

    # PASSATA 2 — scrittura JSONL
    with gzip.open(in_path, 'rb') as gz, open(out_path, out_mode, encoding='utf-8') as fout:
        reader = csv.DictReader(io.TextIOWrapper(gz, encoding='utf-8', newline=''))

        started = (last_key is None)   # se non ho cursore, parto subito
        seen = set()                   # dedup nello stesso run
        for row in reader:
            txid = row.get('txid','')
            vout = int(row.get('vout_index','0') or 0)
            key  = f"{txid}:{vout}"

            if not started:
                if key == last_key:
                    started = True
                continue  # finché non trovo la last_key, salto

            if key in seen:
                continue
            seen.add(key)

            bh = row.get('block_hash','')
            bt, height = hdr.get(bh, (0, -1))

            script_hex = row.get('opret_script_hex','')
            payload = extract_payload_from_script(script_hex)
            if payload is None:
                continue

            res = {
                'utf8':        d_utf8(payload),
                'url_utf8':    d_url_utf8(payload),
                'hex2_utf8':   d_hex2_utf8(payload),
                'base64_utf8': d_base64_utf8(payload),
                'base58_hex':  d_base58_hex(payload),
            }
            first = next((k for k,v in res.items() if v is not None), None)

            obj = {
                'txid': txid,
                'vout': vout,
                'block_hash': bh,
                'block_time': bt,
                'block_height': height,
                'hex': payload.hex(),
                'utf8': res['utf8'],
                'url_utf8': res['url_utf8'],
                'hex2_utf8': res['hex2_utf8'],
                'base64_utf8': res['base64_utf8'],
                'base58_hex': res['base58_hex'],
                'decoded_any': any(res.values()),
                'first_success': first,
            }
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

if __name__ == '__main__':
    main()

