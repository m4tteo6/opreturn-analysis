"""
OP_RETURN_hex
Cosa fa:
  - Si connette al nodo Bitcoin Core via RPC (user/password).
  - Scorre i blocchi dalla prossima altezza rispetto all'ultimo blocco salvato.
  - Per ogni transazione di ogni blocco:
      * per ogni output OP_RETURN (scriptPubKey.type == "nulldata")
        scrive una riga nel CSV compresso (gzip) con:
        [block_hash, tx_index, txid, vout_index, value_btc, opret_script_hex]
  - Salva/aggiorna un file di stato con l'ultimo blocco processato,
    così alla prossima esecuzione riparte da lì + 1.

File usati
  - CSV.gz:   /var/lib/opreturns/op_returns_hex.csv.gz
  - Stato:    /var/lib/opreturns/op_returns_hex.height
"""

import os
import sys
import io
import gzip
import csv
from bitcoinrpc.authproxy import AuthServiceProxy

# ======================================================

# Prende user/password dal bitcoin.conf:
# rpcuser=bitcoinrpc
# rpcpassword=RTY657YU
RPC_URL    = "http://bitcoinrpc:YOUR_PASS@127.0.0.1:8332"

# File
OUT_CSV_GZ = "/var/lib/opreturns/op_returns_hex.csv.gz"   # CSV.gz
STATE_FILE = "/var/lib/opreturns/op_returns_hex.height"   # file stato

# Intestazione CSV
CSV_HEADER = ["block_hash", "tx_index", "txid", "vout_index", "value_btc", "opret_script_hex"]


# =====================================================================================

def ensure_dirs():
    """Crea le cartelle di output se mancano."""
    os.makedirs(os.path.dirname(OUT_CSV_GZ), exist_ok=True)
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

def connect_rpc():
    """Crea il client RPC verso bitcoind usando user/password."""
    return AuthServiceProxy(RPC_URL)

def read_last_height():
    """Legge l'ultimo blocco processato. Se non esiste -1 (così si parte da 0)."""
    try:
        with open(STATE_FILE, "r") as f:
            return int(f.read().strip())
    except Exception:
        return -1

def write_last_height(h):
    """Scrive su disco l'ultimo blocco processato."""
    with open(STATE_FILE, "w") as f:
        f.write(str(h))

def write_rows(rows):
    """
    Appende righe al CSV.gz.
    Se il file non esiste ancora, scrive anche l'header.
    """
    file_exists = os.path.exists(OUT_CSV_GZ)
    mode = "ab" if file_exists else "wb"
    with gzip.open(OUT_CSV_GZ, mode) as gz:
        # Wrappa in TextIO per poter usare csv.writer in modo sicuro
        with io.TextIOWrapper(gz, encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            if not file_exists:
                w.writerow(CSV_HEADER)
            w.writerows(rows)


# ================================================================================

def main():
    ensure_dirs()
    rpc = connect_rpc()

    # Calcola da quale blocco partire. Ultimo salvato + 1
    last = read_last_height()
    start_h = last + 1

    # Altezza corrente
    tip = rpc.getblockcount()
    if start_h > tip:
        # Nulla da fare: siamo già allineati
        return

    for h in range(start_h, tip + 1):
        # Hash del blocco
        bh = rpc.getblockhash(h)
        # Blocchi con tx già decodificate (verbosity=2)
        blk = rpc.getblock(bh, 2)

        out_rows = []

        # Scorri tutte le transazioni del blocco
        for ti, tx in enumerate(blk.get("tx", [])):
            for vout in tx.get("vout", []):
                spk = vout.get("scriptPubKey", {}) or {}
                # Salviamo solo gli OP_RETURN
                if spk.get("type") != "nulldata":
                    continue

                # Preleva l'hex.
                opret_hex = spk.get("hex", "") or ""

                # Prepara riga
                row = [
                    bh,                         # block_hash
                    ti,                         # tx_index (posizione della tx nel blocco)
                    tx.get("txid", ""),         # txid
                    vout.get("n", 0),           # vout_index (indice output)
                    f"{float(vout.get('value', 0.0)):.8f}",  # value_btc (tipicamente 0.00000000)
                    opret_hex,                  # opret_script_hex
                ]
                out_rows.append(row)

        # Se il blocco conteneva OP_RETURN, appendiamo le righe
        if out_rows:
            write_rows(out_rows)

        # Aggiorna lo stato
        write_last_height(h)

    #allineato al tip


if __name__ == "__main__":
    main()

