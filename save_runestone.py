#!/opt/opreturns/venv/bin/python -u
# -*- coding: utf-8 -*-
"""
Legge /var/lib/opreturns/op_returns_hex.csv.gz e scrive
/var/lib/opreturns/op_returns_runestone.jsonl con solo le righe che
rappresentano OP_RETURN "Runestone" (script che inizia con 6a5d),
aggiungendo anche:
  - block_height  (altezza del blocco)
  - block_time    (timestamp UNIX del blocco)

Come ricaviamo height/time:
- Dal CSV abbiamo già 'block_hash'.
- Chiediamo al nodo:  bitcoin-cli getblockheader <block_hash>
- Estriamo 'height' e 'time' e li aggiungiamo alla riga JSON.

Campi in input (dal CSV):
  block_hash, tx_index, txid, vout_index, value_btc, opret_script_hex
Campi aggiunti in output (nel JSON):
  block_height (int), block_time (int)
"""

import csv
import gzip
import json
import subprocess

# File di input/output
INPUT_CSV_GZ  = "/var/lib/opreturns/op_returns_hex.csv.gz"
OUTPUT_JSONL  = "/var/lib/opreturns/op_returns_runestone.jsonl"

# Comando per interrogare il nodo 
BITCOIN_CLI = "/usr/local/bin/bitcoin-cli"
RPC_FLAGS = [
    "-rpcconnect=127.0.0.1",
    "-rpcport=8332",
    "-rpcuser=YOUR_USER",
    "-rpcpassword=YOUR_PASS",
]



def is_runestone(script_hex: str) -> bool:
    """
    Dice se lo script OP_RETURN è un "Runestone" 
    deve iniziare con '6a5d'
    """
    if not script_hex:
        return False
    return script_hex.strip().lower().startswith("6a5d")


def fetch_block_header(block_hash: str):
    """
    Chiede al nodo i dati del blocco con 'getblockheader <hash>'.
    Ritorna (height:int, time:int) oppure (None, None) se qualcosa va storto.
    """
    if not block_hash:
        return (None, None)
    try:
        res = subprocess.run(
            [BITCOIN_CLI, *RPC_FLAGS, "getblockheader", block_hash],
            check=True, capture_output=True, text=True
        )
        hdr = json.loads(res.stdout)
        return (int(hdr.get("height")), int(hdr.get("time")))
    except Exception:
        return (None, None)


def main():
    lette = 0      # righe totali lette dal CSV
    salvate = 0    # righe scritte nel JSONL


    header_cache = {}  

    with gzip.open(INPUT_CSV_GZ, mode="rt", encoding="utf-8", errors="replace", newline="") as fin, \
         open(OUTPUT_JSONL, mode="w", encoding="utf-8") as fout:

        reader = csv.DictReader(fin)

        # Check minimo: ci serve la colonna con lo script OP_RETURN
        if "opret_script_hex" not in reader.fieldnames:
            print("[ERRORE] Colonna 'opret_script_hex' non trovata nel CSV.")
            return

        # Usiamo anche block_hash; se non c'è, possiamo comunque salvare senza height/time
        have_block_hash = "block_hash" in reader.fieldnames

        for row in reader:
            lette += 1

            # 1) script che inizia con 6a5d
            if not is_runestone(row.get("opret_script_hex", "")):
                continue

            # 2) Arricchisco con altezza/tempo del blocco
            h = t = None
            if have_block_hash:
                bh = row.get("block_hash", "")
                if bh:
                    if bh in header_cache:
                        h, t = header_cache[bh]
                    else:
                        h, t = fetch_block_header(bh)
                        header_cache[bh] = (h, t)

            # 3) Aggiungo i due campi alla riga
            row_out = dict(row) 
            row_out["block_height"] = h
            row_out["block_time"] = t

            # 4) Scrivo una riga JSON 
            fout.write(json.dumps(row_out, ensure_ascii=False) + "\n")
            salvate += 1

    # Report finale
    print("FATTO ✅")
    print(f"Righe CSV lette: {lette:,}")
    print(f"Righe Runestone scritte: {salvate:,}")
    print(f"Blocchi distinti interrogati (cache): {len(header_cache):,}")
    print(f"Output JSONL: {OUTPUT_JSONL}")


if __name__ == "__main__":
    main()

