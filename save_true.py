"""
Cosa fa:
- Legge il file /var/lib/opreturns/op_returns_decoded.jsonl riga per riga.
- Per ogni riga controlla che 'decode_any' sia True.
- Se è True, scrive la riga intera nell'output /var/lib/opreturns/op_returns_decoded_true.jsonl.


Nota:
- Se una riga non è JSON valido, la salto.
- Se l'ultima riga del file di input è "incompleta" (senza \n), mi fermo: vuol dire che il file è ancora in scrittura.
"""

import json

# Percorsi
INPUT_PATH  = "/var/lib/opreturns/op_returns_decoded.jsonl"
OUTPUT_PATH = "/var/lib/opreturns/op_returns_decoded_true.jsonl"


def decode_any_is_true(line_text):
    """
    Ritorna True se la riga JSON ha 'decode_any' == True, altrimenti False.
    - Se la riga non è JSON valido, ritorna False.
    """
    try:
        obj = json.loads(line_text)
        return isinstance(obj, dict) and (obj.get("decode_any") is True)
    except Exception:
        return False


def main():
    # Contatori solo per info finale
    lette = 0
    salvate = 0

    with open(OUTPUT_PATH, "w", encoding="utf-8") as fout:
        # Apro l'input in lettura testuale UTF-8
        with open(INPUT_PATH, "r", encoding="utf-8", errors="replace") as fin:
            for line in fin:
                # Se la riga non termina con newline, è probabilmente incompleta -> stop
                # (utile se il file sta ancora crescendo mentre lo leggiamo)
                if not line.endswith("\n"):
                    break

                lette += 1

                if decode_any_is_true(line):
                    fout.write(line)
                    salvate += 1

    # Report finale
    print("FATTO ✅")
    print(f"Righe lette: {lette}")
    print(f"Righe salvate (decode_any=true): {salvate}")
    print(f"File creato: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

