"""Vérifie la reproductibilité des CSV et exécute les cinq corrections."""

import hashlib
from pathlib import Path
import subprocess
import sys

import generate_data


ROOT = Path(__file__).resolve().parent


def hashes():
    return {
        str(path.relative_to(generate_data.DATA)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(generate_data.DATA.rglob("*.csv"))
    }


def main():
    generate_data.main()
    first = hashes()
    assert set(first) == {"customers.csv", "orders/orders.csv"}, first
    generate_data.main()
    assert hashes() == first, "La génération des données n'est pas reproductible"

    solutions = sorted((ROOT / "solutions").glob("[0-9][0-9]_*.py"))
    assert len(solutions) == 5, solutions
    for path in solutions:
        print(f"Vérification : {path.name}", flush=True)
        subprocess.run([sys.executable, str(path)], check=True, cwd=ROOT)
    print("Génération et cinq corrections validées.")


if __name__ == "__main__":
    main()
