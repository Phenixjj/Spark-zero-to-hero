"""Génère les fichiers CSV utilisés par les exercices, sans dépendance externe."""

import csv
from pathlib import Path


DATA = Path(__file__).resolve().parent / "data"

CUSTOMERS = [
    (1, "Alice", "Paris"),
    (2, "Bob", "Lyon"),
    (3, "Chloé", "Paris"),
    (4, "David", "Lille"),
    (5, "Eva", "Lyon"),
]

ORDERS = [
    (101, 1, "2024-01-02", "100.00", "completed"),
    (102, 1, "2024-01-03", "50.00", "cancelled"),
    (103, 2, "2024-01-02", "80.00", "completed"),
    (104, 2, "2024-01-04", "20.00", "completed"),
    (105, 3, "2024-01-03", "40.00", "completed"),
    (106, 3, "2024-01-03", "60.00", "completed"),
    (107, 4, "2024-01-05", "", "completed"),
    (108, 1, "2024-01-06", "25.00", "completed"),
]


def write_csv(name, header, rows):
    path = DATA / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def main():
    DATA.mkdir(parents=True, exist_ok=True)
    write_csv("customers.csv", ("customer_id", "name", "city"), CUSTOMERS)
    write_csv(
        "orders/orders.csv",
        ("order_id", "customer_id", "order_date", "amount", "status"),
        ORDERS,
    )
    print(f"Données générées dans {DATA}")


if __name__ == "__main__":
    main()
