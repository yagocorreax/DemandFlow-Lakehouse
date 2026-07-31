import argparse
import os
import random
import time
from decimal import Decimal
from pathlib import Path
from typing import Callable

import psycopg
from dotenv import load_dotenv
from psycopg import Connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"A variável obrigatória {name} não foi encontrada no arquivo .env."
        )

    return value


def create_connection() -> Connection:
    return psycopg.connect(
        host=get_required_env("POSTGRES_HOST"),
        port=get_required_env("POSTGRES_PORT"),
        dbname=get_required_env("POSTGRES_DATABASE"),
        user=get_required_env("POSTGRES_USER"),
        password=get_required_env("POSTGRES_PASSWORD"),
    )


def create_sale(connection: Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT store_id
            FROM stores
            WHERE is_active = TRUE
            ORDER BY RANDOM()
            LIMIT 1;
            """
        )

        store = cursor.fetchone()

        if store is None:
            raise RuntimeError("Nenhuma loja ativa foi encontrada.")

        store_id = store[0]

        cursor.execute(
            """
            SELECT
                products.product_id,
                products.unit_price,
                inventory.stock_quantity
            FROM products
            INNER JOIN inventory
                ON inventory.product_id = products.product_id
            WHERE inventory.store_id = %s
              AND inventory.stock_quantity > 0
              AND products.is_active = TRUE
            ORDER BY RANDOM()
            LIMIT 1
            FOR UPDATE OF inventory;
            """,
            (store_id,),
        )

        product = cursor.fetchone()

        if product is None:
            raise RuntimeError(
                f"Nenhum produto com estoque foi encontrado na loja {store_id}."
            )

        product_id, unit_price, stock_quantity = product
        quantity = random.randint(1, min(3, stock_quantity))
        total_amount = unit_price * quantity

        cursor.execute(
            """
            INSERT INTO orders (
                store_id,
                order_status,
                total_amount
            )
            VALUES (%s, 'PAID', %s)
            RETURNING order_id;
            """,
            (store_id, total_amount),
        )

        order_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO order_items (
                order_id,
                product_id,
                quantity,
                unit_price
            )
            VALUES (%s, %s, %s, %s);
            """,
            (
                order_id,
                product_id,
                quantity,
                unit_price,
            ),
        )

        cursor.execute(
            """
            UPDATE inventory
            SET stock_quantity = stock_quantity - %s
            WHERE store_id = %s
              AND product_id = %s;
            """,
            (
                quantity,
                store_id,
                product_id,
            ),
        )

        cursor.execute(
            """
            INSERT INTO inventory_movements (
                store_id,
                product_id,
                movement_type,
                quantity,
                reason
            )
            VALUES (%s, %s, 'OUT', %s, %s);
            """,
            (
                store_id,
                product_id,
                -quantity,
                f"Venda do pedido {order_id}",
            ),
        )

        print(
            f"[VENDA] Pedido={order_id} "
            f"Loja={store_id} Produto={product_id} "
            f"Quantidade={quantity} Total=R$ {total_amount}"
        )


def replenish_stock(connection: Connection) -> None:
    quantity = random.randint(10, 50)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT store_id, product_id
            FROM inventory
            ORDER BY RANDOM()
            LIMIT 1
            FOR UPDATE;
            """
        )

        inventory_record = cursor.fetchone()

        if inventory_record is None:
            raise RuntimeError("Nenhum registro de estoque foi encontrado.")

        store_id, product_id = inventory_record

        cursor.execute(
            """
            UPDATE inventory
            SET stock_quantity = stock_quantity + %s
            WHERE store_id = %s
              AND product_id = %s;
            """,
            (
                quantity,
                store_id,
                product_id,
            ),
        )

        cursor.execute(
            """
            INSERT INTO inventory_movements (
                store_id,
                product_id,
                movement_type,
                quantity,
                reason
            )
            VALUES (%s, %s, 'IN', %s, 'Reposição de estoque');
            """,
            (
                store_id,
                product_id,
                quantity,
            ),
        )

        print(
            f"[REPOSIÇÃO] Loja={store_id} "
            f"Produto={product_id} Quantidade={quantity}"
        )


def update_product_price(connection: Connection) -> None:
    multiplier = random.choice(
        [
            Decimal("0.95"),
            Decimal("1.05"),
            Decimal("1.10"),
        ]
    )

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT product_id, unit_price
            FROM products
            WHERE is_active = TRUE
            ORDER BY RANDOM()
            LIMIT 1
            FOR UPDATE;
            """
        )

        product = cursor.fetchone()

        if product is None:
            raise RuntimeError("Nenhum produto ativo foi encontrado.")

        product_id, current_price = product

        new_price = (current_price * multiplier).quantize(
            Decimal("0.01")
        )

        cursor.execute(
            """
            UPDATE products
            SET unit_price = %s
            WHERE product_id = %s;
            """,
            (
                new_price,
                product_id,
            ),
        )

        print(
            f"[PREÇO] Produto={product_id} "
            f"Preço anterior=R$ {current_price} "
            f"Novo preço=R$ {new_price}"
        )


def cancel_order(connection: Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                orders.order_id,
                orders.store_id,
                order_items.product_id,
                order_items.quantity
            FROM orders
            INNER JOIN order_items
                ON order_items.order_id = orders.order_id
            WHERE orders.order_status = 'PAID'
            ORDER BY RANDOM()
            LIMIT 1
            FOR UPDATE OF orders;
            """
        )

        order = cursor.fetchone()

        if order is None:
            print("[CANCELAMENTO] Nenhum pedido disponível.")
            return

        order_id, store_id, product_id, quantity = order

        cursor.execute(
            """
            UPDATE orders
            SET order_status = 'CANCELLED'
            WHERE order_id = %s;
            """,
            (order_id,),
        )

        cursor.execute(
            """
            UPDATE inventory
            SET stock_quantity = stock_quantity + %s
            WHERE store_id = %s
              AND product_id = %s;
            """,
            (
                quantity,
                store_id,
                product_id,
            ),
        )

        cursor.execute(
            """
            INSERT INTO inventory_movements (
                store_id,
                product_id,
                movement_type,
                quantity,
                reason
            )
            VALUES (%s, %s, 'RETURN', %s, %s);
            """,
            (
                store_id,
                product_id,
                quantity,
                f"Cancelamento do pedido {order_id}",
            ),
        )

        print(
            f"[CANCELAMENTO] Pedido={order_id} "
            f"Estoque devolvido={quantity}"
        )


def update_forecast(connection: Connection) -> None:
    multiplier = Decimal(
        str(random.uniform(0.80, 1.20))
    )

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT forecast_id, forecast_quantity
            FROM demand_forecasts
            ORDER BY RANDOM()
            LIMIT 1
            FOR UPDATE;
            """
        )

        forecast = cursor.fetchone()

        if forecast is None:
            raise RuntimeError("Nenhuma previsão foi encontrada.")

        forecast_id, current_quantity = forecast

        new_quantity = max(
            Decimal("0"),
            (current_quantity * multiplier).quantize(
                Decimal("0.01")
            ),
        )

        cursor.execute(
            """
            UPDATE demand_forecasts
            SET forecast_quantity = %s
            WHERE forecast_id = %s;
            """,
            (
                new_quantity,
                forecast_id,
            ),
        )

        print(
            f"[FORECAST] Previsão={forecast_id} "
            f"Quantidade anterior={current_quantity} "
            f"Nova quantidade={new_quantity}"
        )


def run_generator(
    iterations: int,
    interval_seconds: float,
) -> None:
    actions: list[Callable[[Connection], None]] = [
        create_sale,
        create_sale,
        create_sale,
        replenish_stock,
        update_product_price,
        cancel_order,
        update_forecast,
        update_forecast,
    ]

    with create_connection() as connection:
        print("Conexão com o PostgreSQL realizada.")

        for iteration in range(1, iterations + 1):
            action = random.choice(actions)

            try:
                action(connection)
                connection.commit()
                print(
                    f"Operação {iteration}/{iterations} confirmada.\n"
                )
            except Exception as error:
                connection.rollback()
                print(
                    f"Operação {iteration}/{iterations} revertida: "
                    f"{error}\n"
                )

            if iteration < iterations and interval_seconds > 0:
                time.sleep(interval_seconds)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gerador transacional do DemandFlow."
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Quantidade de operações que serão executadas.",
    )

    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=0,
        help="Intervalo entre as operações.",
    )

    arguments = parser.parse_args()

    if arguments.iterations < 1:
        parser.error("--iterations deve ser maior que zero.")

    if arguments.interval_seconds < 0:
        parser.error("--interval-seconds não pode ser negativo.")

    return arguments


def main() -> None:
    arguments = parse_arguments()

    run_generator(
        iterations=arguments.iterations,
        interval_seconds=arguments.interval_seconds,
    )


if __name__ == "__main__":
    main()
