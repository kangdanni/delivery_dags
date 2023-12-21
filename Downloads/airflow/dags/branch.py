import json
import logging

import pendulum

from airflow.decorators import dag, task, task_group
from airflow.utils.edgemodifier import Label

logger = logging.getLogger(__name__)


@dag(start_date=pendulum.datetime(2022, 1, 1), schedule=None, tags=["spike"])
def branch_with_flowtask_api():
    import random

    random.seed(pendulum.now().timestamp())

    @task
    def load() -> int:
        return random.randint(1, 10)

    @task.branch
    def branch(value: int) -> str:
        return "odd" if value % 2 else "even"

    @task
    def even(value: int) -> None:
        logger.info(f"an even value: {value}")

    @task
    def odd(value: int) -> None:
        logger.info(f"an odd value: {value}")

    # 작업정의
    load_task = load()
    # branch_task = branch(load_task)
    # even_task = even(load_task)
    # odd_task = odd(load_task)
    branch_task = branch("{{ti.xcom_pull(task_ids='load_task'}}")
    even_task = even("{{ti.xcom_pull(task_ids='load_task'}}")
    odd_task = odd("{{ti.xcom_pull(task_ids='load_task'}}")
    (
        load_task
        >> branch_task
        >> [
            even_task,
            odd_task,
        ]
    )


branch_with_flowtask_api()


@dag(start_date=pendulum.datetime(2022, 1, 1), schedule=None, tags=["spike"])
def example_cb_etl():
    @task()
    def extract() -> dict:
        return json.loads('{"item_1": 42, "item_2": 24}')

    @task()
    def transform(items_in_usd: dict) -> dict:
        usd_to_euro_conversion_rate = 0.937478
        return {
            name: round(cost_in_usd * usd_to_euro_conversion_rate, 2)
            for name, cost_in_usd in items_in_usd.items()
        }

    @task.short_circuit
    def test_not_negative(items_in_usd: dict) -> bool:
        for item in items_in_usd.values():
            if item < 0:
                return False

        return True

    @task()
    def load(items_in_euros: dict) -> int:
        count = 0
        for name, cost_in_euros in items_in_euros.items():
            logger.info(f"item {name} is {cost_in_euros} euros.")
            count += 1

        return count

    transform_task = transform(extract())
    condition_task = test_not_negative(transform_task)
    load_task = load(transform_task)

    transform_task >> condition_task >> load_task


example_cb_etl()
