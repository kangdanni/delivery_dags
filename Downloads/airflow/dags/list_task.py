import json
import logging

import pendulum

from airflow.decorators import dag, task, task_group
from airflow.utils.edgemodifier import Label

logger = logging.getLogger(__name__)


@dag(start_date=pendulum.datetime(2022, 1, 1), schedule=None, tags=["spike"])
def list_to_task():
    @task
    def extract() -> list[int]:
        return [1, 2, 3, 4, 5, 6, 7, 8]

    def split(item: int) -> int:
        return item

    @task
    def load(value: int) -> None:
        logger.info(f"value = {value}")

    # extract_list_task = extract().map(split)
    # load.expand(value=extract_list_task)
    load.expand(value=extract())


list_to_task()
