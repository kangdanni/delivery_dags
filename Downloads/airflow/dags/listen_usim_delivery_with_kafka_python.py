# -*- coding: utf-8 -*-
#
# 유심배송 요청 수신
#
# .. note::
#    Airflow 커넥션
#    - mvno.slack_webhook_connection.task_failure: MVNO 작업오류 슬랙 웹훅 커넥션
#    - mvno.kafka_connection.airflow-kafka_python: 카프카 Connection ID
#
#    Airflow 변수
#    - mvno.usim_order_topics: 토픽
#    - mvno.usim_order_expiration_secs: 배송만료시각
#    - poc.usim_order_mentions: 오류발생시 확인할 슬랙이용자(콤마구분, 옵션)
#
# .. moduleauthor::
#    - john doe<john_doe@sk.com>
#
import functools as fn
import json
import logging
import typing as t
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import PokeReturnValue
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import TopicPartition
from utils import on_task_failure_wrapper

from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

# region 상수
TARGET_DAG_ID = "check_usim_delivery"
# endregion

# region 변수
on_task_failure_ = fn.partial(
    on_task_failure_wrapper,
    connection_id="mvno.slack_webhook_connection.task_failure",
    mentions=lambda: (lambda x: x.split(",") if x else [])(
        Variable.get("poc.usim_order_mentions", "")
    ),
)
# endregion


def transform(
    topic_partition: TopicPartition,
    record: ConsumerRecord,
    expiration_secs: t.Callable[[], int],
) -> dict:
    """
    카프카 메세지를 변환한다

    메세지 헤더에 `expiration(timestamp)`이 기록되어 있다면 expiration을 `execution_date`로 사용하고
    그 외에 메세지 `생성시각(timestamp)`과 `만료시각(expiration_secs)`를 더한 값을 `execution_date`로 사용한다.

    :param topic_partition: 메세지 토픽 및 파티션
    :param record: 메세지
    :param expiration_secs: 만료시각
    :returns: 변경된 메세지를 반환한다
        * conf: 트리거에 전달할 파라미터
        * execution_date: 트리거 시작시각
    """
    expiration_secs_ = expiration_secs()

    logger.debug(
        f"topic: {topic_partition[0]} partition: {topic_partition[1]}, record: {record}, expiration_secs: {expiration_secs_}"
    )

    headers = {key: value for (key, value) in record.headers} if record.headers else {}
    expiration = (
        float(headers.get("expiration", "0"))
        or (record.timestamp / 1000) + expiration_secs_
    )
    logger.debug(f"expiration: {expiration}")

    return {
        "conf": record.value,
        "execution_date": pendulum.from_timestamp(expiration),
    }


@dag(
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    schedule="@continuous",
    max_active_runs=1,
    catchup=False,
    render_template_as_native_obj=True,
    default_args={
        "owner": "poc",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "on_failure_callback": on_task_failure_,
    },
    tags=["poc", "리스너"],
)
def listen_usim_delivery_with_kafka_python():
    """
    ### 유심배송정보를 수신한다

    유심배송정보를 수신하여 정해신 시간 후에 배송상태를 조회하는 작업을 요청한다
    """

    @task.sensor(poke_interval=10, timeout=3600, soft_fail=True)
    def listen(
        kafka_config_id: str,
        topics: list[str],
        poll_timeout_secs: int = 30,
        transformer: t.Callable[[TopicPartition, ConsumerRecord], t.Any] = None,
    ) -> PokeReturnValue:
        """
        #### 배송정보를 수신한다

        :param kafka_config_id: 카프카 연결정보
        :param topics: 토픽목록
        :param poll_timeout_secs: 메세지 수신 대기 시간
        :transformer: 메세지변환 콜백함수
        :returns: 수신한 메세지를 돌려준다.
            `transformer`가 정의되어 있지 않다면 `(TopicPartition, ConsumerRecord) 목록`을 돌려주고
            `transformer`가 정의되어 있다면 `transformer`에서 정의한 목록을 돌려준다
        """
        # 연결정보를 가져온다
        connection = Connection.get_connection_from_secrets(kafka_config_id)
        config = connection.extra_dejson
        config = {
            **config,
            "value_deserializer": lambda x: json.loads(x.decode("utf-8")),
        }

        logger.debug(f"topics: {topics}, config: {config}")

        # 메세지를 가져온다
        consumer = KafkaConsumer(*topics, **config)

        try:
            if records := consumer.poll(timeout_ms=poll_timeout_secs * 1000):
                logger.debug(f"size: {len(records)}")

                # 메세지를 변환한다
                records_ = (
                    [
                        (key, record)
                        for key, value in records.items()
                        for record in value
                    ]
                    if not transformer
                    else [
                        transformer(key, record)
                        for key, value in records.items()
                        for record in value
                    ]
                )

                logger.debug(f"records: {records_}")

                return PokeReturnValue(is_done=True, xcom_value=records_)
        finally:
            consumer.close()

    # region 플로우 정의
    listen_task = listen(
        kafka_config_id="mvno.kafka_connection.airflow-kafka_python",
        topics="{{ var.value.get('mvno.usim_order_topics').split(',') }}",
        transformer=fn.partial(
            transform,
            expiration_secs=lambda: int(
                Variable.get("mvno.usim_order_expiration_secs", "1800")
            ),
        ),
    )

    trigger_task = TriggerDagRunOperator.partial(
        task_id=f"trigger_check_usim",
        trigger_dag_id=TARGET_DAG_ID,
    ).expand_kwargs(listen_task)
    trigger_task.md_doc = "#### 배송정보를 배송정보확인 플로우에 전달한다"

    # endregion


listen_usim_delivery_with_kafka_python()
