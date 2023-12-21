# -*- coding: utf-8 -*-
#
# 유심배송 요청 수신
#
# .. note::
#    Airflow 커넥션
#    - mvno.slack_webhook_connection.task_failure: MVNO 작업오류 슬랙 웹훅 커넥션
#    - mvno.kafka_connection.airflow: 카프카 Connection ID
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
import uuid
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.kafka.sensors.kafka import (
    AwaitMessageTriggerFunctionSensor,
)
from confluent_kafka import Message
from utils import on_task_failure_wrapper

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


def transform(message: Message, expiration_secs: int) -> dict:
    """
    수신된 메세지를 파이썬 객체로 변환한다

    메세지 헤더에 `expiration(timestamp)`이 기록되어 있다면 expiration을 `execution_date`로 사용하고
    그 외에 메세지 `생성시각(timestamp)`과 `만료시각(expiration_secs)`를 더한 값을 `execution_date`로 사용한다.

    :param message: 카프카 메세지
    :param expiration_secs: 만료시각
    :returns: 변경된 메세지를 반환한다
        * conf: 트리거에 전달할 파라미터
        * execution_date: 트리거 시작시각
    """
    logger.debug(
        f"timestamp: {message.timestamp()}, headers: {message.headers()}, topic: {message.topic()}, key: {message.key()}, value: {message.value()}"
    )

    headers = (
        {key: value for (key, value) in message.headers()} if message.headers() else {}
    )
    (_, timestamp) = message.timestamp()
    expiration = (
        float(headers.get("expiration", "0")) or (timestamp / 1000) + expiration_secs
    )

    return {
        "conf": json.loads(message.value()),
        "execution_date": pendulum.from_timestamp(expiration),
    }


def trigger(message: dict, **context: dict) -> None:
    """
    배송조회 작업을 호출한다

    :param message: 메세지
        * conf: 트리거에 전달할 파라미터
        * execution_date: 트리거 시작시각
    :param context: 컨텍스트
    """
    logger.debug(f"event: {message}, context: {context}")

    TriggerDagRunOperator(
        task_id=f"triggered_downstream_dag_{uuid.uuid4()}",
        trigger_dag_id=TARGET_DAG_ID,
        conf=message["conf"],
        execution_date=message["execution_date"],
    ).execute(context)


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
def listen_usim_delivery():
    """
    ### 유심 배송요청 정보를 수신한 후 정해진 시간 후에 배송상태를 조회한다
    """

    # region 플로우 정의
    listen_task = AwaitMessageTriggerFunctionSensor(
        task_id=f"listen",
        kafka_config_id="mvno.kafka_connection.airflow",
        topics="{{ var.value.get('mvno.usim_order_topics').split(',') }}",
        poll_interval=0,
        apply_function="listen_usim_delivery.transform",
        apply_function_kwargs={
            "expiration_secs": "{{ var.value.get('mvno.usim_order_expiration_secs', '1800')|int }}",
        },
        event_triggered_function=trigger,
    )
    listen_task.md_doc = "#### 배송정보를 수신한다"

    # endregion


listen_usim_delivery()
