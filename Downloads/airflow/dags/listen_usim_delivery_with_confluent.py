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
import typing as t
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.sensors.base import PokeReturnValue
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
    카프카 메세지를 변환한다

    메세지 헤더에 `expiration(timestamp)`이 기록되어 있다면 expiration을 `execution_date`로 사용하고
    그 외에 메세지 `생성시각(timestamp)`과 `만료시각(expiration_secs)`를 더한 값을 `execution_date`로 사용한다.

    :param message: 메세지
    :param expiration_secs: 만료시각
    :returns: 변경된 메세지를 반환한다
        * conf: 트리거에 전달할 파라미터
        * execution_date: 트리거 시작시각
    """

    logger.debug(
        f"topic: {message.topic()} partition: {message.partition()}, offset: {message.offset()}, expiration_secs: {expiration_secs}"
    )

    headers = (
        {key: value for (key, value) in message.headers()} if message.headers() else {}
    )
    expiration = (
        float(headers.get("expiration", "0"))
        or (message.timestamp() / 1000) + expiration_secs
    )
    logger.debug(f"expiration: {expiration}")

    return {
        "conf": json.loads(message.value()),
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
def listen_usim_delivery_with_confluent():
    """
    ### 유심배송정보를 수신한다

    유심배송정보를 수신하여 정해신 시간 후에 배송상태를 조회하는 작업을 요청한다
    """

    @task.sensor(poke_interval=10, timeout=3600, soft_fail=True)
    def listen(
        kafka_config_id: str,
        topics: list[str],
        transformer: t.Callable[[Message], t.Any],
        num_messages: int = 100,
        poll_timeout_secs: int = 30,
    ) -> PokeReturnValue:
        """
        #### 배송정보를 수신한다

        :param kafka_config_id: 카프카 연결정보
        :param topics: 토픽목록
        :transformer: 메세지변환 콜백함수
        :param num_messages: 최대 메세지 개수
        :param poll_timeout_secs: 메세지 수신 대기 시간
        :returns: 수신한 메세지를 돌려준다.
        """
        # 메세지를 가져온다
        hook = KafkaConsumerHook(topics, kafka_config_id=kafka_config_id)
        consumer = hook.get_consumer()

        try:
            if messages := consumer.consume(
                num_messages=num_messages, timeout=poll_timeout_secs
            ):
                logger.debug(f"size: {len(messages)}")

                # 메세지를 변환한다
                messages_ = [transformer(message) for message in messages]
                logger.debug(f"records: {messages_}")

                return PokeReturnValue(is_done=True, xcom_value=messages_)
        finally:
            consumer.close()

    # region 플로우 정의
    listen_task = listen(
        kafka_config_id="mvno.kafka_connection.airflow",
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


listen_usim_delivery_with_confluent()
