# -*- coding: utf-8 -*-
#
# 유심배송 확인
#
# .. note::
#    Airflow 커넥션
#    - mvno.slack_webhook_connection.task_failure: MVNO 작업오류 슬랙 웹훅 커넥션
#    - poc.http_connection: POC API서버
#    - delivery.http_connection: 배송플랫폼 API서버
#
#    Airflow 변수
#    - delivery.health_check_endpoint: 헬스체크 Endpoint
#    - delivery.inquiry_endpoint: 배송조회 Endpoint
#    - poc.health_check_endpoint: 헬스체크 Endpoint
#    - poc.usim_order_cancel_endpoint: 배차취소 Endpoint
#    - poc.usim_order_complete_endpoint: 배차성공 Endpoint
#    - poc.usim_order_mentions: 오류발생시 확인할 슬랙이용자(콤마구분, 옵션)
#
# .. moduleauthor::
#    - john doe<john_doe@sk.com>
#
import functools as fn
import json
import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.trigger_rule import TriggerRule
from utils import on_task_failure_wrapper

logger = logging.getLogger(__name__)

# region 상수

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


@dag(
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    params={"orderId": Param("", type="string", description="주문번호", section="필수"),
            # "book_id": Param("", type="string", description="업체주문번호", section="필수")
            },
    default_args={
        "owner": "poc",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "on_failure_callback": on_task_failure_,
    },
    tags=["poc"],
)
def check_usim_delivery():
    """
    ### 배송처리 상태를 확인한다
    전달받은 주문에 대해 배차가 되지 않았다면 배송취소를 수행한다
    """

    @task.short_circuit
    def is_dispatched(info: dict) -> bool:
        """
        #### 배차되었는지 확인한다

        :param info:배차정보
        :returns: 배차가 되었으면 True 그 외에는 False를 돌려준다
        """
        logger.debug(f"info: {info}")
        return True if not info["status"] == "10" else False

    @task.short_circuit
    def is_not_dispatched(info: dict) -> bool:
        """
        #### 배차되지 않았는지 확인한다

        :param info:배차정보
        :returns: 배차가 되지 않았다면 True 그 외에는 False를 돌려준다
        """
        logger.debug(f"info: {info}")
        return True if info["status"] == "10" else False

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0, on_failure_callback=None)
    def on_external_system_error(
        ti: TaskInstance = None, params: dict = None, **context: dict
    ) -> None:
        """
        #### 외부시스템 오류에 대해서 처리한다

        :param ti: 작업인스턴스
        :param params: 파라미터
        :param context: 컨텍스트
        """
        # TODO 오류에 대한 후처리를 수행한다  (예: 메세지큐에 주문정보 재등록 등, DB기록, 슬랙전송 등)

        # XXX 오류로 작업을 종료한다
        raise AirflowFailException()

    # region 플로우 정의

    delivery_check_task = HttpOperator(
        task_id="check_delivery",
        http_conn_id="delivery.http_connection",
        method="POST",
        endpoint="{{ var.value.get('delivery.inquiry_endpoint') }}",
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "purc_rcpt_id": "{{ params.orderId }}"
            }
        ),
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )
    delivery_check_task.doc_md = "#### 배송접수를 조회한다"


    cancel_order_task = HttpOperator(
        task_id="cancel_order",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        http_conn_id="poc.http_connection",
        method="POST",
        endpoint="{{ var.value.get('poc.usim_order_cancel_endpoint') }}",
        data=json.dumps(
            {
                "purc_rcpt_id": "{{ params.orderId }}"
            }
        ),
        headers={"Content-Type": "application/json"},
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )
    cancel_order_task.doc_md = "#### 주문을 취소한다"

    

    # 기본 플로우

    delivery_check_task >> cancel_order_task



    not_dispatched = is_not_dispatched(delivery_check_task.output)
    not_dispatched >> cancel_order_task

    # 예외 플로우
    [
        delivery_check_task,
        cancel_order_task
   
    ] >> on_external_system_error()
    # endregion


check_usim_delivery()
