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
#    - mvno.if_batch_queue_name: 배치큐 이름
#
# .. moduleauthor::
#    - john doe<john_doe@sk.com>
#
import functools as fn
import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task_group
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.providers.amazon.aws.sensors.batch import BatchSensor
from utils import on_task_failure_wrapper

logger = logging.getLogger(__name__)

# region 상수
REGION_NAME = "ap-northeast-2"
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
    params={
        "yyyymmdd": Param(
            pendulum.yesterday(tz="Asia/Seoul").format("YYYYMMDD"),
            type="string",
            description="일자",
            section="필수",
        )
    },
    default_args={
        "owner": "mvno",
        "on_failure_callback": on_task_failure_,
    },
    tags=["mvno", "배치", "고객정보"],
)
def apply_ctrt_info():
    """
    ### 고객정보 반영
    인터페이스로 전송받은 고객정보를 시스템에 반영한다
    """

    # region 플로우 정의
    apply_ctrt_info_task = BatchOperator(
        task_id="mvno.aws_connection",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="apply-ctrt-info-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-cust",
        container_overrides={
            "command": ["batch_cust", "apply-ctrt-info", "--yyyymmdd", "Ref::date"]
        },
        parameters={"date": "{{ params.yyyymmdd }}"},
    )
    apply_ctrt_info_task.md_doc = "#### 고객정보 시스템 반영"

    apply_addons_task = BatchOperator(
        task_id="apply-addon-task",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="apply-addon-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-cust",
        container_overrides={
            "command": ["batch_cust", "apply-addons", "--yyyymmdd", "Ref::date"]
        },
        parameters={"date": "{{ params.yyyymmdd }}"},
    )
    apply_addons_task.md_doc = "#### 부가서비스 시스템 반영"

    agg_ctrt_info_task = BatchOperator(
        task_id="agg-ctrt-info-task",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="agg-ctrt-info-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-sett",
        container_overrides={
            "command": ["batch_sett_agg", "agg-ctrt-info", "--yyyymmdd", "Ref::date"]
        },
        parameters={"date": "{{ params.yyyymmdd }}"},
    )
    agg_ctrt_info_task.md_doc = "#### 일별 가입/해지정보 수집"

    apply_ctrt_info_task >> apply_addons_task >> agg_ctrt_info_task
    # endregion


apply_ctrt_info()
