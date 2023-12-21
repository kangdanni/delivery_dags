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

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.amazon.aws.operators.batch import BatchOperator
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


@task_group
def summary_monthly_usage(yyyymm: str) -> None:
    @task_group()
    def generate_temporary_information(yyyymm: str):
        # region 플로우 정의
        agg_work_temp_ctrt_task = BatchOperator(
            task_id="agg_work_temp_ctrt",
            aws_conn_id="mvno.aws_connection",
            region_name=REGION_NAME,
            job_name="agg-work-temp-ctrt-task",
            job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
            job_definition="batch-sett",
            container_overrides={
                "command": [
                    "batch_sett_agg",
                    "agg-work-temp-ctrt",
                    "--yyyymm",
                    "Ref::yyyymm",
                ]
            },
            parameters={"yyyymm": yyyymm},
        )
        agg_work_temp_ctrt_task.md_doc = "#### 월별통계 계약임시"

        agg_work_temp_svc_task = BatchOperator(
            task_id="agg_work_temp_svc",
            aws_conn_id="mvno.aws_connection",
            region_name=REGION_NAME,
            job_name="agg-work-temp-svc-task",
            job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
            job_definition="batch-sett",
            container_overrides={
                "command": [
                    "batch_sett_agg",
                    "agg-work-temp-svc",
                    "--yyyymm",
                    "Ref::yyyymm",
                ]
            },
            parameters={"yyyymm": yyyymm},
        )
        agg_work_temp_svc_task.md_doc = "#### 월별통계 서비스"

        agg_work_temp_ppay_cdr_task = BatchOperator(
            task_id="agg_work_temp_ppay_cdr",
            aws_conn_id="mvno.aws_connection",
            region_name=REGION_NAME,
            job_name="agg-work-temp-ppay-cdr-task",
            job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
            job_definition="batch-sett",
            container_overrides={
                "command": [
                    "batch_sett_agg",
                    "agg-work-temp-ppay-cdr",
                    "--yyyymm",
                    "Ref::yyyymm",
                ]
            },
            parameters={"yyyymm": yyyymm},
        )
        agg_work_temp_ppay_cdr_task.md_doc = "#### 월별통계 선불CDR"

        agg_work_temp_post_cdr_task = BatchOperator(
            task_id="agg_work_temp_post_cdr",
            aws_conn_id="mvno.aws_connection",
            region_name=REGION_NAME,
            job_name="agg-work-temp-post-cdr-task",
            job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
            job_definition="batch-sett",
            container_overrides={
                "command": [
                    "batch_sett_agg",
                    "agg-work-temp-post-cdr",
                    "--yyyymm",
                    "Ref::yyyymm",
                ]
            },
            parameters={"yyyymm": yyyymm},
        )
        agg_work_temp_post_cdr_task.md_doc = "#### 월별통계 후불CDR"

        agg_work_temp_band_cdr_task = BatchOperator(
            task_id="agg_work_temp_band_cdr",
            aws_conn_id="mvno.aws_connection",
            region_name=REGION_NAME,
            job_name="agg-work-temp-band-cdr-task",
            job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
            job_definition="batch-sett",
            container_overrides={
                "command": [
                    "batch_sett_agg",
                    "agg-work-temp-band-cdr",
                    "--yyyymm",
                    "Ref::yyyymm",
                ]
            },
            parameters={"yyyymm": yyyymm},
        )
        agg_work_temp_band_cdr_task.md_doc = "#### 밴드CDR 후불CDR"
        # endregion

    # region 플로우 정의
    agg_mon_cdr_sum_task = BatchOperator(
        task_id="agg_mon_cdr_sum",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="agg-mon-cdr-sum-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-sett",
        container_overrides={
            "command": ["batch_sett_agg", "agg-mon-cdr-sum", "--yyyymm", "Ref::yyyymm"]
        },
        parameters={"yyyymm": yyyymm},
    )
    agg_mon_cdr_sum_task.md_doc = "#### 월별집계"

    temp_task = generate_temporary_information(yyyymm)
    temp_task >> agg_mon_cdr_sum_task
    # endregion


@task_group
def aggregate_1st(yyyymm: str) -> None:
    @task_group
    def prepare() -> None:
        agg_chrg_pln_subsr_stus_task = BatchOperator(
            task_id="agg_chrg_pln_subsr_stus",
            aws_conn_id="mvno.aws_connection",
            region_name=REGION_NAME,
            job_name="agg-chrg-pln-subsr-stus-task",
            job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
            job_definition="batch-sett",
            container_overrides={
                "command": [
                    "batch_sett_agg",
                    "agg-chrg-pln-subsr-stus",
                    "--yyyymm",
                    "Ref::yyyymm",
                ]
            },
            parameters={"yyyymm": yyyymm},
        )
        agg_chrg_pln_subsr_stus_task.md_doc = "#### 월별요금제별가입현황"

        agg_ppay_sal_stus_task = BatchOperator(
            task_id="agg_ppay_sal_stus",
            aws_conn_id="mvno.aws_connection",
            region_name=REGION_NAME,
            job_name="agg-ppay-sal-stus-task",
            job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
            job_definition="batch-sett",
            container_overrides={
                "command": [
                    "batch_sett_agg",
                    "agg-ppay-sal-stus",
                    "--yyyymm",
                    "Ref::yyyymm",
                ]
            },
            parameters={"yyyymm": yyyymm},
        )
        agg_ppay_sal_stus_task.md_doc = "#### 월별선불매출현황"

        agg_prod_usage_task = BatchOperator(
            task_id="agg_prod_usage",
            aws_conn_id="mvno.aws_connection",
            region_name=REGION_NAME,
            job_name="agg-prod-usage-task",
            job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
            job_definition="batch-sett",
            container_overrides={
                "command": [
                    "batch_sett_agg",
                    "agg-prod-usage",
                    "--yyyymm",
                    "Ref::yyyymm",
                ]
            },
            parameters={"yyyymm": yyyymm},
        )
        agg_prod_usage_task.md_doc = "#### 요금제별이용현황"

        agg_chrg_pln_subsr_stus_task >> agg_prod_usage_task

    agg_whole_sale_sum_task = BatchOperator(
        task_id="agg_whole_sale_sum",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="agg-whole-sale-sum-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-sett",
        container_overrides={
            "command": ["batch_sett_agg", "agg-whole-sale-sum", "Ref::yyyymm"]
        },
        parameters={"yyyymm": yyyymm},
    )
    agg_whole_sale_sum_task.md_doc = "#### 도매대가결산"

    agg_prod_use_task = BatchOperator(
        task_id="agg_prod_use",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="agg-prod-use-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-sett",
        container_overrides={
            "command": ["batch_sett_agg", "agg-prod-use", "Ref::yyyymm"]
        },
        parameters={"yyyymm": yyyymm},
    )
    agg_prod_use_task.md_doc = "#### 가결산용 요금제별평균요금 집계"

    prepare() >> agg_whole_sale_sum_task >> agg_prod_use_task


@task_group
def aggregate_2nd(yyyymm: str) -> None:
    agg_ppay_sal_stus_task = BatchOperator(
        task_id="agg_ppay_sal_stus",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="agg-ppay-sal-stus-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-sett",
        container_overrides={
            "command": [
                "batch_sett_agg",
                "agg-ppay-sal-stus",
                "--yyyymm",
                "Ref::yyyymm",
            ]
        },
        parameters={"yyyymm": yyyymm},
    )
    agg_ppay_sal_stus_task.md_doc = "#### 월별선불매출현황"

    agg_prod_usage_task = BatchOperator(
        task_id="agg_prod_usage",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="agg-prod-usage-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-sett",
        container_overrides={
            "command": [
                "batch_sett_agg",
                "agg-prod-usage",
                "--yyyymm",
                "Ref::yyyymm",
            ]
        },
        parameters={"yyyymm": yyyymm},
    )
    agg_prod_usage_task.md_doc = "#### 요금제별이용현황"

    agg_spam_sus_task = BatchOperator(
        task_id="agg_spam_sus",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="agg-spam-sus-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-sett",
        container_overrides={
            "command": [
                "batch_sett_agg",
                "agg-spam-sus",
            ]
        },
    )
    agg_spam_sus_task.md_doc = "#### SPAM정지이력"

    agg_ppay_maintain_task = BatchOperator(
        task_id="agg_ppay_maintain",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="agg-ppay-maintain-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-sett",
        container_overrides={
            "command": [
                "batch_sett_agg",
                "agg-ppay-maintain",
                "--yyyymm",
                "Ref::yyyymm",
            ]
        },
        parameters={"yyyymm": yyyymm},
    )
    agg_ppay_maintain_task.md_doc = "#### 선불지표관리실사용률"

    agg_ppay_month_info_task = BatchOperator(
        task_id="agg_ppay_month_info",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="agg-ppay-month-info-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-sett",
        container_overrides={
            "command": [
                "batch_sett_agg",
                "agg-ppay-month-info",
                "--yyyymm",
                "Ref::yyyymm",
            ]
        },
        parameters={"yyyymm": yyyymm},
    )
    agg_ppay_month_info_task.md_doc = "#### 선불지표관리월집계"

    agg_ppay_add_info_task = BatchOperator(
        task_id="agg_ppay_add_info",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="agg-ppay-add-info-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-sett",
        container_overrides={
            "command": [
                "batch_sett_agg",
                "agg-ppay-add-info",
                "--yyyymm",
                "Ref::yyyymm",
            ]
        },
        parameters={"yyyymm": yyyymm},
    )
    agg_ppay_add_info_task.md_doc = "#### 선불지표관리추가정보"

    agg_ppay_avg_info_task = BatchOperator(
        task_id="agg_ppay_avg_info",
        aws_conn_id="mvno.aws_connection",
        region_name=REGION_NAME,
        job_name="agg-ppay-avg-info-task",
        job_queue="{{ var.value.get('mvno.if_batch_queue_name') }}",
        job_definition="batch-sett",
        container_overrides={
            "command": [
                "batch_sett_agg",
                "agg-ppay-avg-info",
                "--yyyymm",
                "Ref::yyyymm",
            ]
        },
        parameters={"yyyymm": yyyymm},
    )
    agg_ppay_avg_info_task.md_doc = "#### 선불지표관리평균요건"


@dag(
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    schedule="0 4 1,5 * *",
    catchup=False,
    default_args={
        "owner": "mvno",
        "on_failure_callback": on_task_failure_,
    },
    tags=["mvno", "배치", "집계"],
)
def aggregate_usage_data():
    """
    ### 사용량 집계
    """

    @task
    def extract_date_info() -> dict:
        return {
            "last_month": pendulum.now(tz="Asia/Seoul").add(months=-1).format("YYYYMM"),
            "day": pendulum.now("Asia/Seoul").day,
        }

    @task.short_circuit
    def is_valid_day(day: int, days: list[int]) -> bool:
        """
        #### 현재일자가 days에 포함되는지를 확인한다
        :param day: 일자
        :param days: 유효한 일자 목록
        :returns: day가 days에 포함되면 True를 돌려주고 그 외에는 False를 돌려준다
        """
        logger.debug(f"day: {day}, days: {days}")
        return day in days

    # region 플로우 정의
    extract_date_info_task = extract_date_info()
    summary_monthly_uage_task = summary_monthly_usage(
        "{{ ti.xcom_pull(task_ids='extract_date_info')['last_month'] }}"
    )

    extract_date_info_task >> summary_monthly_uage_task

    is_1st_day_task = is_valid_day.override(task_id="is_1st_day")(
        day=extract_date_info_task["day"], days=[1]
    )
    is_5th_day_task = is_valid_day.override(task_id="is_5th_day")(
        day=extract_date_info_task["day"], days=[5]
    )

    summary_monthly_uage_task >> [is_1st_day_task, is_5th_day_task]

    aggregate_1st_task = aggregate_1st(
        "{{ ti.xcom_pull(task_ids='extract_date_info')['last_month'] }}"
    )
    aggregate_2nd_task = aggregate_2nd(
        "{{ ti.xcom_pull(task_ids='extract_date_info')['last_month'] }}"
    )

    is_1st_day_task >> aggregate_1st_task
    is_5th_day_task >> aggregate_2nd_task
    # endregion


aggregate_usage_data()
