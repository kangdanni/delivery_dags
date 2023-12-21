import json
import logging
import typing as t

import pendulum
from airflow.models import Variable
from airflow.models.dag import DagContext
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from jinja2 import BaseLoader, Environment

logger = logging.getLogger(__name__)

# region 상수
ON_FAILURE_TITLE = "Task Failed!!!"
ON_FAILURE_TEMPLATE = """
[
    {
        "type": "header",
        "text": {"type": "plain_text", "text": "{{ message }}", "emoji": true}
    },
    {
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": "*Dag:*\\n {{ dag_id }}"},
            {"type": "mrkdwn", "text": "*Task:*\\n {{ task_id }}"}
        ]
    },
    {
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": "*Start:*\\n {{ start }}"},
            {"type": "mrkdwn", "text": "*End:*\\n {{ end }}"}
        ]
    },
    {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*Exception:*\\n {{ exception }}"}
    },
    {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "<{{ log_url }}|View Log>"}
    }
]
"""
# endregion

# region 변수
on_failure_template = Environment(loader=BaseLoader).from_string(ON_FAILURE_TEMPLATE)
# endregion


def on_task_failure(
    context: DagContext,
    connection_id: str,
    mentions: list[str] = [],
    timezone: str = "Asia/Seoul",
) -> None:
    """
    실패가 발생햇을 경우 슬랙에 메세지를 전송한다

    :param context: Dag컨텍스트
    :param connection_id: 슬랙 CONNECTION ID
    :param mentions: 사용자목록
    :param timezone: 타임존
    """
    logger.debug(f"context: {context}, connection_id: {connection_id}")

    mentions_ = ",".join(map(lambda x: f"<@{x}>", mentions)) if mentions else None
    start = pendulum.instance(context.get("data_interval_start")).set(tz=timezone)
    end = pendulum.instance(context.get("data_interval_end")).set(tz=timezone)
    content = on_failure_template.render(
        message=ON_FAILURE_TITLE,
        dag_id=context.get("task_instance").dag_id,
        task_id=context.get("task_instance").task_id,
        start=start.to_datetime_string(),
        end=end.to_datetime_string(),
        exception=str(context.get("exception")),
        log_url=context.get("task_instance").log_url,
    )
    logger.debug(f"content: {content}")

    title = f"{ON_FAILURE_TITLE} {mentions_}" if mentions_ else ON_FAILURE_TITLE
    content_ = json.loads(content)
    logger.debug(f"content: {content_}")

    hook = SlackWebhookHook(slack_webhook_conn_id=connection_id)
    hook.send(text=title, blocks=content_)


def on_task_failure_wrapper(
    context: DagContext,
    connection_id: str,
    mentions: t.Callable[[], list[str]] = None,
    timezone: str = "Asia/Seoul",
) -> None:
    """
    실패가 발생햇을 경우 슬랙에 메세지를 전송한다

    :param context: Dag컨텍스트
    :param connection_id: 슬랙 CONNECTION ID
    :param mentions: 사용자목록
    :param timezone: 타임존
    """
    return on_task_failure(context, connection_id, mentions(), timezone)
