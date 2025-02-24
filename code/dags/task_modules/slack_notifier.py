from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime
import logging

SLACK_CONN_RUNTIME = 'slack_report_runtime'
SLACK_CONN_ERROR = 'slack_error'


def dag_slack_call_start(context):
    """
    Sends a notification to Slack when the DAG starts.
    
    Args:
        context (dict): The context object passed by Airflow which contains task and execution info.

    Returns:
        None
    """
    try:
        task_instance = context.get('task_instance')
        dag = context.get('dag')
        execution_date = context.get('execution_date')
        local_dt = execution_date.strftime('%Y-%m-%d %H:%M:%S')
        slack_message = f"🚀 *{dag}* started in {local_dt} 🚀"

        slack_notify = SlackWebhookOperator(
            task_id="send_slack_notification",
            slack_webhook_conn_id=SLACK_CONN_RUNTIME,
            message=slack_message,
            username="airflow_bot"
        )
        return slack_notify.execute(context=context)
    except Exception as e:
        logging.error(f"Error sending Slack start message: {e}")
        return None


def dag_success_slack_alert(context):
    """
    Sends a success message to Slack when the DAG finishes successfully.

    Args:
        context (dict): The context object passed by Airflow which contains task and execution info.

    Returns:
        None
    """
    try:
        task_instance = context.get('task_instance')
        execution_date = context.get('execution_date')
        local_dt = execution_date.strftime('%Y-%m-%d %H:%M:%S')

        slack_msg = f"""\
        :white_check_mark: *Successfully finished DAG!*
*Dag*: {task_instance.dag_id}
*Execution Date*: {local_dt}
*Log Url*: <{task_instance.log_url}|Click here for logs>
=============================
        """
        slack_notify = SlackWebhookOperator(
            task_id='slack_success_message',
            slack_webhook_conn_id=SLACK_CONN_RUNTIME,
            message=slack_msg,
            username="airflow_bot"
        )
        return slack_notify.execute(context=context)
    except Exception as e:
        logging.error(f"Error sending Slack success message: {e}")
        return None


def dag_failure_slack_alert(context):
    """
    Sends an error message to Slack when a task in the DAG fails.

    Args:
        context (dict): The context object passed by Airflow which contains task and execution info.

    Returns:
        None
    """
    try:
        dag = context.get('dag')
        task_instance = context.get('task_instance')
        execution_date = context.get('execution_date')
        local_dt = execution_date.strftime('%Y-%m-%d %H:%M:%S')
        slack_msg = f"""\
        :red_circle: *Error in DAG!!!*
*Dag*: {task_instance.dag_id}
*Task*: {task_instance.task_id}
*Execution Date*: {local_dt}
*Log Url*: {task_instance.log_url}
=============================
        """
        failed_alert = SlackWebhookOperator(
            task_id='slack_failed_message',
            slack_webhook_conn_id=SLACK_CONN_ERROR,
            message=slack_msg,
            username="airflow_bot"
        )
        return failed_alert.execute(context=context)
    except Exception as e:
        logging.error(f"Error sending Slack failure message: {e}")
        return None


def send_slack_report(ti):
    """
    Send report to Slack.
    
    Args:
        report_message (str): Message to send.
    
    Returns:
        None
    """

    try:
        rows_before_raw = ti.xcom_pull(task_ids="count_rows_before_etl", key="rows_before_raw")
        rows_before_errors = ti.xcom_pull(task_ids="count_rows_before_etl", key="rows_before_errors")
        rows_before_clean = ti.xcom_pull(task_ids="count_rows_before_etl", key="rows_before_clean")

        rows_after_raw = ti.xcom_pull(task_ids="count_rows_after_etl", key="rows_after_raw")
        rows_after_errors = ti.xcom_pull(task_ids="count_rows_after_etl", key="rows_after_errors")
        rows_after_clean = ti.xcom_pull(task_ids="count_rows_after_etl", key="rows_after_clean")

        
        report_message = f"""
        *ETL REPORT*:
        
    *Before*:
    Raw: {rows_before_raw}
    Errors: {rows_before_errors}
    Clean: {rows_before_clean}

    *After*:
    Raw: {rows_after_raw}
    Errors: {rows_after_errors}
    Clean: {rows_after_clean}
        """  

        slack_notify = SlackWebhookOperator(
            task_id="send_slack_notification",
            slack_webhook_conn_id=SLACK_CONN_RUNTIME,
            message=report_message,
            username="airflow_bot",
        )
        slack_notify.execute(context={})
        logging.info("📱 Slack report sent successfully")

    except Exception as e:
        logging.error(f"❌ Error while sendinr report to Slack: {e}")
