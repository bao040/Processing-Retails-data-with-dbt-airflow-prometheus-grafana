from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import re
import requests
import json
from datetime import datetime
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import select
from airflow.utils.task_group import TaskGroup

# Set up logging
logger = logging.getLogger(__name__)

# MS Teams Webhook URL
TEAMS_WEBHOOK_URL = "https://fptsoftware362.webhook.office.com/webhookb2/8f2b0e33-b08a-496e-903b-147f82bc7bb2@f01e930a-b52e-42b1-b70f-a8882b5d043b/IncomingWebhook/43ffed40b8374cbd82d4630c217a9e07/dcae0e10-51e2-4d88-9656-035e9644c9d9/V2Emm4XSYtmYdXI08RWkhChwDpjTlmEtO67NT1-RTH5Zo1"

# PostgreSQL connection details
PG_CONN = {
    'host': 'host.docker.internal',
    'port': '5435',
    'dbname': 'airflow15',
    'user': 'airflow15',
    'password': 'airflow15'
}

# Function to clean ANSI color codes from logs
def clean_ansi_codes(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

# Function to parse dbt run/snapshot logs
def parse_dbt_run_logs(logs):
    summary = {
        "pass": 0,
        "warn": 0,
        "error": 0,
        "skip": 0,
        "failed_models": []
    }
    if not isinstance(logs, str):
        summary["failed_models"] = ["Log parsing failed: Invalid or missing logs"]
        return summary
    summary_match = re.search(r"Done\. PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+) TOTAL=(\d+)", logs)
    if summary_match:
        summary["pass"] = int(summary_match.group(1))
        summary["warn"] = int(summary_match.group(2))
        summary["error"] = int(summary_match.group(3))
        summary["skip"] = int(summary_match.group(4))
    failed_matches = re.findall(r"(?:Failure in model|ERROR creating) ([^\s]+)", logs)
    summary["failed_models"] = failed_matches
    return summary

# Function to parse dbt test logs
def parse_dbt_test_logs(logs):
    summary = {
        "pass": 0,
        "warn": 0,
        "error": 0,
        "skip": 0,
        "failed_tests": [],
        "warned_tests": []
    }
    if not isinstance(logs, str):
        summary["failed_tests"] = ["Log parsing failed: Invalid or missing logs"]
        return summary
    logs = clean_ansi_codes(logs)
    summary_match = re.search(r"Done\. PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+) TOTAL=(\d+)", logs)
    if summary_match:
        summary["pass"] = int(summary_match.group(1))
        summary["warn"] = int(summary_match.group(2))
        summary["error"] = int(summary_match.group(3))
        summary["skip"] = int(summary_match.group(4))
    failed_matches = re.findall(
        r"Failure in test ([^\s]+) \([^\)]+\)\s*\n\s*\d+:\d+:\d+\s+Got (\d+) results?, configured to fail if != 0",
        logs,
        re.MULTILINE
    )
    warned_matches = re.findall(
        r"Warning in test ([^\s]+) \([^\)]+\)\s*\n\s*\d+:\d+:\d+\s+Got (\d+) results?, configured to warn if != 0",
        logs,
        re.MULTILINE
    )
    summary["failed_tests"] = [f"{test} (Failed {count} rows)" for test, count in failed_matches]
    summary["warned_tests"] = [f"{test} (Warned {count} rows)" for test, count in warned_matches]
    return summary

# Function to summarize and send results to MS Teams
def send_teams_notification(**context):
    ti = context['ti']
    staging_summary = parse_dbt_run_logs(ti.xcom_pull(task_ids='dbt_run_staging'))
    snapshot_summary = parse_dbt_run_logs(ti.xcom_pull(task_ids='dbt_snapshot'))
    intermediate_summary = parse_dbt_run_logs(ti.xcom_pull(task_ids='dbt_run_intermediate'))
    
    intermediate_test_log_file = '/opt/airflow/dbt_project/retails_project/target/test_intermediate_output.log'
    try:
        with open(intermediate_test_log_file, 'r') as f:
            intermediate_test_logs = f.read()
        logger.info(f"Intermediate test logs content (first 2000 chars):\n{intermediate_test_logs[:2000]}")
    except Exception as e:
        intermediate_test_logs = f"Failed to read intermediate test log file: {str(e)}"
        logger.error(intermediate_test_logs)
    intermediate_test_summary = parse_dbt_test_logs(intermediate_test_logs)
    
    marts_summary = parse_dbt_run_logs(ti.xcom_pull(task_ids='dbt_run_marts'))
    
    marts_test_log_file = '/opt/airflow/dbt_project/retails_project/target/test_marts_output.log'
    try:
        with open(marts_test_log_file, 'r') as f:
            marts_test_logs = f.read()
        logger.info(f"Marts test logs content (first 2000 chars):\n{marts_test_logs[:2000]}")
    except Exception as e:
        marts_test_logs = f"Failed to read marts test log file: {str(e)}"
        logger.error(marts_test_logs)
    marts_test_summary = parse_dbt_test_logs(marts_test_logs)

    logger.info(f"Intermediate test summary: {intermediate_test_summary}")
    logger.info(f"Marts test summary: {marts_test_summary}")

    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": "DBT Pipeline Run Summary",
        "sections": [
            {
                "activityTitle": "DBT Pipeline Run Summary",
                "activitySubtitle": f"Run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "facts": [
                    {
                        "name": "Staging Models",
                        "value": f"PASS={staging_summary['pass']}, WARN={staging_summary['warn']}, ERROR={staging_summary['error']}, SKIP={staging_summary['skip']}"
                    },
                    {
                        "name": "Snapshots",
                        "value": f"PASS={snapshot_summary['pass']}, WARN={snapshot_summary['warn']}, ERROR={snapshot_summary['error']}, SKIP={snapshot_summary['skip']}"
                    },
                    {
                        "name": "Intermediate Models",
                        "value": f"PASS={intermediate_summary['pass']}, WARN={intermediate_summary['warn']}, ERROR={intermediate_summary['error']}, SKIP={intermediate_summary['skip']}"
                    },
                    {
                        "name": "Tests (Intermediate/Valid)",
                        "value": f"PASS={intermediate_test_summary['pass']}, WARN={intermediate_test_summary['warn']}, ERROR={intermediate_test_summary['error']}, SKIP={intermediate_test_summary['skip']}"
                    },
                    {
                        "name": "Marts Models",
                        "value": f"PASS={marts_summary['pass']}, WARN={marts_summary['warn']}, ERROR={marts_summary['error']}, SKIP={marts_summary['skip']}"
                    },
                    {
                        "name": "Tests (Marts)",
                        "value": f"PASS={marts_test_summary['pass']}, WARN={marts_test_summary['warn']}, ERROR={marts_test_summary['error']}, SKIP={marts_test_summary['skip']}"
                    }
                ]
            }
        ]
    }

    if (staging_summary["failed_models"] or snapshot_summary["failed_models"] or 
        intermediate_summary["failed_models"] or marts_summary["failed_models"] or 
        intermediate_test_summary["failed_tests"] or intermediate_test_summary["warned_tests"] or
        marts_test_summary["failed_tests"] or marts_test_summary["warned_tests"]):
        details_section = {
            "title": "Details of Failures and Warnings",
            "text": ""
        }
        if staging_summary["failed_models"]:
            details_section["text"] += f"\n **Failed Staging Models**:\n- " + "\n- ".join(staging_summary["failed_models"]) + "\n"
        if snapshot_summary["failed_models"]:
            details_section["text"] += f"\n **Failed Snapshots**:\n- " + "\n- ".join(snapshot_summary["failed_models"]) + "\n"
        if intermediate_summary["failed_models"]:
            details_section["text"] += f"\n **Failed Intermediate Models**:\n- " + "\n- ".join(intermediate_summary["failed_models"]) + "\n"
        if marts_summary["failed_models"]:
            details_section["text"] += f"\n **Failed Marts Models**:\n- " + "\n- ".join(marts_summary["failed_models"]) + "\n"
        if intermediate_test_summary["failed_tests"]:
            details_section["text"] += f"\n **Failed Intermediate Tests**:\n- " + "\n- ".join(intermediate_test_summary["failed_tests"]) + "\n"
        if intermediate_test_summary["warned_tests"]:
            details_section["text"] += f"\n **Warned Intermediate Tests**:\n- " + "\n- ".join(intermediate_test_summary["warned_tests"]) + "\n"
        if marts_test_summary["failed_tests"]:
            details_section["text"] += f"\n **Failed Marts Tests**:\n- " + "\n- ".join(marts_test_summary["failed_tests"]) + "\n"
        if marts_test_summary["warned_tests"]:
            details_section["text"] += f"\n **Warned Marts Tests**:\n- " + "\n- ".join(marts_test_summary["warned_tests"]) + "\n"
        message["sections"].append(details_section)
    else:
        if intermediate_test_summary["error"] > 0 or marts_test_summary["error"] > 0:
            logger.warning("No failed tests parsed, including cleaned raw log snippet")
            details_section = {
                "title": "Details of Failures and Warnings",
                "text": ""
            }
            if intermediate_test_summary["error"] > 0:
                cleaned_logs = clean_ansi_codes(intermediate_test_logs)
                intermediate_snippet = "\n".join(
                    line.strip() for line in cleaned_logs.splitlines()
                    if "Failure in test" in line or "Got" in line
                )[:500]
                details_section["text"] += f"**Failed Intermediate Tests (Raw Log Snippet)**:\n{intermediate_snippet}\n"
            if marts_test_summary["error"] > 0:
                cleaned_logs = clean_ansi_codes(marts_test_logs)
                marts_snippet = "\n".join(
                    line.strip() for line in cleaned_logs.splitlines()
                    if "Failure in test" in line or "Got" in line
                )[:500]
                details_section["text"] += f"**Failed Marts Tests (Raw Log Snippet)**:\n{marts_snippet}"
            message["sections"].append(details_section)

    headers = {'Content-Type': 'application/json'}
    response = requests.post(TEAMS_WEBHOOK_URL, headers=headers, data=json.dumps(message))
    if response.status_code != 200:
        raise ValueError(f"Failed to send Teams notification: {response.text}")

# Custom sensor to listen for PostgreSQL NOTIFY events
class PostgresNotifySensor(BaseSensorOperator):
    def __init__(self, conn_params, channel, schema, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_params = conn_params
        self.channel = channel
        self.schema = schema

    def poke(self, context):
        try:
            conn = psycopg2.connect(**self.conn_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            cursor.execute(f"LISTEN {self.channel};")

            # Wait for notification up to poke_interval seconds
            if select.select([conn], [], [], self.poke_interval) == ([], [], []):
                cursor.close()
                conn.close()
                return False  # No notification yet

            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                logger.info(f"Received notification: {notify.payload}")
                cursor.close()
                conn.close()
                return True
        except Exception as e:
            logger.error(f"Error in PostgresNotifySensor: {str(e)}")
            raise

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'trigger_rule': TriggerRule.ALL_DONE
}

with DAG(
    'dbt_retails_pipeline',
    default_args=default_args,
    description='A DAG to run dbt commands triggered by DB changes and send summary to MS Teams',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Sensor to listen for DB changes
    db_change_sensor = PostgresNotifySensor(
        task_id='db_change_sensor',
        conn_params=PG_CONN,
        channel='table_changes',
        schema='pos_raw_retails_data',
        poke_interval=10,
        timeout=3600,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # --- Task Group: Staging ---
    with TaskGroup("staging_group", tooltip="Run dbt staging and snapshot") as staging_group:
        dbt_run_staging = BashOperator(
            task_id='dbt_run_staging',
            bash_command='cd /opt/airflow/dbt_project/retails_project && dbt run --select models/staging --profiles-dir profiles',
            do_xcom_push=True,
            retries=0,
            trigger_rule=TriggerRule.ALL_DONE
        )

        dbt_snapshot = BashOperator(
            task_id='dbt_snapshot',
            bash_command='cd /opt/airflow/dbt_project/retails_project && dbt snapshot --profiles-dir profiles',
            do_xcom_push=True,
            retries=0,
            trigger_rule=TriggerRule.ALL_DONE
        )

        # dbt_run_staging >> dbt_snapshot

    # --- Task Group: Intermediate ---
    with TaskGroup("intermediate_group", tooltip="Run and test intermediate models") as intermediate_group:
        dbt_run_intermediate = BashOperator(
            task_id='dbt_run_intermediate',
            bash_command='cd /opt/airflow/dbt_project/retails_project && dbt run --select models/intermediate --profiles-dir profiles',
            do_xcom_push=True,
            retries=0,
            trigger_rule=TriggerRule.ALL_DONE
        )

        dbt_test_intermediate = BashOperator(
            task_id='dbt_test_intermediate',
            bash_command='cd /opt/airflow/dbt_project/retails_project && set -o pipefail && dbt test --select models/intermediate/valid --profiles-dir profiles > /opt/airflow/dbt_project/retails_project/target/test_intermediate_output.log 2>&1 || true',
            do_xcom_push=True,
            retries=0,
            trigger_rule=TriggerRule.ALL_DONE
        )

        # dbt_run_intermediate >> dbt_test_intermediate

    # --- Task Group: Marts ---
    with TaskGroup("marts_group", tooltip="Run and test marts models") as marts_group:
        dbt_run_marts = BashOperator(
            task_id='dbt_run_marts',
            bash_command='cd /opt/airflow/dbt_project/retails_project && dbt run --select models/marts --profiles-dir profiles',
            do_xcom_push=True,
            retries=0,
            trigger_rule=TriggerRule.ALL_DONE
        )

        dbt_test_marts = BashOperator(
            task_id='dbt_test_marts',
            bash_command='cd /opt/airflow/dbt_project/retails_project && set -o pipefail && dbt test --select models/marts --profiles-dir profiles > /opt/airflow/dbt_project/retails_project/target/test_marts_output.log 2>&1 || true',
            do_xcom_push=True,
            retries=0,
            trigger_rule=TriggerRule.ALL_DONE
        )

        # dbt_run_marts >> dbt_test_marts

    # --- Final Task: Send summary to Teams ---
    send_summary = PythonOperator(
        task_id='send_teams_summary',
        python_callable=send_teams_notification,
        provide_context=True,
        retries=0,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # --- Set dependencies ---
    db_change_sensor >> staging_group >> intermediate_group >> marts_group >> send_summary




# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 0,
#     'trigger_rule': TriggerRule.ALL_DONE
# }

# with DAG(
#     'dbt_retails_pipeline',
#     default_args=default_args,
#     description='A DAG to run dbt commands triggered by DB changes and send summary to MS Teams',
#     schedule_interval=None,
#     start_date=days_ago(1),
#     catchup=False,
# ) as dag:

#     # Sensor to listen for DB changes
#     db_change_sensor = PostgresNotifySensor(
#         task_id='db_change_sensor',
#         conn_params=PG_CONN,
#         channel='table_changes',
#         schema='pos_raw_retails_data',
#         poke_interval=10,  # Check every 10 seconds
#         timeout=3600,  # Timeout after 1 hour
#         trigger_rule=TriggerRule.ALL_DONE
#     )

#     # Task for dbt run --select models/staging
#     dbt_run_staging = BashOperator(
#         task_id='dbt_run_staging',
#         bash_command='cd /opt/airflow/dbt_project/retails_project && dbt run --select models/staging --profiles-dir profiles',
#         do_xcom_push=True,
#         retries=0,
#         trigger_rule=TriggerRule.ALL_DONE
#     )

#     # Task for dbt snapshot
#     dbt_snapshot = BashOperator(
#         task_id='dbt_snapshot',
#         bash_command='cd /opt/airflow/dbt_project/retails_project && dbt snapshot --profiles-dir profiles',
#         do_xcom_push=True,
#         retries=0,
#         trigger_rule=TriggerRule.ALL_DONE
#     )

#     # Task for dbt run --select models/intermediate
#     dbt_run_intermediate = BashOperator(
#         task_id='dbt_run_intermediate',
#         bash_command='cd /opt/airflow/dbt_project/retails_project && dbt run --select models/intermediate --profiles-dir profiles',
#         do_xcom_push=True,
#         retries=0,
#         trigger_rule=TriggerRule.ALL_DONE
#     )

#     # Task for dbt test --select models/intermediate/valid
#     dbt_test_intermediate = BashOperator(
#         task_id='dbt_test_intermediate',
#         bash_command='cd /opt/airflow/dbt_project/retails_project && set -o pipefail && dbt test --select models/intermediate/valid --profiles-dir profiles > /opt/airflow/dbt_project/retails_project/target/test_intermediate_output.log 2>&1 || true',
#         do_xcom_push=True,
#         retries=0,
#         trigger_rule=TriggerRule.ALL_DONE
#     )

#     # Task for dbt run --select models/marts
#     dbt_run_marts = BashOperator(
#         task_id='dbt_run_marts',
#         bash_command='cd /opt/airflow/dbt_project/retails_project && dbt run --select models/marts --profiles-dir profiles',
#         do_xcom_push=True,
#         retries=0,
#         trigger_rule=TriggerRule.ALL_DONE
#     )

#     # Task for dbt test --select models/marts
#     dbt_test_marts = BashOperator(
#         task_id='dbt_test_marts',
#         bash_command='cd /opt/airflow/dbt_project/retails_project && set -o pipefail && dbt test --select models/marts --profiles-dir profiles > /opt/airflow/dbt_project/retails_project/target/test_marts_output.log 2>&1 || true',
#         do_xcom_push=True,
#         retries=0,
#         trigger_rule=TriggerRule.ALL_DONE
#     )

#     # Task to send summary to MS Teams
#     send_summary = PythonOperator(
#         task_id='send_teams_summary',
#         python_callable=send_teams_notification,
#         provide_context=True,
#         retries=0,
#         trigger_rule=TriggerRule.ALL_DONE
#     )

#     # Set task dependencies
#     db_change_sensor >> dbt_run_staging >> dbt_snapshot >> dbt_run_intermediate >> dbt_test_intermediate >> dbt_run_marts >> dbt_test_marts >> send_summary