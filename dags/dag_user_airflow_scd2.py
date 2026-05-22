import sys
sys.path.insert(0, "/home/yancy/airflow/dags")
from datetime import timedelta
from airflow.decorators import dag
import os
import dlt
from dlt.common import pendulum
from dlt.helpers.airflow_helper import PipelineTasksGroup
# ---------------------- 日志配置（你要的打印配置！） ----------------------
import logging
logger = logging.getLogger("airflow.task")
# modify the default task arguments - all the tasks created for dlt pipeline will inherit it
# - set e-mail notifications
# - we set retries to 0 and recommend to use `PipelineTasksGroup` retry policies with tenacity library, you can also retry just extract and load steps
# - execution_timeout is set to 20 hours, tasks running longer that that will be terminated

default_task_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': '3268575247@qq.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'execution_timeout': timedelta(hours=20),
}

# modify the default DAG arguments
# - the schedule below sets the pipeline to `@daily` be run each day after midnight, you can use crontab expression instead
# - start_date - a date from which to generate backfill runs
# - catchup is False which means that the daily runs from `start_date` will not be run, set to True to enable backfill
# - max_active_runs - how many dag runs to perform in parallel. you should always start with 1


@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_task_args
)
def load_csv_data():
    logger.info("=" * 50)
    logger.info("📢 任务启动成功！")
    logger.info("🗄️ 数据库配置信息：")
    logger.info(f"用户名：root")
    logger.info(f"数据库名：airflow")
    logger.info(f"数据库主机：10.255.255.254")
    logger.info(f"端口：3306")
    logger.info("=" * 50)
    # set `use_data_folder` to True to store temporary data on the `data` bucket. Use only when it does not fit on the local storage
    tasks = PipelineTasksGroup("pipeline_decomposed", use_data_folder=False, wipe_local_data=True)

    # import your source from pipeline script
    from scd2_csv import user_source as source


    # modify the pipeline parameters 
    pipeline = dlt.pipeline(pipeline_name='user_airflow_scd2',
                     dataset_name='employee_scd2',
                     destination='sqlalchemy',
                     full_refresh=False # must be false if we decompose
                     )
    # create the source, the "serialize" decompose option will converts dlt resources into Airflow tasks. use "none" to disable it
    tasks.add_run(pipeline, source(), decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)
    logger.info("✅ 任务提交完成！")
    
load_csv_data()