import dlt
import pandas as pd


@dlt.resource(
    name="user_data",
    primary_key="user_id",
    write_disposition={
        "disposition": "merge",
        "strategy": "scd2",
        "validity_column_names": ["valid_from", "valid_to"],  # 自定义列名
        "active_record_timestamp": "9999-12-31",             # 有效表示
    }
)
def load_user_data():
    df = pd.read_csv("/home/yancy/airflow/dags/data/user_data.csv")     
    yield from df.to_dict("records")

@dlt.source(
    name="user_infor",
    schema_contract={
        "tables": "evolve",
        "columns": "evolve",
        "data_type": "evolve"
    }
)
def user_source():
    yield load_user_data

def run():
    pipeline = dlt.pipeline(
        pipeline_name="user_pipeline_scd2",
        destination="sqlalchemy",
        dataset_name="employee_scd2",
        dev_mode=False,
        progress="log"
    )

    load_info = pipeline.run(user_source())
    print("\n=== 最终 SCD2 Schema ===")
    print(pipeline.default_schema.to_pretty_yaml())

if __name__ == "__main__":
    run()