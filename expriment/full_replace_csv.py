import dlt
import pandas as pd

# 数据源 + Schema契约：锁定表、列、类型不允许自动乱演化
@dlt.source(
    name="user_infor",
    schema_contract={
        "tables": "evolve",
        "columns": "evolve",
        "data_type": "evolve"
    }
)
def user_source():
    df = pd.read_csv("./data/user_data.csv")
    yield dlt.resource(
        df,
        name="user_data",
        write_disposition="replace"
    )

def run():
    pipeline = dlt.pipeline(
        pipeline_name="user_pipeline",
        destination="sqlalchemy",
        dataset_name="employee_database",
        dev_mode=False,
        progress="log",
        import_schema_path="schemas/import"
        # export_schema_path="schemas/export"
    )

    load_info = pipeline.run(user_source())
    print("\n=== 最终生效的 Schema ===")
    print(pipeline.default_schema.to_pretty_yaml())

if __name__ == "__main__":
    run()