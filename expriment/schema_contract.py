import logging
import pandas as pd
import dlt
from dlt.common.pendulum import pendulum
from datetime import date

@dlt.source(
    name="user_infor",
    schema_contract={
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "discard_row"
    }
)
def user_source():
    df = pd.read_csv("./data/user_data.csv")
    df_1 = df[:9].copy()
    df_2 = df[9:].copy()
    # df_1["phone_number"] = df_1["phone_number"].astype(str)

    @dlt.resource(name="user_data_1", write_disposition="replace")
    def get_user_data_1():
        yield from df_1.to_dict("records")

    @dlt.resource(name="user_data_2", write_disposition="replace")
    def get_user_data_2():
        yield from df_2.to_dict("records")
    return [
        get_user_data_1,
        get_user_data_2
    ]

# --------------------------
# 运行 pipeline
# --------------------------
def run():
    pipeline = dlt.pipeline(
        pipeline_name="user_pipeline_a",
        destination="sqlalchemy",
        dataset_name="employee_database_a",
        dev_mode=False,
        progress="log",
        export_schema_path="schemas/export",
        import_schema_path="schemas/import"
    )

    load_info = pipeline.run(user_source())
    print("\n=== ✅ 最终生效的 Schema ===")
    print(pipeline.default_schema.to_pretty_yaml())

if __name__ == "__main__":
    run()