import logging
import pandas as pd
import dlt
from dlt.common.pendulum import pendulum
from datetime import date

# --------------------------
# 全局日志配置（控制台+文件，和你之前的GitHub示例风格统一）
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("dlt_user_pipeline.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("User_CSV_DLT_Pipeline")

# --------------------------
# 1. 定义带增量的 CSV 数据源（复刻 GitHub 示例的增量写法）
# --------------------------
@dlt.source(name="user_incremental")
def user_source():
    # 定义带增量的资源，和GitHub示例的写法完全对应
    @dlt.resource(
        name="user_data",
        write_disposition="append",  # 演示append模式
        primary_key="user_id",        # 可选，保证数据唯一性
        # 【核心增量配置】和GitHub示例的endpoint.incremental逻辑完全对应
        incremental=dlt.sources.incremental(
            cursor_path="signup_date",          # 用signup_date作为游标
            initial_value=date(2024, 1, 1),       # 首次运行的起始日期
            range_end="closed"                   # 包含等于游标值的数据
        )
    )
    def user_data():
        # 读取CSV数据
        df = pd.read_csv("./data/user_data.csv")
        
        # 把signup_date转为日期格式（关键！否则增量会失效）
        df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.date
        
        # 按增量游标过滤数据：只返回大于等于上次记录的signup_date的数据
        # DLT会自动把上次的游标值传入，这里和GitHub示例的since参数作用一致
        yield from df.to_dict("records")

    yield user_data()

# --------------------------
# 2. 运行流水线（和你之前的GitHub示例结构完全一致）
# --------------------------
def load_user_data() -> None:
    logger.info("=== 启动 CSV 用户数据增量同步流水线 ===")
    
    try:
        pipeline = dlt.pipeline(
            pipeline_name="user_csv_pipeline",
            destination="sqlalchemy",
            dataset_name="esa_database",
            progress="log",  # 保留日志进度条，和GitHub示例统一
            dev_mode=False   # 生产/演示模式，保留增量状态
        )

        # 运行source，自动处理增量过滤
        load_info = pipeline.run(user_source())
        
        logger.info("=== CSV 用户数据增量同步完成 ===")
        logger.info(f"加载结果：\n{load_info}")
        print(load_info)

        # 【演示高光】打印当前的增量游标状态，让观众直观看到效果
        current_state = pipeline.last_trace.last_normalize_info
        if current_state:
            logger.info(f"当前增量游标状态：{pipeline.state}")

    except Exception as e:
        logger.error(f"=== CSV 用户数据同步失败：{str(e)} ===", exc_info=True)
        raise

if __name__ == "__main__":
    load_user_data()