# pip install "dlt[sqlalchemy]"
import logging
from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)
# 全局日志配置（控制台+文件持久化）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("dlt_github_pipeline.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("GitHub_DLT_Pipeline")

@dlt.source(name="github")
def github_source(access_token: Optional[str] = dlt.secrets["rest_api_github"]['access_token']) -> Any:
    # Create a REST API configuration for the GitHub API
    # Use RESTAPIConfig to get autocompletion and type checking
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.github.com/repos/dlt-hub/dlt/",
            "auth": (
                {
                    "type": "bearer",
                    "token": access_token,
                }
                if dlt.secrets["rest_api_github"]['access_token']
                else None
            ),
        },
        # The default configuration for all resources and their endpoints
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "per_page": 100,
                },
            },
        },
        "resources": [
            {
                "name": "issues",
                "endpoint": {
                    "path": "issues",
                    # Query parameters for the endpoint
                    "params": {
                        "sort": "updated",
                        "direction": "desc",
                        "state": "open",
                        #增量
                        # "since": "{incremental.start_value}",
                    },
                    ##增量incremental
                    # "incremental": {
                    #     "cursor_path": "updated_at",
                    #     "initial_value": pendulum.today().subtract(days=30).to_iso8601_string(),
                    # },
                },
            },

            {
                "name": "issue_comments",
                "endpoint": {
                    "path": "issues/{resources.issues.number}/comments",
                },
                # Include data from `id` field of the parent resource
                # in the child data. The field name in the child data
                # will be called `_issues_id` (_{resource_name}_{field_name})
                "include_from_parent": ["id"],
            },
        ],
    }

    yield from rest_api_resources(config)




def load_github() -> None:
    logger.info("=== 启动 GitHub 数据同步流水线 ===")
    
    try:
        # 【核心修改】添加 progress="log" 参数
        pipeline = dlt.pipeline(
            pipeline_name="rest_api_github",
            destination='sqlalchemy',
            dataset_name="my_data",
            progress="log"  # DLT官方进度日志监控器
        )

        load_info = pipeline.run(github_source())
        
        logger.info("=== GitHub 数据同步完成 ===")
        logger.info(f"加载结果：\n{load_info}")
        print(load_info)

    except Exception as e:
        logger.error(f"=== GitHub 数据同步失败：{str(e)} ===", exc_info=True)
        raise


# def load_pokemon(base_url: str = "https://pokeapi.co/api/v2/") -> None:
#     pipeline = dlt.pipeline(
#         pipeline_name="rest_api_pokemon",
#         destination='sqlalchemy',
#         dataset_name="rest_api_data",
#     )

#     pokemon_source = rest_api_source(
#         {
#             "client": {
#                 "base_url": base_url,
#                 # If you leave out the paginator, it will be inferred from the API:
#                 # "paginator": "json_link",
#             },
#             "resource_defaults": {
#                 "endpoint": {
#                     "params": {
#                         "limit": 1000,
#                     },
#                 },
#             },
#             "resources": [
#                 "pokemon",
#                 "berry",
#                 "location",
#             ],
#         },
#         name="pokemon",
#     )

#     def check_network_and_authentication() -> None:
#         (can_connect, error_msg) = check_connection(
#             pokemon_source,
#             "not_existing_endpoint",
#         )
#         if not can_connect:
#             pass  # do something with the error message

#     check_network_and_authentication()

#     load_info = pipeline.run(pokemon_source)
#     print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_github()
    # load_pokemon()
