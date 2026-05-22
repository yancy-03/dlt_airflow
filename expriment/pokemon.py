import dlt
import requests
import logging
from typing import Dict, Iterator, Any
# 基础配置
BASE_URL = "https://pokeapi.co/api/v2/"

# 通用请求函数（和你业务代码风格一致）

def unpack_nested_array(data: Dict[str, Any], nested_key: str) -> Iterator[Dict[str, Any]]:
    base_data = data.copy()
    nested_list = base_data.pop(nested_key, [])
    for item in nested_list:
        yield {**base_data, nested_key: item}

@dlt.source(name='pokemon_source')
def pokemon_source():
    def get_page_response(endpoint, params=None, method="GET"):
        url = f"{BASE_URL}{endpoint.lstrip('/')}"
        if method == "GET":
            resp = requests.get(url, params=params)
        resp.raise_for_status()
        yield resp.json()

# ==============================================
# 一级资源：获取宝可梦ID列表
# ==============================================
    @dlt.resource(name="pokemon_list",parallelized=False)
    def get_pokemon_list():
        # 调用PokeAPI列表接口，获取所有宝可梦名称/ID
        for item in get_page_response("pokemon", params={"limit": 5}): 
            for result in item["results"]:
                yield {
                    "pokemon_id": result["url"].split("/")[-2],
                    "pokemon_name": result["name"]
                }
    pokemon_list = get_pokemon_list()
    
    @dlt.resource(name="pokemon_encounter_methods", parallelized=False)
    def get_pokemon_encounter_methods(inputs):
        for item in inputs:
            pokemon_id = item["pokemon_id"]
            pokemon_name = item["pokemon_name"]
            try:
                # 调用正确端点：/pokemon/{id}/encounters
                for response in get_page_response(f"pokemon/{pokemon_id}/encounters"):
                    # 直接遍历response数组（每个元素是location_area对象）
                    for location_area in response:
                        location_name = location_area["location_area"]["name"]
                        methods = set()  # 用于去重
                        
                        # 遍历version_details（不同游戏版本）
                        for version_detail in location_area.get("version_details", []):
                            # 遍历encounter_details（encounter记录）
                            for encounter_detail in version_detail.get("encounter_details", []):
                                method_name = encounter_detail["method"]["name"]
                                methods.add(method_name)
                        
                        # 输出location_area和对应的method.name（去重后）
                        for method in methods:
                            yield {
                                "pokemon_id": pokemon_id,
                                "pokemon_name": pokemon_name,
                                "location_area": location_name,
                                "encounter_method": method
                            }
            except Exception as e:
                logging.warning(f"宝可梦 {pokemon_id} 无 encounter 数据: {e}")
            
    pokemon_encounter_methods = get_pokemon_encounter_methods(inputs=pokemon_list)


    @dlt.resource(name="pokemon_encounter_methods_info", parallelized=False)
    def get_pokemon_encounter_info(inputs):
        encounter_method_set = set()  # 用于去重
        for item in inputs:
            encounter_method = item["encounter_method"]
            encounter_method_set.add(encounter_method)
        for encounter_method in encounter_method_set:
            try:
                # 遇见该宝可梦精灵的方法
                for response in get_page_response(f"encounter-method/{encounter_method}"):

                    response["encounter_method"] = encounter_method

                    for sub_unpack in unpack_nested_array(response, "names"):
                        if sub_unpack["names"]["language"]["name"] != "en":
                            continue
                        # yield sub_unpack
                        yield {
                            "encounter_method": sub_unpack["encounter_method"],
                            "describe_en": sub_unpack["names"]["name"],
                            "language_name": sub_unpack["names"]["language"]["name"],
                            "language__url": sub_unpack["names"]["language"]["url"],
                        }
            except Exception as e:
                logging.warning(f"宝可梦 {encounter_method} 无出没数据: {e}")

    pokemon_encounter_info = get_pokemon_encounter_info(inputs = pokemon_encounter_methods)


    return [pokemon_list, 
            pokemon_encounter_methods,
            pokemon_encounter_info,
    ]

source = pokemon_source()

def load_pokemon_source():
    pipeline = dlt.pipeline(
        pipeline_name="pokemon_source_demo",
        destination="sqlalchemy",
        dataset_name="pokemon_source_data",
        progress="log"
    )

    load_info = pipeline.run(pokemon_source())
    print(load_info)

if __name__ == "__main__":
    load_pokemon_source()