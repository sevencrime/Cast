# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime
from datetime import timedelta

import requests
import psycopg2
import pandas as pd
from pandas.testing import assert_frame_equal

# prod
conn = psycopg2.connect(database='vap',user='postgres',password='Buzhongyao123',host='pgm-uf6e9e82v7d4ivxa117430.pg.rds.aliyuncs.com',port='1921')

# dev
# conn = psycopg2.connect(database='vap',user='postgres',password='Buzhongyao123',host='postgres.develop.meetwhale.com',port='5432')
cur = conn.cursor()


def get_hourly_chime(stamp, step=0):
    # 时间戳按分钟取整
    # step: 往前或往后跳跃取整值，默认为0，即当前所在的时间，正数为往后，负数往前
    dt = datetime.fromtimestamp(int(stamp))
    td = timedelta(days=0, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=-step, hours=0, weeks=0)
    new_dt = dt - td
    # timestamp = new_dt.timestamp()  # 对于 python 3 可以直接使用 timestamp 获取时间戳
    timestamp = (new_dt - datetime.fromtimestamp(0)).total_seconds()  # Python 2 需手动换算
    return int(timestamp)

def get_cookies(cookie):
    cookies={}  #初始化cookies字典变量
    for line in cookie.split(';'):   #按照字符：进行划分读取
        #其设置为1就会把字符串拆分成2份
        name, value = line.strip().split('=',1)
        cookies[name] = value  #为字典cookies添加内容

    return cookies

def searchDicKV(dic, keyword):
    if isinstance(dic, dict):
        for x in range(len(dic)):
            temp_key = list(dic.keys())[x]
            temp_value = dic[temp_key]
            if temp_key == keyword:
                return_value = temp_value
                return return_value
            return_value = searchDicKV(temp_value, keyword)
            if return_value != None:
                return return_value
    elif isinstance(dic, list):
        for d in dic:
            return searchDicKV(d, keyword)

def assert_info(merge_df, col_x, col_y):
    # 输出差异数据
    try:
        diff_df = merge_df[merge_df[col_x] != merge_df[col_y]][['timestamp', 'room_id', col_x, col_y]]
        assert len(diff_df) == 0
    except AssertionError:
        print(f"{col_x} 和 {col_y} 的查询数据：")
        print("######" * 30)
        print(diff_df)
        print(diff_df['room_id'].unique())
        print("######" * 30)
        raise AssertionError


cookie = '''
_tea_utm_cache_4031=undefined; _tea_utm_cache_4499=undefined; ttcid=6e0319ad055042038f7370f71366683f15; MONITOR_WEB_ID=76aa9794-d071-4973-bc23-f359f74578d1; MONITOR_DEVICE_ID=f9dd6a74-b80a-44f9-8013-4c4dd3cee9dd; MONITOR_WEB_ID=27d5f820-b346-44ad-adff-149e6c8f600e; passport_csrf_token=cb372d69ca859108b27d80aa98389116; passport_csrf_token_default=cb372d69ca859108b27d80aa98389116; x-jupiter-uuid=16526684611454382; s_v_web_id=verify_l3844z9m_peQ8GHT8_7iAY_429M_ALAY_UCYhI1yZUIEm; _tea_utm_cache_2018=undefined; ttwid=1%7CxjwmNWAxhfqhaIZsAcPS278b2p1Iz50Bu_vwsI3SgqU%7C1652670345%7C45062c146813b531f2b5e53ee9adf3bee73b98e562469fe3af39f336483ff713; d_ticket=6d9356e33f7361c3c99b360d0963702539626; n_mh=xbx-dGjhA61PpJqTn5s4gkjLhRXxfdvZabxGLOdkTlA; passport_auth_status=6a06e8d4de446b7e941dfef4b890be25%2C85b456b529e0722946a9ebcaad51c820; passport_auth_status_ss=6a06e8d4de446b7e941dfef4b890be25%2C85b456b529e0722946a9ebcaad51c820; sso_auth_status=a326d1404190108509a26b72169415d0; sso_auth_status_ss=a326d1404190108509a26b72169415d0; sso_uid_tt=e19c310e53ff9d1ef35531547bd9a82b; sso_uid_tt_ss=e19c310e53ff9d1ef35531547bd9a82b; toutiao_sso_user=e4b0bd367571b6464265b18391438bfe; toutiao_sso_user_ss=e4b0bd367571b6464265b18391438bfe; sid_ucp_sso_v1=1.0.0-KDU2OTI5MDAzZmIxNDM5NDdhNzhlNjYwYmQ3NDM5MTU2ODNhMjQwYjMKHwi7sKC46o2GBRCv_4aUBhiwISAMMJHiqY8GOAJA8QcaAmxmIiBlNGIwYmQzNjc1NzFiNjQ2NDI2NWIxODM5MTQzOGJmZQ; ssid_ucp_sso_v1=1.0.0-KDU2OTI5MDAzZmIxNDM5NDdhNzhlNjYwYmQ3NDM5MTU2ODNhMjQwYjMKHwi7sKC46o2GBRCv_4aUBhiwISAMMJHiqY8GOAJA8QcaAmxmIiBlNGIwYmQzNjc1NzFiNjQ2NDI2NWIxODM5MTQzOGJmZQ; odin_tt=cf7c639cd7d44391833ef0955d0dc10a083115532c7cb2cdc4ce5e8cac1abcebbb84bf8ba6fc45cbd2a90da40c4255e8e0184390d446f9043f01f21174353083; ucas_sso_c0=CkEKBTEuMC4wEIeIiMb49O_AYhjmJiDsydDR4YzcAiiwITC7sKC46o2GBUCz_4aUBkizs8OWBlCIvKnEuMesnmJYbxIU4BRbGSyInLGTdiv-MEx5GXDNE6Q; ucas_sso_c0_ss=CkEKBTEuMC4wEIeIiMb49O_AYhjmJiDsydDR4YzcAiiwITC7sKC46o2GBUCz_4aUBkizs8OWBlCIvKnEuMesnmJYbxIU4BRbGSyInLGTdiv-MEx5GXDNE6Q; ucas_c0=CkEKBTEuMC4wEIyIhr6K9O_AYhjmJiDsydDR4YzcAiiwITC7sKC46o2GBUC0_4aUBki0s8OWBlCIvKnEuMesnmJYbxIU3r6ygK-gGbYX4_lK1zMkfi-6ESs; ucas_c0_ss=CkEKBTEuMC4wEIyIhr6K9O_AYhjmJiDsydDR4YzcAiiwITC7sKC46o2GBUC0_4aUBki0s8OWBlCIvKnEuMesnmJYbxIU3r6ygK-gGbYX4_lK1zMkfi-6ESs; sid_guard=1fe9c6968e5234dd86709d536b81350c%7C1652670388%7C5184000%7CFri%2C+15-Jul-2022+03%3A06%3A28+GMT; uid_tt=b5158a59530f896700bf946743523016; uid_tt_ss=b5158a59530f896700bf946743523016; sid_tt=1fe9c6968e5234dd86709d536b81350c; sessionid=1fe9c6968e5234dd86709d536b81350c; sessionid_ss=1fe9c6968e5234dd86709d536b81350c; sid_ucp_v1=1.0.0-KDk5M2FlY2FhM2RjOWU4MjMzNjYwYWM3MDgwZGI2ZjRlYjA0ZGVhM2QKFwi7sKC46o2GBRC0_4aUBhiwITgCQPEHGgJobCIgMWZlOWM2OTY4ZTUyMzRkZDg2NzA5ZDUzNmI4MTM1MGM; ssid_ucp_v1=1.0.0-KDk5M2FlY2FhM2RjOWU4MjMzNjYwYWM3MDgwZGI2ZjRlYjA0ZGVhM2QKFwi7sKC46o2GBRC0_4aUBhiwITgCQPEHGgJobCIgMWZlOWM2OTY4ZTUyMzRkZDg2NzA5ZDUzNmI4MTM1MGM; LUOPAN_DT=session_7098165687864066317; msToken=rsXjerWiAXxLnBT_2tKPvehJ6GjqYM21D03FOrl7B5EXcRmkrOfQCgc8yLv-unvvWZZvhHaGwYrs287rjSJ4q9CO6iXiGY2woBMdnT9IrfTSPwF-J2HCSw==; csrftoken=I0TkUZqH7tnqn9ddkmdJ9ygg; tt_scid=rkv5x37Bj9aHdHJWsX2iXHO-OrNEGDwtlPgE07Cq.bAhhK1zX1qW2woykHAT4.Nqd8fd
'''
cookies = get_cookies(cookie)

headers = {
    'content-type': 'application/json',
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36"
}


# 互动表爬虫：数据库 doudian_compass_reptile_interaction 表， 接口在互动分组下面
def interaction():
    url = "https://compass.jinritemai.com/business_api/shop/live_room/content/trend_analysis"

    body = {
        "live_room_id": "7099432268078304008",
        "start_ts": "1652965409",
        "end_ts": "1652982079",
        "index_selected": "online_user_cnt,watch_ucnt,leave_ucnt,comment_cnt,like_cnt,share_cnt,send_gift_cnt,incr_fans_cnt,fans_club_ucnt",
        "_lid": "633413455966",
        "_signature": "_02B4Z6wo00901OokwQgAAIDDfvKaB.dJD.jqIMWAAFgf1cg61EEBZIb8iWb7-lr0mR9hAHfBs5wo0h8r0oFidhabfXrlpCeoyn6ePr.SKzQ2ch-DGig1J0AW2MbK6KspzlNmOCcZoQwQR0iT0f"
    }

    resp = requests.get(url, params=body, headers=headers, cookies=cookies).json()

    resp_data = resp["data"]["trends"]
    # print(resp_data)

    interaction_list = []
    step = 9                # 接口返回的每9个一组
    for i in range(0, len(resp_data), step):
        _map = {}
        for x in resp_data[i:i+step]:
            _map[x["point_name"]] = x["vertical"]
            _map["timestamp"] = x["date_time"]
        # print(_map)
        interaction_list.append(_map)


    df1 = pd.DataFrame(interaction_list)
    print(df1)
    df1_col = ",".join(df1.columns.values.tolist())
    # print(df1.iloc[-1].tolist())

    # # 查询sql
    sql = f"SELECT {df1_col} FROM doudian_compass_reptile_interaction WHERE room_id='{body['live_room_id']}' AND timestamp BETWEEN {get_hourly_chime(body['start_ts'])} AND {get_hourly_chime(body['end_ts'])} order by timestamp"
    print(sql)

    df2 = pd.read_sql(sql, conn)
    print(df2)

    assert len(df1) == len(df2)
    assert_frame_equal(df1, df2)


# doudian_compass_reptile_explain  解说表， 讲解, 页面在综合趋势分析
def explain():
    url = "https://compass.jinritemai.com/business_api/shop/live_room/flow/trend_analysis_point"
    body = {
        "live_room_id": "7099432268078304008",
        "cargo_author_type": "1",
        "_lid": "634244518300",
        "_signature": "_02B4Z6wo00901qvengwAAIDBPwjFAT9fj2Kr2pqAAMhz1cg61EEBZIb8iWb7-lr0mR9hAHfBs5wo0h8r0oFidhabfXrlpCeoyn6ePr.SKzQ2ch-DGig1J0AW2MbK6KspzlNmOCcZoQwQR0iT34"
    }

    resp = requests.get(url, params=body, headers=headers, cookies=cookies)
    # print(resp.json())
    resp_data = resp.json()
    result_list = []
    total_sum = 0
    for point in resp_data["data"]["points"]:
        result_dict = {}
        if point.get("explain"):
            # print(point)
            result_dict["room_id"] = body["live_room_id"]
            result_dict["timestamp"] = searchDicKV(point, "date_time")
            result_dict["explain_time"] = searchDicKV(point, "explain_time")
            result_dict["prod_name"] = searchDicKV(point, "prod_name")
            result_dict["gmv"] = searchDicKV(point, "gmv").replace("¥","").replace(",","")
            # 判断金额存不存在小数位
            # assert result_dict["gmv"].find(".") == -1
            result_dict["gmv"] = int(float(result_dict["gmv"]) * 100)            # 金额*100入库
            total_sum += result_dict["gmv"]

            # print(result_dict)
            result_list.append(result_dict)

    print(total_sum)


    df1 = pd.DataFrame(result_list)
    print(df1)
    df1_col = ",".join(df1.columns.values.tolist())
    print(df1.iloc[-1].tolist())

    # # 查询sql
    sql = f"SELECT {df1_col} FROM doudian_compass_reptile_explain WHERE room_id = '{body['live_room_id']}' ORDER BY timestamp LIMIT 100000"
    print(sql)

    df2 = pd.read_sql(sql, conn)
    print(df2)

    # pd.options.display.max_columns = None

    assert len(df1) == len(df2)
    assert_frame_equal(df1, df2)

    #################### 下面是输出差异数据 #######################
    data_new = pd.merge(df1, df2, how='outer', on=['room_id', 'timestamp'])
    # print(data_new)
    assert_info(data_new, 'gmv_x', 'gmv_y')
    assert_info(data_new, 'explain_time_x', 'explain_time_y')
    assert_info(data_new, 'prod_name_x', 'prod_name_y')



# doudian_compass_reptile_flow_source  流量来源表, 页面在流量来源趋势
def flow_source():
    # 根据片段时间对比
    url = "https://compass.jinritemai.com/business_api/shop/live_room/flow_analysis/flow_source"
    body = {
        "start_ts": "1650887722",
        "end_ts": "1650905995",
        "index_selected": "自然流量,短视频引流,直播广场,活动页,搜索,推荐feed,抖音商城推荐,头条西瓜,同城,其他推荐场景,其他,关注,个人主页&店铺&橱窗,付费流量,小店随心推,品牌广告,千川品牌广告,千川PC版,其他广告",
        "live_room_start_ts": "1650887722",
        "live_room_end_ts": "1650905995",
        "live_room_id": "7090508668550007559",
        "app_id": "1128",
        "_lid": "791459254205",
        "_signature": "_02B4Z6wo00901SoyxXAAAIDCvuSefpC4qhEqNsHAACgF07QG9sSFXq0aDiC3jTNX0Kj3HFR3XJFOalArMsgEnrxvbD.mBC98haao1aZrTGuPiBzR9YNufXs7er5IjevLkhteZTtQ8WDCB6tWf4"
    }
    resp = requests.get(url, params=body, headers=headers, cookies=cookies).json()
    # print(resp['data']['trends'])
    resp_data = resp['data']['trends']

    trends_list = []
    step = len(resp['data']['index_selected'])
    for i in range(0, len(resp_data), step):
        for x in resp_data[i:i+step]:
            _map = {}
            _map["room_id"] = body["live_room_id"]
            _map["timestamp"] = x['date_time']
            _map['num'] = x['vertical']
            _map['point_name'] = x['point_name']

            trends_list.append(_map)

    assert len(trends_list) == len(resp_data)
    df1 = pd.DataFrame(trends_list)
    print(df1)
    df1_col = ",".join(df1.columns.values.tolist())
    # print(df1.iloc[-1].tolist())

    # # 查询sql
    sql = f"SELECT {df1_col} FROM doudian_compass_reptile_flow_source WHERE room_id='{body['live_room_id']}' AND timestamp BETWEEN {get_hourly_chime(body['start_ts'])} AND {get_hourly_chime(body['end_ts'])} order by timestamp"
    print(sql)

    df2 = pd.read_sql(sql, conn)
    print(df2)

    assert len(df1) == len(df2)
    assert_frame_equal(df1, df2)



# doudian_compass_reptile_goods_shelves 商品上架, 页面：综合趋势分析中的上架
def goods_shelves():
    url = "https://compass.jinritemai.com/business_api/shop/live_room/flow/trend_analysis_point"
    body = {
        "live_room_id": "7094036493617892132",
        "cargo_author_type": "1",
        "_lid": "819036492265",
        "_signature": "_02B4Z6wo00f01an9YfQAAIDCPSs6-RrOsjGp-WVAAAjyqJFD-lLmabba9iOCsiEUOrw-5LiqgEypdyc7NZ7qZNh7FkVNdQWBEtZa53PzOKRDX1lTgJth3WU8CS8MBONnYmVm7Jy0mnL9S2iZ81"
    }

    rst_list = []
    resp = requests.get(url, params=body, headers=headers, cookies=cookies).json()
    # print(resp)
    for point in resp["data"]["points"]:
        if not point.get("explain"):
            # print(point)
            for prod_name in searchDicKV(point, "prod_name"):
                _prod_dict = {}
                _prod_dict["room_id"] = body["live_room_id"]
                _prod_dict["timestamp"] = point["date_time"]
                _prod_dict["prod_name"] = prod_name
                _prod_dict["other_num"] = searchDicKV(point, "other_num") or 0
                print(_prod_dict)
                rst_list.append(_prod_dict)

    df1 = pd.DataFrame(rst_list)
    print(df1)
    df1_col = ",".join(df1.columns.values.tolist())
    # print(df1.iloc[-1].tolist())

    # # 查询sql
    sql = f"SELECT {df1_col} FROM doudian_compass_reptile_goods_shelves WHERE room_id = '{body['live_room_id']}' LIMIT 1000;"
    print(sql)

    df2 = pd.read_sql(sql, conn)
    print(df2)

    assert len(df1) == len(df2)
    assert_frame_equal(df1, df2)




# doudian_compass_reptile_transaction_amount  交易信息, 第一个接口：综合分析， 分钟汇总：商品曝光中的商品讲解趋势图
def transaction_amount():
    url = "https://compass.jinritemai.com/business_api/shop/live_room/flow/trend_analysis"
    body = {
        "index_selected": "online_user_cnt,watch_ucnt,min_gmv",
        "live_room_id": "7099432268078304008",
        "_lid": "624217810306",
        "_signature": "_02B4Z6wo00f01QaINMAAAIDCkl5vzWJWPQkGjDBAACM16Lktuv4RLBk-4X8TntAs7fd2UpvsitVZn7nbe6xDODIAi-23VkxAK3QplbTldDafC8vb99P-RB3KE7fGGD74cxQj0EzxgxcCtzOE3e"
    }
    resp = requests.get(url, params=body, headers=headers, cookies=cookies).json()
    # print(resp)

    total_url = "https://compass.jinritemai.com/business_api/shop/live_room/product/room_trend"
    total_body = {
        "live_room_id": "7099432268078304008",
        "_lid": "624745635827",
        "_signature": "_02B4Z6wo00901PS6AjgAAIDDYGxZNA0E5DD0vgaAAF.E1cg61EEBZIb8iWb7-lr0mR9hAHfBs5wo0h8r0oFidhabfXrlpCeoyn6ePr.SKzQ2ch-DGig1J0AW2MbK6KspzlNmOCcZoQwQR0iT6b"
    }
    t_resp = requests.get(total_url, params=total_body, headers=headers, cookies=cookies).json()

    rst_list = []
    for trade in resp["data"]["trade_trend"]:
        _map = {}
        # print(trade)
        _map["timestamp"] = trade["date_time"]
        _map["min_gmv"] = trade["right_vertical"]       # 返回的单位就是分
        _map["room_id"] = body["live_room_id"]
        # print(_map)
        rst_list.append(_map)

    total_gmv_list = []
    for total_i in t_resp["data"]["trade_trend"]:
        _t_map = {}
        # print(total_i)
        _t_map['timestamp'] = total_i["date_time"]
        _t_map['pay_order_gmv'] = int(total_i["right_vertical"] * 100)       # 单位元转成分
        total_gmv_list.append(_t_map)

    df_gmv = pd.DataFrame(rst_list)
    df_total_gmv = pd.DataFrame(total_gmv_list)

    # 两个接口合并成完整的数据
    df1 = pd.merge(df_gmv, df_total_gmv, how='outer', on=['timestamp'])
    print(df1)
    print(df1.columns.values)


    df1_col = ",".join(df1.columns.values.tolist())

    # # 查询sql
    sql = f"SELECT {df1_col} FROM doudian_compass_reptile_transaction_amount WHERE room_id = '{body['live_room_id']}' LIMIT 100000"
    print(sql)

    df2 = pd.read_sql(sql, conn)
    print(df2)

    assert len(df1) == len(df2)
    # assert_frame_equal(df1, df2)

    #################### 下面是输出差异数据 #######################
    data_new = pd.merge(df1, df2, how='outer', on=['room_id', 'timestamp'])
    print(data_new)
    print(data_new.columns.values)

    assert_info(data_new, 'min_gmv_x', 'min_gmv_y')
    assert_info(data_new, 'pay_order_gmv_x', 'pay_order_gmv_y')



if __name__ == "__main__":
    # interaction()
    # explain()
         # flow_source()
         # goods_shelves()  业务上不使用
    transaction_amount()

    conn.commit()
    cur.close()
    conn.close()
