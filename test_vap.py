# -*- coding: utf-8 -*-

import time
import datetime
import copy
import traceback
import logging
import os
import shutil
import glob
import json
import math

import pytest
import allure
import psycopg2
import pandas as pd
import numpy as np
import requests
from pandas.testing import assert_frame_equal




@pytest.fixture(scope='class')
def conn():
    # ########################### prod #############################
    # conn = psycopg2.connect(database='vap',user='postgres',password='Buzhongyao123',host='pgr-uf62b66442e382djvo.pg.rds.aliyuncs.com',port='5432')

    # ########################### dev ##############################
    conn = psycopg2.connect(database='vap',user='postgres',password='Buzhongyao123',host='postgres.develop.meetwhale.com',port='5432')

    cur = conn.cursor()
    yield conn
    conn.commit()
    cur.close()
    conn.close()

# @pytest.fixture(scope='class')
# def dev_conn():
#     # ########################### prod #############################
#     # conn = psycopg2.connect(database='vap',user='postgres',password='Buzhongyao123',host='pgr-uf62b66442e382djvo.pg.rds.aliyuncs.com',port='5432')

#     # ########################### dev ##############################
#     conn = psycopg2.connect(database='vap',user='postgres',password='Buzhongyao123',host='postgres.develop.meetwhale.com',port='5432')

#     cur = conn.cursor()
#     yield conn
#     conn.commit()
#     cur.close()
#     conn.close()




class Test_Vap_Data():
    overview_url = 'http://10.200.2.100:30441/vap/api/overview'      # dev
    # overview_url = 'http://10.168.0.103:31243/vap/api/overview'      # prod

    cast_api_dev = 'http://10.200.2.100:30441'
    cast_api_prod = ''

    logging.basicConfig(
                level=logging.DEBUG
                , format = '%(asctime)s %(levelname)s [line:%(lineno)d] %(message)s'
                # , filename='logger.log'
                # , filemode = 'w'
                )
    pd.set_option('display.width', 300)
    pd.set_option('display.max_columns', 8)
    pd.set_option('display.max_rows', None)


    def assert_info(self, merge_df, col_x, col_y, col_list=['room_id', 'event_time']):
        # 输出差异数据
        col = copy.deepcopy(col_list)
        col.append(col_x)
        col.append(col_y)

        try:
            if merge_df[col_x].dtypes == 'float64':
                # logging.debug(abs(merge_df[col_x] - merge_df[col_y]))
                diff_df = merge_df[abs(merge_df[col_x] - merge_df[col_y]) >= 0.01][col]
            else:
                diff_df = merge_df[merge_df[col_x] != merge_df[col_y]][col]
            assert len(diff_df) == 0
        except AssertionError:
            s_info =  traceback.extract_stack()
            logging.debug(f"{s_info[-2][2]} >>>, {col_x} 和 {col_y} 的差异数据：")
            logging.debug("######" * 10)
            logging.debug(f'\n {diff_df}')
            # 防止太长保存csv时导致精度丢失
            for key in diff_df.columns.values:
                diff_df[key] = diff_df[key].map(lambda x: str(x) + '\t')

            logging.debug("######" * 10)
            logging.debug("\n\n")
            # diff_df.to_csv('77777.csv')
            raise AssertionError


    def execute_sql_stime(self, sql, conn):
        '''
            pandas执行sql， 并输出时间
        '''
        start1 = time.time()
        df = pd.read_sql(sql, conn)
        ex_sql_t = time.time() - start1
        if ex_sql_t >= 1:
            logging.debug(f'\n {sql}')
        logging.debug(f'sql查询时间：{ex_sql_t}')

        return df


    @allure.feature("验证实时流")
    def test_check_vap_douyin_live_rooms_flow_real(self, conn):
        # 要以zhengti的直播间为准
        # 数据对比， 前一天的数据， 第二天下午已经校准了，需要看看能不能判断
        pg_sql_1 = '''
            SELECT
                A.start_time
                , A.end_time
                , to_char(to_timestamp(A.end_time), 'YYYY-MM-DD HH24:MI:SS') AS s_end_time
                , A.douyin_no
                , A.title
                , A.room_id AS room_id_all
                , B.*
            FROM vap_douyin_live_rooms A
            LEFT JOIN vap_douyin_live_rooms_flow B ON A.room_id = B.room_id
            WHERE start_time > floor(extract(epoch from date_trunc('day', current_date))) - 3600 * 24
                AND end_time > 0
            ORDER BY A.start_time
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)
        print(df1)
        room_id_list = tuple(df1['room_id_all'].tolist())
        print(room_id_list)

        pg_sql_2 = f'''
            WITH zhenti AS (
                SELECT * FROM (
                    SELECT room_id
                        , gpm
                        , watch_ucnt
                        , follow_anchor_ucnt AS add_fans
                        , (CASE WHEN watch_ucnt = 0 THEN 0 ELSE follow_anchor_ucnt/watch_ucnt::NUMERIC END) AS add_fans_rate
                        , interact_watch_ucnt_ratio AS interactive_rate
                        , fans_club_join_ucnt AS add_club_num
                        , (CASE WHEN watch_ucnt = 0 THEN 0 ELSE fans_club_join_ucnt/watch_ucnt::NUMERIC END) AS add_club_rate
                        , gmv AS pay_amt
                        , watch_pay_ucnt_ratio
                        , follow_anchor_watch_ucnt_ratio AS attention_rate
                        , DENSE_RANK() OVER(PARTITION BY room_id ORDER BY "timestamp" DESC) AS rank1
                    FROM doudian_compass_screen_zhengti
                    WHERE room_id IN {room_id_list}
                ) t WHERE rank1 = 1
            ),
            room_real_time AS (
                SELECT * FROM (
                    SELECT room_id
                        , like_cnt AS like_num
                        , comment_cnt AS comment_num
                        , product_show_click_ucnt_ratio AS click_payrate
                        , DENSE_RANK() OVER(PARTITION BY room_id ORDER BY "timestamp" DESC) AS rank1
                    FROM doudian_compass_reptile_room_real_time
                    WHERE room_id IN {room_id_list}
                ) t WHERE rank1 = 1
            ),
            reptile_interaction AS (
                SELECT SUM(share_cnt) AS share_num, room_id
                FROM doudian_compass_reptile_interaction
                WHERE room_id IN {room_id_list}
                GROUP BY room_id
            )
            SELECT
                A.room_id AS room_id_all
                , gpm
                , watch_ucnt
                , like_num
                , share_num
                , comment_num
                , add_fans
                , round(add_fans_rate, 4) AS add_fans_rate
                , add_club_num
                , round(add_club_rate, 4) AS add_club_rate
                , interactive_rate
                , pay_amt
                , watch_pay_ucnt_ratio
                , click_payrate
                , attention_rate
            FROM reptile_interaction A
            LEFT JOIN zhenti B ON A.room_id = B.room_id
            LEFT JOIN room_real_time C ON A.room_id = C.room_id

        '''

        df2 = self.execute_sql_stime(pg_sql_2, conn)

        m_df = pd.merge(df1, df2, how='outer', on=['room_id_all'])
        print(m_df)
        print(m_df.columns.values)
        m_df['room_id_all'] = m_df['room_id_all'].map(lambda x: str(x) + '\t')
        m_df.to_csv("111.csv")

        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'watch_ucnt_x', 'watch_ucnt_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'like_num_x', 'like_num_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'share_num_x', 'share_num_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'comment_num_x', 'comment_num_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'add_fans_x', 'add_fans_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'add_fans_rate_x', 'add_fans_rate_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'add_club_num_x', 'add_club_num_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'add_club_rate_x', 'add_club_rate_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'interactive_rate_x', 'interactive_rate_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'pay_amt_x', 'pay_amt_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'watch_pay_ucnt_ratio_x', 'watch_pay_ucnt_ratio_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'click_payrate_x', 'click_payrate_y', col_list=['room_id', 's_end_time', 'title'])
        self.assert_info(m_df, 'attention_rate_x', 'attention_rate_y', col_list=['room_id', 's_end_time', 'title'])







    @allure.feature("验证T+1批处理")
    def test_check_vap_douyin_live_rooms_offline_batch(self, conn):
        pg_sql_result = '''
            SELECT * FROM vap_douyin_live_rooms_offline WHERE room_id NOT IN ('demo_data_finish_1', 'demo_finish')
        '''
        df_result = self.execute_sql_stime(pg_sql_result, conn)
        print(df_result)

        pg_sql_1 = '''
            WITH reptile_room AS (
                    SELECT
                            room_id
                            , round(gpm/100::NUMERIC, 2) AS gpm
                            , watch_ucnt
                            , like_cnt AS like_num
                            , comment_cnt AS comment_num
                            , interact_ratio AS interactive_rate
                            , pay_amt
                            , watch_pay_ucnt_ratio
                            , product_show_click_ucnt_ratio AS click_payrate
                            , attention_ratio AS attention_rate
                            , to_char(to_timestamp(update_time), 'YYYY-MM-DD HH24:MI:SS') AS update_time
                    FROM doudian_compass_reptile_room
            ),
            reptile_inter AS (
                    SELECT
                            room_id AS room_id_1
                             , SUM(share_cnt) AS share_num
                             , SUM(incr_fans_cnt) AS add_fans
                             , SUM(fans_club_ucnt) AS add_club_num
                    FROM doudian_compass_reptile_interaction
                    GROUP BY room_id
            ),
            live_rooms AS (
                SELECT A.room_id AS room_id_2
                    , end_time AS end_time_stamp
                    , to_char(to_timestamp(start_time), 'YYYY-MM-DD HH24:MI:SS') AS start_time
                    , to_char(to_timestamp(end_time), 'YYYY-MM-DD HH24:MI:SS') AS end_time
                FROM vap_douyin_live_rooms A
                WHERE end_time > 0
                ORDER BY A.start_time
            )
            -- 用reptile_room表关联，有爬到的就更新数据， 没爬到的用实时的数据
            SELECT *
                , round((CASE WHEN watch_ucnt = 0 THEN 0 ELSE add_fans/watch_ucnt::NUMERIC END), 4) AS add_fans_rate
                , round((CASE WHEN watch_ucnt = 0 THEN 0 ELSE add_club_num/watch_ucnt::NUMERIC END ), 4) AS add_club_rate
            FROM reptile_room A
            LEFT JOIN reptile_inter B ON A.room_id = B.room_id_1
            LEFT JOIN live_rooms C ON A.room_id = C.room_id_2

        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)
        print(df1)


        m_df = pd.merge(df1, df_result, how='outer', on=['room_id'])
        # m_df = pd.merge(df1, df_result, how='right', on=['room_id'])
        print(m_df)
        print(m_df.columns.values)

        # 取结束时间小于前一天的数据。昨天结束的直播，今天下午2点爬T+1的数据，第二天凌晨跑批脚步更新数据
        m_df = m_df[m_df['end_time_stamp'] <= (int(time.mktime(datetime.date.today().timetuple())) - 3600*24)]
        # m_df['room_id'] = m_df['room_id'].map(lambda x: str(x) + '\t')
        m_df.to_csv("111.csv")

        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'watch_ucnt_x', 'watch_ucnt_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'like_num_x', 'like_num_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'share_num_x', 'share_num_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'comment_num_x', 'comment_num_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'add_fans_x', 'add_fans_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'add_fans_rate_x', 'add_fans_rate_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'add_club_num_x', 'add_club_num_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'add_club_rate_x', 'add_club_rate_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'interactive_rate_x', 'interactive_rate_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'pay_amt_x', 'pay_amt_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'watch_pay_ucnt_ratio_x', 'watch_pay_ucnt_ratio_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'click_payrate_x', 'click_payrate_y', col_list=['room_id', 'end_time_x', 'update_time'])
        self.assert_info(m_df, 'attention_rate_x', 'attention_rate_y', col_list=['room_id', 'end_time_x', 'update_time'])




    @allure.feature("验证product_info实时流")
    def test_check_doudian_compass_live_product_info_real(self, conn):
        pg_sql_1 = '''
            SELECT
                A.start_time
                , A.end_time
                , A.douyin_no
                , A.title
                , A.room_id AS room_id_all
                , B.*
            FROM vap_douyin_live_rooms A
            LEFT JOIN vap_douyin_live_rooms_flow B ON A.room_id = B.room_id
            WHERE start_time > floor(extract(epoch from date_trunc('day', current_date))) - 3600 * 24
                AND end_time > 0
            ORDER BY A.start_time
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)
        print(df1)
        # 直播结束的所有直播
        print(df1['room_id_all'].tolist())

        for k, v in df1.groupby(['room_id_all']):
            room_id = k
            print(room_id)
            pg_sql_2 = f'''
                SELECT id, douyin_no, room_id, product_id, gmv
                FROM doudian_compass_live_product_info
                WHERE room_id = '{room_id}'
                ORDER BY product_id
            '''

            df_product_info = self.execute_sql_stime(pg_sql_2, conn)
            # print("doudian_compass_live_product_info 结果表数据\n", df_product_info)

            pg_sql_3 = f'''
                WITH tmp AS (
                        SELECT douyin_no
                                , room_id
                                , product_id
                                , gmv
                                , timestamp
                                , DENSE_RANK() OVER(PARTITION BY room_id, product_id ORDER BY "timestamp" DESC) AS d_rank
                        FROM doudian_compass_screen_product
                        WHERE room_id = '{room_id}'
                        ORDER BY "timestamp" DESC
                )
                SELECT * FROM tmp WHERE d_rank = 1 ORDER BY product_id

            '''

            df_real = self.execute_sql_stime(pg_sql_3, conn)
            # print("爬虫表计算的结果\n", df_real)

            m_df = pd.merge(df_product_info, df_real, how='outer', on=['room_id', 'product_id', 'douyin_no'])
            # print(m_df)
            # print(m_df.columns.values)

            self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['room_id', 'product_id'])





    def test_time_period_target(self, conn):
        pg_sql_1 = '''
            SELECT
                live_id
                , anchor_id
                , period_start_time
                , period_start_second
                , to_char(to_timestamp(update_time), 'YYYY-MM-DD HH24:MI:SS') AS update_time
                , CAST(targets::jsonb->>'gmv' AS float) AS gmv
                , CAST(targets::jsonb->>'gpm' AS float) AS gpm
                , CAST(targets::jsonb->>'add_fans' AS float) AS add_fans
                , CAST(targets::jsonb->>'like_cnt' AS float) AS like_cnt
                , CAST(targets::jsonb->>'share_cnt' AS float) AS share_cnt
                , CAST(targets::jsonb->>'lives_time' AS float) AS lives_time
                , CAST(targets::jsonb->>'comment_cnt' AS float) AS comment_cnt
                , CAST(targets::jsonb->>'add_fans_club' AS float) AS add_fans_club
                , CAST(targets::jsonb->>'add_fans_rate' AS float) AS add_fans_rate
                , CAST(targets::jsonb->>'interactive_rate' AS float) AS interactive_rate
                , CAST(targets::jsonb->>'live_watch_count' AS float) AS live_watch_count
                , CAST(targets::jsonb->>'add_fans_club_rate' AS float) AS add_fans_club_rate
                , CAST(targets::jsonb->>'product_click_rate' AS float) AS product_click_rate
                , CAST(targets::jsonb->>'transaction_conversion_rate' AS float) AS transaction_conversion_rate
            FROM vap_anchor_live_time_period_offline
            WHERE is_delete = 0
            ORDER BY live_id DESC
        '''
        df_offline = self.execute_sql_stime(pg_sql_1, conn)
        print(df_offline)

        pg_sql_2 = '''
            WITH anchor_live_period AS (
                SELECT
                    room_id
                    , live_id
                    , anchor_id
                    , a_name
                    , period_start_time
                    , period_start_second
                    , update_time
                    -- 偏移函数, 和on_start_time组合成时间段
                    , lead(period_start_second, 1, end_time) over(PARTITION BY live_id ORDER BY period_start_second) AS on_end_time
                FROM (
                    SELECT
                        A.room_id
                        , B.live_id
                        -- 直播间开始时间和结束时间戳按分钟取整
                        , A.start_time - (A.start_time % 60) AS start_time
                        , A.end_time - (A.end_time % 60) AS end_time
                        , B.anchor_id
                        , B.update_time
                        , vap_anchors."name" AS a_name
                        , B.period_start_time
                        -- 主播上播的时间戳, 业务为分钟级别
                        , B.period_start_second - (B.period_start_second % 60) AS period_start_second
                    FROM vap_douyin_live_rooms A
                    INNER JOIN vap_anchor_live_time_period B ON B.live_id = A.id
                    INNER JOIN vap_anchors ON vap_anchors.id = B.anchor_id
                    WHERE B.is_delete = 0 AND vap_anchors.is_delete = 0
                ) t1
            ),
            min_anchor AS (
                SELECT
                    A.*
                    , B.timestamp
                    , B.min_gmv
                    -- 直播时间段秒数
                    , on_end_time - period_start_second AS period_time
                    , ROW_NUMBER() OVER(PARTITION BY timestamp ORDER BY period_start_time desc) AS srank
                FROM anchor_live_period A
                INNER JOIN doudian_compass_reptile_transaction_amount B
                ON A.room_id = B.room_id AND B."timestamp" >= A.period_start_second AND B."timestamp" < A.on_end_time
            ),
            min_anchor_period_info AS (
                -- 关联需要用到的数据,每一分钟
                SELECT
                    A.*
                    , B.watch_ucnt
                    , B.gpm
                    , D.product_click_watch_ucnt_ratio
                    , D.pay_live_show_ucnt_ratio
                    , B.interact_watch_ucnt_ratio
                    , (CASE WHEN C.online_user_cnt = 0 THEN 0 ELSE C.incr_fans_cnt / C.online_user_cnt::numeric END) AS add_fans_rate
                    , (CASE WHEN C.online_user_cnt = 0 THEN 0 ELSE C.fans_club_ucnt / C.online_user_cnt::numeric END) AS add_fans_club_rate
                    , C.like_cnt
                    , C.incr_fans_cnt
                    , C.comment_cnt
                    , C.share_cnt
                    , C.fans_club_ucnt
                FROM min_anchor A
                LEFT JOIN doudian_compass_screen_zhengti B ON A.room_id = B.room_id AND A.timestamp = B.timestamp
                LEFT JOIN doudian_compass_reptile_interaction C ON A.room_id = C.room_id AND A.timestamp = C.timestamp
                LEFT JOIN doudian_compass_reptile_room_diagnosis_flow_trend D ON A.room_id = D.room_id AND A.timestamp = D.timestamp
            )
            -- 单场直播所有主播的数据
            SELECT
                room_id
                , live_id
                , anchor_id
                , a_name
                , period_start_time
                , SUM(min_gmv) AS gmv
                , floor(AVG(NULLIF(floor(gpm), 0))) AS gpm
                , SUM(incr_fans_cnt) AS add_fans
                , SUM(like_cnt) AS like_cnt
                , SUM(share_cnt) AS share_cnt
                , period_time/60 AS lives_time
                , SUM(comment_cnt) AS comment_cnt
                , SUM(fans_club_ucnt) AS add_fans_club
                , AVG(add_fans_rate)*100 AS add_fans_rate
                , AVG(NULLIF(interact_watch_ucnt_ratio, 0))*100 AS interactive_rate
                , MAX(watch_ucnt) - MIN(watch_ucnt) AS live_watch_count
                , AVG(add_fans_club_rate)*100 AS add_fans_club_rate
                , AVG(NULLIF(product_click_watch_ucnt_ratio, 0)) AS product_click_rate
                , AVG(NULLIF(pay_live_show_ucnt_ratio, 0)) AS transaction_conversion_rate
            FROM min_anchor_period_info
            GROUP BY room_id, anchor_id, period_start_time, period_time, a_name, live_id
            ORDER BY period_start_time

        '''
        df_result = self.execute_sql_stime(pg_sql_2, conn)
        print(df_result)


        m_df = pd.merge(df_offline, df_result, how='outer', on=['live_id', 'anchor_id', 'period_start_time'])
        print(m_df)
        print(m_df.columns.values)

        # m_df.to_csv("111.csv")

        self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'add_fans_x', 'add_fans_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'like_cnt_x', 'like_cnt_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'share_cnt_x', 'share_cnt_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'lives_time_x', 'lives_time_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'comment_cnt_x', 'comment_cnt_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'add_fans_club_x', 'add_fans_club_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'add_fans_rate_x', 'add_fans_rate_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'interactive_rate_x', 'interactive_rate_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'live_watch_count_x', 'live_watch_count_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'add_fans_club_rate_x', 'add_fans_club_rate_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'product_click_rate_x', 'product_click_rate_y', col_list=['live_id', 'anchor_id', 'period_start_time'])
        self.assert_info(m_df, 'transaction_conversion_rate_x', 'transaction_conversion_rate_y', col_list=['live_id', 'anchor_id', 'period_start_time'])



    def test_time_period_products(self, conn):

        # 查询vap_anchor_live_time_period_offline-products下的字段
        pg_sql_product = f'''
            SELECT
                live_id
                , anchor_id
                , period_start_time
                , period_start_second
                , json_array_elements(products::json)::json->>'product_name' AS topic
                , CAST(json_array_elements(products::json)::json->>'gmv' AS float) AS gmv
                , CAST(json_array_elements(products::json)::json->>'product_id' AS bigint) AS topic_analysis_id
                , json_array_elements(products::json)::json->>'price' AS price
                , json_array_elements(products::json)::json->>'product_url' AS topic_img
            FROM vap_anchor_live_time_period_offline
            WHERE is_delete = 0

        '''

        df_offline_product = self.execute_sql_stime(pg_sql_product, conn)
        print(df_offline_product)


        pg_sql_1 = '''
            WITH anchor_live_period AS (
                SELECT
                    room_id
                    , douyin_no
                    , live_id
                    , company_id
                    , anchor_id
                    , a_name
                    , period_start_time
                    , period_start_second
                    -- 偏移函数, 和on_start_time组合成时间段
                    , lead(period_start_second, 1, end_time) over(PARTITION BY live_id ORDER BY period_start_second) AS on_end_time
                FROM (
                    SELECT
                        A.room_id
                        , A.douyin_no
                        , B.live_id
                        , B.company_id
                        -- 直播间开始时间和结束时间戳按分钟取整
                        , A.start_time - (A.start_time % 60) AS start_time
                        , A.end_time - (A.end_time % 60) AS end_time
                        , B.anchor_id
                        , vap_anchors."name" AS a_name
                        , B.period_start_time
                        -- 主播上播的时间戳, 业务为分钟级别
                        , B.period_start_second - (B.period_start_second % 60) AS period_start_second
                    FROM vap_douyin_live_rooms A
                    INNER JOIN vap_anchor_live_time_period B ON B.live_id = A.id
                    INNER JOIN vap_anchors ON vap_anchors.id = B.anchor_id
                    WHERE B.is_delete = 0 AND vap_anchors.is_delete = 0
                ) t1
            ),
            min_anchor AS (
                -- 分组获取每个product_id 的汇总金额
                SELECT product_id, live_id, anchor_id, period_start_time, A.douyin_no, SUM(disperse_gmv) AS gmv, room_id, company_id
                FROM anchor_live_period A
                INNER JOIN doudian_compass_live_product_min_process B
                    ON A.room_id = B.live_room_id AND B."timestamp" >= A.period_start_second AND B."timestamp" < A.on_end_time
                WHERE B.disperse_gmv > 0
                GROUP BY product_id, live_id, anchor_id, period_start_time, A.douyin_no, A.room_id, company_id
            ),
            product_price AS (
                -- 获取商品的最大价格和最小价格
                SELECT * FROM (
                    SELECT A.*, B.product_name, B.price
                    , MIN(price) OVER(partition by B.product_id) AS min_price
                    , MAX(price) OVER(partition by B.product_id) AS max_price
                    FROM min_anchor A
                    RIGHT JOIN doudian_compass_live_product_info B ON A.room_id = B.room_id AND A.product_id = B.product_id
                    WHERE B.price > 0
                ) t WHERE live_id IS NOT NULL
            )
            SELECT *
            FROM (
                SELECT *
                    , ROW_NUMBER() OVER(partition by live_id, anchor_id, period_start_time ORDER BY gmv DESC) AS gmv_rank
                FROM (
                    -- 关联vap_topic_analysis获取id，topic等数据。以vap_topic_analysis为主
                    SELECT A.live_id, A.product_id, A.douyin_no, A.period_start_time, A.anchor_id, A.gmv
                        , B.id AS topic_analysis_id, B.topic, B.topic_img, B.create_time
                        -- 重复时，按A.product_id, A.douyin_no, A.anchor_id, A.period_start_time分组，以create_time取最新
                        , ROW_NUMBER() OVER(partition by A.product_id, A.douyin_no, A.anchor_id, A.period_start_time ORDER BY create_time DESC) AS s_rank
                        -- 组合成price区间
                        , (
                            CASE WHEN min_price = max_price
                            THEN round(min_price/100::NUMERIC, 2)::varchar
                            ELSE round(min_price/100::NUMERIC, 2)::varchar || ' ~ ' || round(max_price/100::NUMERIC, 2)::varchar
                            END) AS price
                    FROM product_price A
                    RIGHT JOIN vap_topic_analysis B ON A.product_id = B.product_id AND A.douyin_no = B.channel_no AND A.company_id = B.company_id
                    WHERE A.live_id IS NOT NULL
                ) t WHERE s_rank = 1
            )t1 WHERE gmv_rank <= 10
            order by period_start_time, gmv


        '''

        df_result = self.execute_sql_stime(pg_sql_1, conn)
        print(df_result)

        m_df = pd.merge(df_offline_product, df_result, how='outer', on=['live_id', 'anchor_id', 'period_start_time', 'topic'])
        print(m_df)
        print(m_df.dtypes)
        print(m_df.columns.values)


        self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['live_id', 'anchor_id', 'period_start_time', 'topic'])
        self.assert_info(m_df, 'topic_analysis_id_x', 'topic_analysis_id_y', col_list=['live_id', 'anchor_id', 'period_start_time', 'topic'])
        self.assert_info(m_df, 'price_x', 'price_y', col_list=['live_id', 'anchor_id', 'period_start_time', 'topic'])
        self.assert_info(m_df, 'topic_img_x', 'topic_img_y', col_list=['live_id', 'anchor_id', 'period_start_time', 'topic'])




    def test_check_vap_anchor_target(self, conn):
        pg_sql_1 = '''
            SELECT
                company_id
                , id AS anchor_id
                , CAST(targets::json->>'add_fans' AS float) AS add_fans
                , CAST(targets::json->>'like_cnt' AS float) AS like_cnt
                , CAST(targets::json->>'share_cnt' AS float) AS share_cnt
                , CAST(targets::json->>'comment_cnt' AS float) AS comment_cnt
                , CAST(targets::json->>'add_fans_club' AS float) AS add_fans_club
                , lives_count
                , lives_time
                , live_ids
                , gmv
                , gpm
                , live_watch_count
                , product_click_rate
                , transaction_conversion_rate
                , interactive_rate
                , add_fans_rate
                , add_fans_club_rate
            FROM vap_anchors_offline

        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        pg_sql_2 = '''
            WITH period_traget AS (
                SELECT
                    live_id
                    , anchor_id
                    , period_start_time
                    , period_start_second
                    , company_id
                    , CAST(targets::json->>'lives_time' AS float) AS lives_time
                    , CAST(targets::json->>'gmv' AS float) AS gmv
                    , CAST(targets::json->>'gpm' AS float) AS gpm
                    , CAST(targets::json->>'add_fans' AS float) AS add_fans
                    , CAST(targets::json->>'like_cnt' AS float) AS like_cnt
                    , CAST(targets::json->>'share_cnt' AS float) AS share_cnt
                    , CAST(targets::json->>'lives_time' AS float) AS live_time
                    , CAST(targets::json->>'comment_cnt' AS float) AS comment_cnt
                    , CAST(targets::json->>'add_fans_club' AS float) AS add_fans_club
                    , CAST(targets::json->>'add_fans_rate' AS float) AS add_fans_rate
                    , CAST(targets::json->>'interactive_rate' AS float) AS interactive_rate
                    , CAST(targets::json->>'live_watch_count' AS float) AS live_watch_count
                    , CAST(targets::json->>'add_fans_club_rate' AS float) AS add_fans_club_rate
                    , CAST(targets::json->>'product_click_rate' AS float) AS product_click_rate
                    , CAST(targets::json->>'transaction_conversion_rate' AS float) AS transaction_conversion_rate
                FROM vap_anchor_live_time_period_offline
                WHERE is_delete = 0 AND period_start_second >= floor(extract(epoch from date_trunc('day', now()))) - 3600*24*30
                ORDER BY live_id DESC
            )
            SELECT
                company_id
                , anchor_id
                , count(DISTINCT live_id) AS lives_count
                , SUM(live_time) AS lives_time
                , array_agg(DISTINCT(live_id)) AS live_ids
                , SUM(gmv) AS gmv
                , floor(AVG(NULLIF(floor(gpm), 0))) AS gpm
                , SUM(live_watch_count) AS live_watch_count
                , CAST(AVG(NULLIF(product_click_rate, 0))::NUMERIC AS DECIMAL(20, 2)) AS product_click_rate
                , CAST(AVG(NULLIF(transaction_conversion_rate, 0))::NUMERIC AS DECIMAL(20, 2)) AS transaction_conversion_rate
                , CAST(AVG(NULLIF(interactive_rate, 0))::NUMERIC AS DECIMAL(20, 2)) AS interactive_rate
                , CAST(AVG(NULLIF(add_fans_rate, 0))::NUMERIC AS DECIMAL(20, 2)) AS add_fans_rate
                , CAST(AVG(NULLIF(add_fans_club_rate, 0))::NUMERIC AS DECIMAL(20, 2)) AS add_fans_club_rate
                , SUM(add_fans) AS add_fans
                , SUM(like_cnt) AS like_cnt
                , SUM(share_cnt) AS share_cnt
                , SUM(comment_cnt) AS comment_cnt
                , SUM(add_fans_club) AS add_fans_club
            FROM period_traget
            GROUP BY company_id, anchor_id
            ORDER BY anchor_id

        '''
        df2 = self.execute_sql_stime(pg_sql_2, conn)
        print(df2)

        m_df = pd.merge(df1, df2, how='outer', on=['company_id', 'anchor_id'])
        print(m_df)
        # live_ids为Nan的替换成{}格式
        m_df['live_ids_y'] = m_df['live_ids_y'].fillna('{}')
        # 转pandas后一个是list，一个是str格式。转字符串后取出来空格保持类型一致
        m_df['live_ids_x'] = m_df['live_ids_x'].map(lambda x: '{}' if x == '{}' else x.replace(" ", ""))
        m_df['live_ids_y'] = m_df['live_ids_y'].map(lambda x: '{}' if x == '{}' else str(x).replace(" ", ""))

        # 对Nan的数据补0
        m_df.fillna(0, inplace = True)
        print(m_df.columns.values)

        self.assert_info(m_df, 'lives_count_x', 'lives_count_y', col_list=['company_id', 'anchor_id'])
        self.assert_info(m_df, 'lives_time_x', 'lives_time_y', col_list=['company_id', 'anchor_id'])
        self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['company_id', 'anchor_id'])
        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['company_id', 'anchor_id'])
        self.assert_info(m_df, 'live_watch_count_x', 'live_watch_count_y', col_list=['company_id', 'anchor_id'])
        self.assert_info(m_df, 'product_click_rate_x', 'product_click_rate_y', col_list=['company_id', 'anchor_id'])
        self.assert_info(m_df, 'transaction_conversion_rate_x', 'transaction_conversion_rate_y', col_list=['company_id', 'anchor_id'])
        self.assert_info(m_df, 'interactive_rate_x', 'interactive_rate_y', col_list=['company_id', 'anchor_id'])
        self.assert_info(m_df, 'add_fans_rate_x', 'add_fans_rate_y', col_list=['company_id', 'anchor_id'])
        self.assert_info(m_df, 'add_fans_club_rate_x', 'add_fans_club_rate_y', col_list=['company_id', 'anchor_id'])
        self.assert_info(m_df, 'live_ids_x', 'live_ids_y', col_list=['company_id', 'anchor_id'])


    # 要确定跑脚本的时间，拿当天0点去判断有点误差
    def test_check_vap_anchor_products(self, conn):
        pg_sql_1 = '''
            SELECT
                company_id
                , id AS anchor_id
                , CAST(json_array_elements(products::json)::json->>'gmv' AS float) * 100 AS gmv
                , json_array_elements(products::json)::json->>'price' AS price
                , CAST(json_array_elements(products::json)::json->>'rank' AS bigint) AS rank
                , CAST(json_array_elements(products::json)::json->>'topic_analysis_id' AS bigint) AS topic_analysis_id
                , CAST( (json_array_elements(products::json)::json->>'product_entity')::json->>'id' AS bigint) AS e_id
                , (json_array_elements(products::json)::json->>'product_entity')::json->>'name' AS e_name
                , (json_array_elements(products::json)::json->>'product_entity')::json->>'price' AS e_price
                , (json_array_elements(products::json)::json->>'product_entity')::json->>'image_url' AS image_url
            FROM vap_anchors_offline
            WHERE is_delete = 0
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)


        pg_sql_2 = '''
            SELECT * FROM (
                SELECT *
                    , ROW_NUMBER() OVER(PARTITION BY company_id, anchor_id ORDER BY gmv DESC) AS rank
                FROM (
                    SELECT DISTINCT anchor_id
                        , company_id
                        , topic
                        -- 汇总gmv
                        , SUM(gmv) OVER(PARTITION BY anchor_id, topic) AS gmv
                        -- , topic_analysis_id
                        , price
                        , topic_img
                    FROM (
                        -- 取出最近30天的数据
                        SELECT
                            live_id
                            , company_id
                            , anchor_id
                            , period_start_time
                            , period_start_second
                            , json_array_elements(products::json)::json->>'product_name' AS topic
                            , CAST(json_array_elements(products::json)::json->>'gmv' AS float) AS gmv
                            , CAST(json_array_elements(products::json)::json->>'product_id' AS bigint) AS topic_analysis_id
                            , json_array_elements(products::json)::json->>'price' AS price
                            , json_array_elements(products::json)::json->>'product_url' AS topic_img
                        FROM vap_anchor_live_time_period_offline
                        WHERE is_delete = 0 AND period_start_second >= floor(extract(epoch from date_trunc('day', now()))) - 3600*24*30 - 8*3600
                    ) t
                ) t1
            ) t2 WHERE rank <= 10

        '''

        df2 = self.execute_sql_stime(pg_sql_2, conn)
        print(df2)

        m_df = pd.merge(df1, df2, how='outer', on=['company_id', 'anchor_id', 'rank'])
        print(m_df)
        print(m_df.dtypes)
        print(m_df.columns.values)

        m_df.to_csv("111.csv")

        self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['company_id', 'anchor_id', 'rank'])
        self.assert_info(m_df, 'price_x', 'price_y', col_list=['company_id', 'anchor_id', 'rank'])
        self.assert_info(m_df, 'topic_analysis_id_x', 'topic_analysis_id_y', col_list=['company_id', 'anchor_id', 'rank'])
        self.assert_info(m_df, 'image_url', 'topic_img', col_list=['company_id', 'anchor_id', 'rank'])
        self.assert_info(m_df, 'e_name', 'topic', col_list=['company_id', 'anchor_id', 'rank'])
        self.assert_info(m_df, 'price_x', 'e_price', col_list=['company_id', 'anchor_id', 'rank'])
        self.assert_info(m_df, 'topic_analysis_id_x', 'e_id', col_list=['company_id', 'anchor_id', 'rank'])



    def test_chenk_vap_topic_detail(self, conn):
        pg_sql_1 = '''
            SELECT
                company_id
                , channel_no AS douyin_no
                , topic_analysis_id
                , live_id
                , (SELECT topic FROM vap_topic_analysis_offline WHERE id = topic_analysis_id) AS prod_name
                , add_fans
                , add_club
                , vip_group_count
                , comments_count
                , likes_count
                , shares_count
                , rewards_count
                , interaction_rate
                , gpm
                , gmv
                , sales_amount
                , price
                , pay_order_count
                , watch_count
                , pay_click_rate
                , order_conversion_rate
                , add_club_rate
                , add_fans_rate
            FROM vap_topic_detail_offline
            ORDER BY live_id DESC

        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)


        for k, v in df1.groupby(['live_id']):
            live_id = k
            print(live_id)

            pg_sql_2 = f'''
                WITH reptile_explain AS (
                    SELECT *
                        , (start_time_stamp) AS s_time_stamp
                        , (end_time_stamp) AS e_time_stamp
                    FROM (
                            SELECT A.room_id, A.douyin_no, prod_name, prod_id, explain_time
                                    , (B.start_time - B.start_time % 60) AS start_time
                                    , (B.end_time - B.end_time % 60) AS end_time
                                    , "timestamp" AS start_time_stamp
                                    , LEAD("timestamp", 1, CAST(B.end_time AS integer)) OVER(PARTITION BY A.room_id ORDER BY "timestamp") AS end_time_stamp
                            FROM doudian_compass_reptile_explain A
                            INNER JOIN vap_douyin_live_rooms B ON A.room_id = B.room_id
                            WHERE B.id = {live_id}
                    )t
                ),
                minutes AS (
                     -- 获取解说表开始时间和结束时间，生成分钟时间序列
                    SELECT generate_series(s_time_stamp::BIGINT, e_time_stamp::BIGINT, 60) AS generate_stamp
                    FROM (
                            SELECT MIN(start_time) AS s_time_stamp, MAX(end_time) AS e_time_stamp FROM reptile_explain
                        ) t
                 ),
                prod_explain AS (
                    -- 解说时间段每分钟
                    SELECT
                        M.room_id
                        , M.douyin_no
                        , M.prod_name
                        , M.prod_id
                        , M.explain_time
                        , N.generate_stamp
                        , to_char(to_timestamp(N.generate_stamp), 'YYYY-MM-DD HH24:MI') AS generate_series
                    FROM reptile_explain M INNER JOIN (SELECT * FROM reptile_explain CROSS JOIN minutes ) N
                    ON M.explain_time = N.explain_time AND N.generate_stamp >= M.s_time_stamp AND N.generate_stamp <= M.e_time_stamp AND M.room_id = N.room_id
                    WHERE M.prod_name = N.prod_name
                ),
                duplication_pro_explain AS (
                -- 去重
                    SELECT DISTINCT room_id, douyin_no, prod_name, prod_id, generate_stamp, generate_series FROM prod_explain
                ),
                interactive AS (
                    -- 每一分钟数据汇总
                    SELECT
                        A.room_id
                        , A.douyin_no
                        , A.prod_name
                        , A.prod_id
                        , (CASE WHEN A.prod_id IS NULL THEN 0 ELSE SUM(B.incr_fans_cnt) END) AS 加粉数
                        , (CASE WHEN A.prod_id IS NULL THEN 0 ELSE SUM(B.comment_cnt) END) AS 评论数
                        , (CASE WHEN A.prod_id IS NULL THEN 0 ELSE SUM(B.like_cnt) END) AS 点赞数
                        , (CASE WHEN A.prod_id IS NULL THEN 0 ELSE SUM(B.share_cnt) END) AS 分享数
                        , (CASE WHEN A.prod_id IS NULL THEN 0 ELSE SUM(B.send_gift_cnt) END) AS 打赏数
                        , (CASE WHEN A.prod_id IS NULL THEN 0 ELSE SUM(B.fans_club_ucnt) END) AS 新加团人数
                        , (CASE WHEN A.prod_id IS NULL THEN 0 ELSE AVG(C.interact_watch_ucnt_ratio) END) AS 互动率
                        , (CASE WHEN A.prod_id IS NULL THEN 0 ELSE AVG((CASE WHEN B.online_user_cnt = 0 THEN 0 ELSE B.incr_fans_cnt / B.online_user_cnt::numeric END)) END) AS 加粉率
                        , (CASE WHEN A.prod_id IS NULL THEN 0 ELSE AVG((CASE WHEN B.online_user_cnt = 0 THEN 0 ELSE B.fans_club_ucnt / B.online_user_cnt::numeric END)) END) AS 加团率
                    FROM duplication_pro_explain A
                    LEFT JOIN doudian_compass_reptile_interaction B ON A.room_id = B.room_id AND A.generate_stamp = B.timestamp
                    LEFT JOIN doudian_compass_screen_zhengti C ON A.room_id = C.room_id AND A.generate_stamp = C.timestamp
                    GROUP BY A.prod_name, A.room_id, A.douyin_no, A.prod_id
                ),
                screen_product AS (
                    -- gpm和成交转化率直接拿doudian_compass_screen_product的数据
                    SELECT
                        A.product_id
                        , AVG(NULLIF(A.gpm, 0)) AS gpm
                        , SUM(A.product_show_pay_ucnt_ratio) AS product_show_pay_ucnt_ratio
                        , A.room_id
                    FROM doudian_compass_screen_product A
                    WHERE room_id = (SELECT room_id FROM reptile_explain LIMIT 1)
                    GROUP BY A.room_id, A.product_id
                ),
                product_info AS (
                    SELECT
                        A.room_id
                        , A.douyin_no
                        , A.product_name AS prod_name
                        , A.product_id
                        , A.gmv AS 成交金额
                        , product_show_ucnt AS 曝光人数
                        , product_show_click_ucnt_ratio AS 商品点击率
                        , B.gpm
                        , (CASE WHEN A.product_show_ucnt = 0 THEN 0 ELSE A.pay_ucnt/A.product_show_ucnt::NUMERIC END ) AS 成交转换率
                        , A.sales_amount AS 成交件数
                        , A.refund_cnt AS 退单数
                        , A.price
                        , A.pay_cnt
                    FROM doudian_compass_live_product_info A
                    LEFT JOIN screen_product B ON A.product_id = B.product_id AND A.room_id = B.room_id
                    WHERE A.room_id = (SELECT room_id FROM reptile_explain LIMIT 1)
                )
                SELECT
                    A.douyin_no
                    , A.prod_name
                    , A.prod_id
                    , (CASE WHEN A.prod_id IS NULL THEN 0 ELSE COUNT(*) END) AS 直播场次
                    , round(SUM(成交金额), 2) AS gmv
                    , SUM(加粉数) AS add_fans
                    , SUM(评论数) AS comments_count
                    , SUM(点赞数) AS likes_count
                    , SUM(分享数) AS shares_count
                    , SUM(打赏数) AS rewards_count
                    , SUM(新加团人数) AS add_club
                    , SUM(新加团人数) AS vip_group_count
                    , SUM(B.曝光人数) AS watch_count
                    , round(AVG(gpm)::numeric, 2) AS gpm
                    , round(AVG(商品点击率), 4) AS pay_click_rate
                    , CAST(AVG(成交转换率) AS DECIMAL(20,3)) AS order_conversion_rate
                    , round(SUM(互动率), 4) AS interaction_rate
                    , round(SUM(加粉率), 4) AS add_fans_rate
                    , round(SUM(加团率), 4) AS add_club_rate
                    , SUM(成交件数) AS sales_amount
                    , SUM(B.price)*100 AS price
                    , SUM(pay_cnt) AS pay_order_count
                FROM interactive A
                LEFT JOIN product_info B ON A.room_id = B.room_id AND A.prod_id = B.product_id
                GROUP BY A.douyin_no, A.prod_name, A.prod_id
                ORDER BY gmv DESC
            '''

            df2 = self.execute_sql_stime(pg_sql_2, conn)

            m_df = pd.merge(v, df2, how='outer', on=['douyin_no', 'prod_name'])
            # print(m_df)
            # print(m_df.columns.values)
            m_df.fillna(0, inplace = True)


            self.assert_info(m_df, 'add_fans_x', 'add_fans_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'add_club_x', 'add_club_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'vip_group_count_x', 'vip_group_count_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'comments_count_x', 'comments_count_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'likes_count_x', 'likes_count_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'shares_count_x', 'shares_count_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'rewards_count_x', 'rewards_count_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'interaction_rate_x', 'interaction_rate_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'sales_amount_x', 'sales_amount_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'price_x', 'price_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'pay_order_count_x', 'pay_order_count_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'watch_count_x', 'watch_count_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'pay_click_rate_x', 'pay_click_rate_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'order_conversion_rate_x', 'order_conversion_rate_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'add_club_rate_x', 'add_club_rate_y', col_list=['company_id', 'douyin_no', 'prod_name'])
            self.assert_info(m_df, 'add_fans_rate_x', 'add_fans_rate_y', col_list=['company_id', 'douyin_no', 'prod_name'])



    # 测试库vap_douyin_live_rooms与prod不一致，需要等上prod后再测试
    def test_check_topic_analysis(self, conn):
        pg_sql_1 = '''
            SELECT
                id,
                topic AS prod_name,
                channel_no AS douyin_no,
                add_fans,
                add_club,
                vip_group_count,
                comments_count,
                likes_count,
                shares_count,
                rewards_count,
                interaction_rate,
                gpm,
                gmv,
                sales_amount,
                price_range,
                pay_order_count,
                watch_count,
                pay_click_rate,
                order_conversion_rate,
                add_club_rate,
                add_fans_rate
            FROM vap_topic_analysis_offline
            WHERE channel_no <> 'demo'

        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        pg_sql_2 = '''
            SELECT
                topic_analysis_id AS id
                , prod_name
                , douyin_no
                , company_id
                , SUM(add_fans) AS add_fans
                , SUM(add_club) AS add_club
                , SUM(vip_group_count) AS vip_group_count
                , SUM(comments_count) AS comments_count
                , SUM(likes_count) AS likes_count
                , SUM(shares_count) AS shares_count
                , SUM(rewards_count) AS rewards_count
                , CAST(AVG(NULLIF(interaction_rate, 0)) AS DECIMAL(20, 4)) AS interaction_rate
                , CAST(AVG(NULLIF(gpm, 0)) AS DECIMAL(20, 2)) AS gpm
                , SUM(gmv) AS gmv
                , SUM(sales_amount) AS sales_amount
                , SUM(price) AS price
                , SUM(pay_order_count) AS pay_order_count
                , SUM(watch_count) AS watch_count
                , CAST(AVG(NULLIF(pay_click_rate, 0)) AS DECIMAL(20, 4) ) AS pay_click_rate
                , CAST(AVG(NULLIF(order_conversion_rate, 0)) AS DECIMAL(20, 4) ) AS order_conversion_rate
                , CAST(AVG(NULLIF(add_club_rate, 0)) AS DECIMAL(20, 4) ) AS add_club_rate
                , CAST(AVG(NULLIF(add_fans_rate, 0)) AS DECIMAL(20, 4) ) AS add_fans_rate
            FROM (
                SELECT
                    A.company_id
                    , A.channel_no AS douyin_no
                    , A.topic_analysis_id
                    , A.live_id
                    , (SELECT topic FROM vap_topic_analysis_offline WHERE id = topic_analysis_id) AS prod_name
                    , (SELECT start_time FROM vap_douyin_live_rooms WHERE id = A.live_id) AS start_time
                    , A.add_fans
                    , A.add_club
                    , A.vip_group_count
                    , A.comments_count
                    , A.likes_count
                    , A.shares_count
                    , A.rewards_count
                    , A.interaction_rate
                    , A.gpm
                    , A.gmv
                    , A.sales_amount
                    , A.price
                    , A.pay_order_count
                    , A.watch_count
                    , A.pay_click_rate
                    , A.order_conversion_rate
                    , A.add_club_rate
                    , A.add_fans_rate
                FROM vap_topic_detail_offline A
            ) t
            WHERE topic_analysis_id > 0 AND prod_name <> '' AND start_time >= floor(extract(epoch from date_trunc('day', now()))) - 3600*24*30
            GROUP BY topic_analysis_id, prod_name, douyin_no, company_id

        '''
        df2 = self.execute_sql_stime(pg_sql_2, conn)

        m_df = pd.merge(df1, df2, how='outer', on=['id', 'prod_name', 'douyin_no'])
        print(m_df)
        print(m_df.columns.values)
        m_df.fillna(0, inplace = True)


        self.assert_info(m_df, 'add_fans_x', 'add_fans_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'add_club_x', 'add_club_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'vip_group_count_x', 'vip_group_count_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'comments_count_x', 'comments_count_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'likes_count_x', 'likes_count_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'shares_count_x', 'shares_count_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'rewards_count_x', 'rewards_count_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'interaction_rate_x', 'interaction_rate_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'sales_amount_x', 'sales_amount_y', col_list=['id', 'prod_name', 'douyin_no'])
        # self.assert_info(m_df, 'price_range', '', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'pay_order_count_x', 'pay_order_count_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'watch_count_x', 'watch_count_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'pay_click_rate_x', 'pay_click_rate_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'order_conversion_rate_x', 'order_conversion_rate_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'add_club_rate_x', 'add_club_rate_y', col_list=['id', 'prod_name', 'douyin_no'])
        self.assert_info(m_df, 'add_fans_rate_x', 'add_fans_rate_y', col_list=['id', 'prod_name', 'douyin_no'])



    # 数据库和接口地址环境要对应
    def test_check_VAP_API(self, conn):

        api_start_time = 1677859200
        api_end_time = 1678377599
        api_douyin_no = 'wusu868688'


        room_ids_sql = f'''
            SELECT room_id
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY etl_flag DESC) AS s_rank
                    , to_char(to_timestamp(start_time), 'YYYY-MM-DD HH24:MI:SS') AS str_time
                FROM vap_review_room
            ) t WHERE s_rank = 1 AND douyin_no = '{api_douyin_no}'
                AND start_time >= {api_start_time}
                AND start_time <= {api_end_time}
        '''
        room_ids_df = self.execute_sql_stime(room_ids_sql, conn)
        room_ids = room_ids_df['room_id'].tolist()
        tup_room_ids = tuple(room_ids)
        if len(tup_room_ids) == 1:
            tup_room_ids = str(tup_room_ids).replace(',', '')


        room_ids_last_cycle_sql = f'''
            SELECT room_id
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY etl_flag DESC) AS s_rank
                    , to_char(to_timestamp(start_time), 'YYYY-MM-DD HH24:MI:SS') AS str_time
                FROM vap_review_room
            ) t WHERE s_rank = 1 AND douyin_no = '{api_douyin_no}'
                -- 环比区间的开始时间：start_time - (end_time - start_time) - 1
                -- 环比区间的结束时间：end_time - (end_time - start_time) - 1
                AND start_time >= {api_start_time} - ({api_end_time} - {api_start_time}) - 1
                AND start_time <= {api_end_time} - ({api_end_time} - {api_start_time}) - 1
        '''
        room_ids_last_cycle_df = self.execute_sql_stime(room_ids_last_cycle_sql, conn)
        room_ids_last_cycle = room_ids_last_cycle_df['room_id'].tolist()
        tup_room_ids_last_cycle = tuple(room_ids_last_cycle)
        if len(tup_room_ids_last_cycle) == 1:
            tup_room_ids_last_cycle = str(tup_room_ids_last_cycle).replace(',', '')


        body = {
            "douyin_no": api_douyin_no,
            "start_time": api_start_time,
            "end_time": api_end_time,
            "room_ids": room_ids,
            "room_ids_last_cycle": room_ids_last_cycle,
            "metrics": [
                "gmv",
                "gpm",
                "exposure_view_rate",
                "view_product_click_rate",
                "view_deal_order_rate",
                "watch_ucnt",
                "avg_watch_duration",
                "watch_cnt",
                "view_fans_rate",
                "view_clubs_rate",
                "view_interact_rate",
                "jl_form_submit_cnt",
                "jl_watch_comment_rate",
                "jl_watch_share_rate",
                "jl_watch_like_rate",
                "jl_card_show_click_rate",
                "jl_card_click_form_submit_rate",
                "jl_watch_ucnt_day",
                "jl_avg_watch_duration"
            ]
        }
        print(json.dumps(body))
        resp = requests.post(self.overview_url, json.dumps(body)).json()
        # print(resp)

        pg_sql_1 = f'''
            WITH review_room AS (
                SELECT *
                FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY etl_flag DESC) AS s_rank
                        , to_char(to_timestamp(start_time), 'YYYY-MM-DD HH24:MI:SS') AS str_time
                    FROM vap_review_room
                ) t WHERE s_rank = 1
            ),
            this_period AS (
                SELECT
                    douyin_no
                    , SUM(gmv) AS gmv
                    , round(AVG(NULLIF(gpm, 0)), 2) AS gpm
                    , round((CASE WHEN SUM(exposure_cnt) = 0 THEN 0 ELSE SUM(watch_cnt) / SUM(exposure_cnt)::NUMERIC END)*100, 2) AS exposure_view_rate
                    , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(product_click_cnt) / SUM(watch_cnt)::NUMERIC END)*100, 2) AS view_product_click_rate
                    , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(deal_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS view_deal_order_rate
                    , SUM(watch_ucnt) AS watch_ucnt
                    , round(AVG(avg_watch_duration)) AS avg_watch_duration
                    , SUM(watch_cnt) AS watch_cnt
                    , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(fans_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS fans_ucnt
                    , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(clubs_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS clubs_ucnt
                    , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(interact_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS view_interact_rate
                    , SUM(jl_form_submit_cnt) AS jl_form_submit_cnt
                    , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_comment_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_comment_rate
                    , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_share_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_share_rate
                    , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_like_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_like_rate
                    , round((CASE WHEN SUM(jl_card_show_cnt) = 0 THEN 0 ELSE SUM(jl_card_click_cnt)/SUM(jl_card_show_cnt)::NUMERIC END)*100, 2) AS jl_card_show_click_rate
                    , round((CASE WHEN SUM(jl_card_click_cnt) = 0 THEN 0 ELSE SUM(jl_form_submit_cnt)/SUM(jl_card_click_cnt)::NUMERIC END)*100, 2) AS jl_card_click_form_submit_rate
                    , round(SUM(jl_watch_ucnt) / SUM(day_num)::NUMERIC) AS jl_watch_ucnt_day
                    , round(AVG(jl_avg_watch_duration), 2) AS jl_avg_watch_duration
                FROM (
                    SELECT *, (CASE WHEN ROW_NUMBER() OVER(PARTITION BY to_char(to_timestamp(start_time), 'YYYY-MM-DD')) = 1 THEN 1 ELSE 0 END) AS day_num
                    FROM review_room
                    WHERE douyin_no = '{body['douyin_no']}'
                        AND room_id IN {tup_room_ids if len(tup_room_ids) > 0 else "(' ')"}
                        AND start_time >= {body['start_time']}
                        AND start_time <= {body['end_time']}
                ) t
                GROUP BY douyin_no
            ),
            last_period AS (
                SELECT
                    douyin_no
                    , SUM(gmv) AS gmv
                    , round(AVG(NULLIF(gpm, 0)), 2) AS gpm
                    , round((CASE WHEN SUM(exposure_cnt) = 0 THEN 0 ELSE SUM(watch_cnt) / SUM(exposure_cnt)::NUMERIC END)*100, 2) AS exposure_view_rate
                    , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(product_click_cnt) / SUM(watch_cnt)::NUMERIC END)*100, 2) AS view_product_click_rate
                    , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(deal_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS view_deal_order_rate
                    , SUM(watch_ucnt) AS watch_ucnt
                    , round(AVG(avg_watch_duration)) AS avg_watch_duration
                    , SUM(watch_cnt) AS watch_cnt
                    , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(fans_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS fans_ucnt
                    , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(clubs_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS clubs_ucnt
                    , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(interact_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS view_interact_rate
                    , SUM(jl_form_submit_cnt) AS jl_form_submit_cnt
                    , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_comment_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_comment_rate
                    , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_share_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_share_rate
                    , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_like_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_like_rate
                    , round((CASE WHEN SUM(jl_card_show_cnt) = 0 THEN 0 ELSE SUM(jl_card_click_cnt)/SUM(jl_card_show_cnt)::NUMERIC END)*100, 2) AS jl_card_show_click_rate
                    , round((CASE WHEN SUM(jl_card_click_cnt) = 0 THEN 0 ELSE SUM(jl_form_submit_cnt)/SUM(jl_card_click_cnt)::NUMERIC END)*100, 2) AS jl_card_click_form_submit_rate
                    , round(SUM(jl_watch_ucnt) / SUM(day_num)::NUMERIC) AS jl_watch_ucnt_day
                    , round(AVG(jl_avg_watch_duration), 2) AS jl_avg_watch_duration
                FROM (
                    SELECT *, (CASE WHEN ROW_NUMBER() OVER(PARTITION BY to_char(to_timestamp(start_time), 'YYYY-MM-DD')) = 1 THEN 1 ELSE 0 END) AS day_num
                    FROM review_room
                   WHERE douyin_no = '{body['douyin_no']}'
                        -- 环比区间的开始时间：start_time - (end_time - start_time) - 1
                        -- 环比区间的结束时间：end_time - (end_time - start_time) - 1
                        AND start_time >= {body['start_time']} - ({body['end_time']} - {body['start_time']}) -1
                        AND start_time <= {body['end_time']} - ({body['end_time']} - {body['start_time']}) -1
                        AND room_id IN {tup_room_ids_last_cycle if len(tup_room_ids_last_cycle) > 0 else "(' ')"}
                ) t
                GROUP BY douyin_no
            )
            SELECT
                A.douyin_no
                , round(A.gmv, 2) AS gmv
                , round(A.gpm, 2) AS gpm
                , round(A.exposure_view_rate, 2) AS exposure_view_rate
                , round(A.view_product_click_rate, 2) AS view_product_click_rate
                , round(A.view_deal_order_rate, 2) AS view_deal_order_rate
                , round(A.watch_ucnt, 2) AS watch_ucnt
                , round(A.avg_watch_duration, 2) AS avg_watch_duration
                , round(A.watch_cnt, 2) AS watch_cnt
                , round(A.fans_ucnt, 2) AS fans_ucnt
                , round(A.clubs_ucnt, 2) AS clubs_ucnt
                , round(A.view_interact_rate, 2) AS view_interact_rate
                , A.jl_form_submit_cnt
                , A.jl_watch_comment_rate
                , A.jl_watch_share_rate
                , A.jl_watch_like_rate
                , A.jl_card_show_click_rate
                , A.jl_card_click_form_submit_rate
                , A.jl_watch_ucnt_day
                , A.jl_avg_watch_duration
                -- 下面是diff
                , round(COALESCE(B.gmv, 0), 2) AS diff_gmv
                , round(COALESCE(B.gpm, 0), 2) AS diff_gpm
                , round(COALESCE(B.exposure_view_rate, 0), 2) AS diff_exposure_view_rate
                , round(COALESCE(B.view_product_click_rate, 0), 2) AS diff_view_product_click_rate
                , round(COALESCE(B.view_deal_order_rate, 0), 2) AS diff_view_deal_order_rate
                , round(COALESCE(B.watch_ucnt, 0), 2) AS diff_watch_ucnt
                , round(COALESCE(B.avg_watch_duration, 0), 2) AS diff_avg_watch_duration
                , round(COALESCE(B.watch_cnt, 0), 2) AS diff_watch_cnt
                , round(COALESCE(B.fans_ucnt, 0), 2) AS diff_fans_ucnt
                , round(COALESCE(B.clubs_ucnt, 0), 2) AS diff_clubs_ucnt
                , round(COALESCE(B.view_interact_rate, 0), 2) AS diff_view_interact_rate
                , round(COALESCE(B.jl_form_submit_cnt, 0), 2) AS diff_jl_form_submit_cnt
                , round(COALESCE(B.jl_watch_comment_rate, 0), 2) AS diff_jl_watch_comment_rate
                , round(COALESCE(B.jl_watch_share_rate, 0), 2) AS diff_jl_watch_share_rate
                , round(COALESCE(B.jl_watch_like_rate, 0), 2) AS diff_jl_watch_like_rate
                , round(COALESCE(B.jl_card_show_click_rate, 0), 2) AS diff_jl_card_show_click_rate
                , round(COALESCE(B.jl_card_click_form_submit_rate, 0), 2) AS diff_jl_card_click_form_submit_rate
                , round(COALESCE(B.jl_watch_ucnt_day, 0), 2) AS diff_jl_watch_ucnt_day
                , B.jl_avg_watch_duration AS diff_jl_avg_watch_duration
                -- 下面是计算环比的
                , round((CASE WHEN B.gmv = 0 THEN 0 ELSE (A.gmv - B.gmv) / B.gmv::NUMERIC * 100 END), 2) AS "compare_last_gmv"
                , round((CASE WHEN B.gpm = 0 THEN 0 ELSE (A.gpm - B.gpm) / B.gpm::NUMERIC * 100 END), 2) AS "compare_last_gpm"
                , round((CASE WHEN B.exposure_view_rate = 0 THEN 0 ELSE (A.exposure_view_rate - B.exposure_view_rate) / B.exposure_view_rate::NUMERIC * 100 END), 2) AS "compare_last_exposure_view_rate"
                , round((CASE WHEN B.view_product_click_rate = 0 THEN 0 ELSE (A.view_product_click_rate - B.view_product_click_rate) / B.view_product_click_rate::NUMERIC * 100 END), 2) AS "compare_last_view_product_click_rate"
                , round((CASE WHEN B.view_deal_order_rate = 0 THEN 0 ELSE (A.view_deal_order_rate - B.view_deal_order_rate) / B.view_deal_order_rate::NUMERIC * 100 END), 2) AS "compare_last_view_deal_order_rate"
                , round((CASE WHEN B.watch_ucnt = 0 THEN 0 ELSE (A.watch_ucnt - B.watch_ucnt) / B.watch_ucnt::NUMERIC * 100 END), 2) AS "compare_last_watch_ucnt"
                , round((CASE WHEN B.avg_watch_duration = 0 THEN 0 ELSE (A.avg_watch_duration - B.avg_watch_duration) / B.avg_watch_duration::NUMERIC * 100 END), 2) AS "compare_last_avg_watch_duration"
                , round((CASE WHEN B.watch_cnt = 0 THEN 0 ELSE (A.watch_cnt - B.watch_cnt) / B.watch_cnt::NUMERIC * 100 END), 2) AS "compare_last_watch_cnt"
                , round((CASE WHEN B.fans_ucnt = 0 THEN 0 ELSE (A.fans_ucnt - B.fans_ucnt) / B.fans_ucnt::NUMERIC * 100 END), 2) AS "compare_last_fans_ucnt"
                , round((CASE WHEN B.clubs_ucnt = 0 THEN 0 ELSE (A.clubs_ucnt - B.clubs_ucnt) / B.clubs_ucnt::NUMERIC * 100 END), 2) AS "compare_last_clubs_ucnt"
                , round((CASE WHEN B.view_interact_rate = 0 THEN 0 ELSE (A.view_interact_rate - B.view_interact_rate) / B.view_interact_rate::NUMERIC * 100 END), 2) AS "compare_last_view_interact_rate"
                , round((CASE WHEN B.jl_form_submit_cnt = 0 THEN 0 ELSE (A.jl_form_submit_cnt - B.jl_form_submit_cnt) / B.jl_form_submit_cnt::NUMERIC * 100 END), 2) AS "compare_last_jl_form_submit_cnt"
                , round((CASE WHEN B.jl_watch_comment_rate = 0 THEN 0 ELSE (A.jl_watch_comment_rate - B.jl_watch_comment_rate) / B.jl_watch_comment_rate::NUMERIC * 100 END), 2) AS "compare_last_jl_watch_comment_rate"
                , round((CASE WHEN B.jl_watch_share_rate = 0 THEN 0 ELSE (A.jl_watch_share_rate - B.jl_watch_share_rate) / B.jl_watch_share_rate::NUMERIC * 100 END), 2) AS "compare_last_jl_watch_share_rate"
                , round((CASE WHEN B.jl_watch_like_rate = 0 THEN 0 ELSE (A.jl_watch_like_rate - B.jl_watch_like_rate) / B.jl_watch_like_rate::NUMERIC * 100 END), 2) AS "compare_last_jl_watch_like_rate"
                , round((CASE WHEN B.jl_card_show_click_rate = 0 THEN 0 ELSE (A.jl_card_show_click_rate - B.jl_card_show_click_rate) / B.jl_card_show_click_rate::NUMERIC * 100 END), 2) AS "compare_last_jl_card_show_click_rate"
                , round((CASE WHEN B.jl_card_click_form_submit_rate = 0 THEN 0 ELSE (A.jl_card_click_form_submit_rate - B.jl_card_click_form_submit_rate) / B.jl_card_click_form_submit_rate::NUMERIC * 100 END), 2) AS "compare_last_jl_card_click_form_submit_rate"
                , round((CASE WHEN B.jl_watch_ucnt_day = 0 THEN 0 ELSE (A.jl_watch_ucnt_day - B.jl_watch_ucnt_day) / B.jl_watch_ucnt_day::NUMERIC * 100 END), 2) AS "compare_last_jl_watch_ucnt_day"
                , round((CASE WHEN B.jl_avg_watch_duration = 0 THEN 0 ELSE (A.jl_avg_watch_duration - B.jl_avg_watch_duration) / B.jl_avg_watch_duration::NUMERIC * 100 END), 2) AS "compare_last_jl_avg_watch_duration"
            FROM this_period A LEFT JOIN last_period B ON A.douyin_no = B.douyin_no


        '''

        print(pg_sql_1)
        df1 = self.execute_sql_stime(pg_sql_1, conn)
        df1.fillna(0, inplace = True)
        df1.to_csv("111.csv")


        # pg_sql_2 = f'''
        #     WITH review_room AS (
        #         SELECT *
        #         FROM (
        #             SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY etl_flag DESC) AS s_rank
        #                 , to_char(to_timestamp(start_time), 'YYYYMMDD') AS str_time
        #             FROM vap_review_room
        #         ) t WHERE s_rank = 1
        #     ),
        #     this_period AS (
        #         SELECT
        #             str_time AS date_time
        #             , douyin_no
        #             , SUM(gmv) AS gmv
        #             , round(AVG(NULLIF(gpm, 0)), 2) AS gpm
        #             , round((CASE WHEN SUM(exposure_cnt) = 0 THEN 0 ELSE SUM(watch_cnt) / SUM(exposure_cnt) END)*100, 2) AS exposure_view_rate
        #             , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(product_click_cnt) / SUM(watch_cnt) END)*100, 2) AS view_product_click_rate
        #             , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(deal_order_cnt) / SUM(watch_cnt) END)*100, 2) AS view_deal_order_rate
        #             , SUM(watch_ucnt) AS watch_ucnt
        #             , round(AVG(avg_watch_duration)) AS avg_watch_duration
        #             , SUM(watch_cnt) AS watch_cnt
        #             , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(fans_ucnt) / SUM(watch_ucnt) END)*100, 2) AS fans_ucnt
        #             , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(clubs_ucnt) / SUM(watch_ucnt) END)*100, 2) AS clubs_ucnt
        #             , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(interact_ucnt) / SUM(watch_ucnt) END)*100, 2) AS view_interact_rate
        #         FROM review_room
        #         WHERE douyin_no = '{body['douyin_no']}'
        #             AND room_id IN {tup_room_ids if len(tup_room_ids) > 0 else "(' ')"}
        #             AND start_time >= {body['start_time']}
        #             AND start_time <= {body['end_time']}
        #         GROUP BY douyin_no, str_time
        #     ),
        #     last_period AS (
        #         SELECT
        #             str_time AS date_time
        #             , douyin_no
        #             , SUM(gmv) AS gmv
        #             , round(AVG(NULLIF(gpm, 0)), 2) AS gpm
        #             , round((CASE WHEN SUM(exposure_cnt) = 0 THEN 0 ELSE SUM(watch_cnt) / SUM(exposure_cnt) END)*100, 2) AS exposure_view_rate
        #             , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(product_click_cnt) / SUM(watch_cnt) END)*100, 2) AS view_product_click_rate
        #             , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(deal_order_cnt) / SUM(watch_cnt) END)*100, 2) AS view_deal_order_rate
        #             , SUM(watch_ucnt) AS watch_ucnt
        #             , round(AVG(avg_watch_duration)) AS avg_watch_duration
        #             , SUM(watch_cnt) AS watch_cnt
        #             , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(fans_ucnt) / SUM(watch_ucnt) END)*100, 2) AS fans_ucnt
        #             , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(clubs_ucnt) / SUM(watch_ucnt) END)*100, 2) AS clubs_ucnt
        #             , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(interact_ucnt) / SUM(watch_ucnt) END)*100, 2) AS view_interact_rate
        #         FROM review_room
        #         WHERE douyin_no = '{body['douyin_no']}'
        #             AND etl_flag = 4
        #             -- 环比区间的开始时间：start_time - (end_time - start_time) - 1
        #             -- 环比区间的结束时间：end_time - (end_time - start_time) - 1
        #             AND start_time >= {body['start_time']} - ({body['end_time']} - {body['start_time']}) -1
        #             AND start_time <= {body['end_time']} - ({body['end_time']} - {body['start_time']}) -1
        #             AND room_id IN {tup_room_ids_last_cycle if len(tup_room_ids_last_cycle) > 0 else "(' ')"}
        #         GROUP BY douyin_no, str_time
        #     )
        #     SELECT * FROM last_period
        #     UNION
        #     SELECT * FROM this_period

        # '''
        # df2 = self.execute_sql_stime(pg_sql_2, conn)


        for x in resp['data']:
            print(x)
            metrics = x['metrics']
            if metrics == 'view_fans_rate':
                metrics = 'fans_ucnt'
            elif metrics == 'view_clubs_rate':
                metrics = 'clubs_ucnt'

            _dic = x.get('detail')
            if _dic:
                assert float(_dic['sum_value']) == float(df1[metrics].values[0])
                assert math.isclose(float(_dic['compare_last_cycle'][:-1]), float(df1['compare_last_' + metrics].values[0]), abs_tol = 0.02)
                assert math.isclose(float(_dic['diff_last_cycle']), float(df1['diff_' + metrics].values[0]), abs_tol = 0.02)
                # _dic['evaluate']

                if float(_dic['compare_last_cycle'][:-1]) < 0:
                    assert _dic['evaluate'] == 'D'
                elif float(_dic['compare_last_cycle'][:-1]) >= 0 and float(_dic['compare_last_cycle'][:-1]) < 20:
                    assert _dic['evaluate'] == 'C'
                elif float(_dic['compare_last_cycle'][:-1]) >= 20 and float(_dic['compare_last_cycle'][:-1]) < 50:
                    assert _dic['evaluate'] == 'B'
                elif float(_dic['compare_last_cycle'][:-1]) >= 50:
                    assert _dic['evaluate'] == 'A'


                df_line = pd.DataFrame(_dic['line_chart'])
                if not df_line.empty:
                    df_line['value'] = df_line['value'].astype('float')
                    df_line = df_line.rename(columns={'value': metrics})
                    # print(df_line)
                    # print(df2[['date_time', metrics]])

                    # assert_frame_equal(df_line, df2[['date_time', metrics]])


    # 数据库和接口地址环境要对应
    def test_check_VAP_API_WeChar(self, conn):

        api_start_time = 1678291200
        api_end_time = 1678377599
        api_douyin_no = 'wusu868688'


        room_ids_sql = f'''
            SELECT room_id
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY etl_flag DESC) AS s_rank
                    , to_char(to_timestamp(start_time), 'YYYY-MM-DD HH24:MI:SS') AS str_time
                FROM vap_review_room
            ) t WHERE s_rank = 1 AND douyin_no = '{api_douyin_no}'
                AND start_time >= {api_start_time}
                AND start_time <= {api_end_time}
        '''
        room_ids_df = self.execute_sql_stime(room_ids_sql, conn)
        room_ids = room_ids_df['room_id'].tolist()
        tup_room_ids = tuple(room_ids)
        if len(tup_room_ids) == 1:
            tup_room_ids = str(tup_room_ids).replace(',', '')


        room_ids_last_cycle_sql = f'''
            SELECT room_id
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY etl_flag DESC) AS s_rank
                    , to_char(to_timestamp(start_time), 'YYYY-MM-DD HH24:MI:SS') AS str_time
                FROM vap_review_room
            ) t WHERE s_rank = 1 AND douyin_no = '{api_douyin_no}'
                AND start_time <= {api_end_time} - 3600*24
                AND start_time >= {api_start_time} - 3600*24*30
        '''
        room_ids_last_cycle_df = self.execute_sql_stime(room_ids_last_cycle_sql, conn)
        room_ids_last_cycle = room_ids_last_cycle_df['room_id'].tolist()
        tup_room_ids_last_cycle = tuple(room_ids_last_cycle)
        if len(tup_room_ids_last_cycle) == 1:
            tup_room_ids_last_cycle = str(tup_room_ids_last_cycle).replace(',', '')


        body = {
            "douyin_no": api_douyin_no,
            "start_time": api_start_time,
            "end_time": api_end_time,
            "room_ids": room_ids,
            "room_ids_last_cycle": room_ids_last_cycle,
            "metrics": [
                "gmv",
                "gpm",
                "exposure_view_rate",
                "view_product_click_rate",
                "view_deal_order_rate",
                "watch_ucnt",
                "avg_watch_duration",
                "watch_cnt",
                "view_fans_rate",
                "view_clubs_rate",
                "view_interact_rate",
                "jl_form_submit_cnt",
                "jl_watch_comment_rate",
                "jl_watch_share_rate",
                "jl_watch_like_rate",
                "jl_card_show_click_rate",
                "jl_card_click_form_submit_rate",
                "jl_watch_ucnt_day",
                "jl_avg_watch_duration"
            ],
            "compare_days":30
        }
        print(json.dumps(body))
        resp = requests.post(self.overview_url, json.dumps(body)).json()
        # print(resp)

        pg_sql_1 = f'''
            WITH review_room AS (
                SELECT *
                FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY etl_flag DESC) AS s_rank
                        , to_char(to_timestamp(start_time), 'YYYY-MM-DD HH24:MI:SS') AS str_time
                    FROM vap_review_room
                ) t WHERE s_rank = 1
            ),
            this_period AS (
                SELECT
                    douyin_no
                    , SUM(gmv) AS gmv
                    , round(AVG(NULLIF(gpm, 0)), 2) AS gpm
                    , round((CASE WHEN SUM(exposure_cnt) = 0 THEN 0 ELSE SUM(watch_cnt) / SUM(exposure_cnt)::NUMERIC END)*100, 2) AS exposure_view_rate
                    , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(product_click_cnt) / SUM(watch_cnt)::NUMERIC END)*100, 2) AS view_product_click_rate
                    , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(deal_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS view_deal_order_rate
                    , SUM(watch_ucnt) AS watch_ucnt
                    , round(AVG(avg_watch_duration)) AS avg_watch_duration
                    , SUM(watch_cnt) AS watch_cnt
                    , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(fans_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS fans_ucnt
                    , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(clubs_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS clubs_ucnt
                    , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(interact_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS view_interact_rate
                    , SUM(jl_form_submit_cnt) AS jl_form_submit_cnt
                    , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_comment_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_comment_rate
                    , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_share_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_share_rate
                    , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_like_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_like_rate
                    , round((CASE WHEN SUM(jl_card_show_cnt) = 0 THEN 0 ELSE SUM(jl_card_click_cnt)/SUM(jl_card_show_cnt)::NUMERIC END)*100, 2) AS jl_card_show_click_rate
                    , round((CASE WHEN SUM(jl_card_click_cnt) = 0 THEN 0 ELSE SUM(jl_form_submit_cnt)/SUM(jl_card_click_cnt)::NUMERIC END)*100, 2) AS jl_card_click_form_submit_rate
                    , round(SUM(jl_watch_ucnt) / SUM(day_num)::NUMERIC) AS jl_watch_ucnt_day
                    , round(AVG(jl_avg_watch_duration), 2) AS jl_avg_watch_duration
                FROM (
                    SELECT *, (CASE WHEN ROW_NUMBER() OVER(PARTITION BY to_char(to_timestamp(start_time), 'YYYY-MM-DD')) = 1 THEN 1 ELSE 0 END) AS day_num
                    FROM review_room
                    WHERE douyin_no = '{body['douyin_no']}'
                        AND room_id IN {tup_room_ids if len(tup_room_ids) > 0 else "(' ')"}
                        AND start_time >= {body['start_time']}
                        AND start_time <= {body['end_time']}
                ) t
                GROUP BY douyin_no
            ),
            last_period AS (
                SELECT douyin_no
                    , AVG(gmv) AS gmv
                    , AVG(gpm) AS gpm
                    , AVG(exposure_view_rate) AS exposure_view_rate
                    , AVG(view_product_click_rate) AS view_product_click_rate
                    , AVG(view_deal_order_rate) AS view_deal_order_rate
                    , AVG(watch_ucnt) AS watch_ucnt
                    , AVG(avg_watch_duration) AS avg_watch_duration
                    , AVG(watch_cnt) AS watch_cnt
                    , AVG(fans_ucnt) AS fans_ucnt
                    , AVG(clubs_ucnt) AS clubs_ucnt
                    , AVG(view_interact_rate) AS view_interact_rate
                    , AVG(jl_form_submit_cnt) AS jl_form_submit_cnt
                    , AVG(jl_watch_comment_rate) AS jl_watch_comment_rate
                    , AVG(jl_watch_share_rate) AS jl_watch_share_rate
                    , AVG(jl_watch_like_rate) AS jl_watch_like_rate
                    , AVG(jl_card_show_click_rate) AS jl_card_show_click_rate
                    , AVG(jl_card_click_form_submit_rate) AS jl_card_click_form_submit_rate
                    , AVG(jl_watch_ucnt_day) AS jl_watch_ucnt_day
                    , AVG(jl_avg_watch_duration) AS jl_avg_watch_duration
                FROM (
                    SELECT
                        douyin_no
                        , to_char(to_timestamp(start_time), 'YYYY-MM-DD')
                        , SUM(gmv) AS gmv
                        , round(AVG(NULLIF(gpm, 0)), 2) AS gpm
                        , round((CASE WHEN SUM(exposure_cnt) = 0 THEN 0 ELSE SUM(watch_cnt) / SUM(exposure_cnt)::NUMERIC END)*100, 2) AS exposure_view_rate
                        , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(product_click_cnt) / SUM(watch_cnt)::NUMERIC END)*100, 2) AS view_product_click_rate
                        , round((CASE WHEN SUM(watch_cnt) = 0 THEN 0 ELSE SUM(deal_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS view_deal_order_rate
                        , SUM(watch_ucnt) AS watch_ucnt
                        , round(AVG(avg_watch_duration)) AS avg_watch_duration
                        , SUM(watch_cnt) AS watch_cnt
                        , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(fans_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS fans_ucnt
                        , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(clubs_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS clubs_ucnt
                        , round((CASE WHEN SUM(watch_ucnt) = 0 THEN 0 ELSE SUM(interact_ucnt) / SUM(watch_ucnt)::NUMERIC END)*100, 2) AS view_interact_rate
                        , SUM(jl_form_submit_cnt) AS jl_form_submit_cnt
                        , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_comment_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_comment_rate
                        , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_share_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_share_rate
                        , round((CASE WHEN SUM(jl_watch_cnt) = 0 THEN 0 ELSE SUM(jl_like_cnt)/SUM(jl_watch_cnt)::NUMERIC END)*100, 2) AS jl_watch_like_rate
                        , round((CASE WHEN SUM(jl_card_show_cnt) = 0 THEN 0 ELSE SUM(jl_card_click_cnt)/SUM(jl_card_show_cnt)::NUMERIC END)*100, 2) AS jl_card_show_click_rate
                        , round((CASE WHEN SUM(jl_card_click_cnt) = 0 THEN 0 ELSE SUM(jl_form_submit_cnt)/SUM(jl_card_click_cnt)::NUMERIC END)*100, 2) AS jl_card_click_form_submit_rate
                        , round(SUM(jl_watch_ucnt) / SUM(day_num)::NUMERIC) AS jl_watch_ucnt_day
                        , round(AVG(jl_avg_watch_duration), 2) AS jl_avg_watch_duration
                    FROM (
                        SELECT *, (CASE WHEN ROW_NUMBER() OVER(PARTITION BY to_char(to_timestamp(start_time), 'YYYY-MM-DD')) = 1 THEN 1 ELSE 0 END) AS day_num
                        FROM review_room
                        WHERE douyin_no = '{body['douyin_no']}'
                            AND start_time <= {body['end_time']} - 3600*24
                            AND start_time >= {body['start_time']} - 3600*24*30
                            AND room_id IN {tup_room_ids_last_cycle if len(tup_room_ids_last_cycle) > 0 else "(' ')"}
                    ) t
                    GROUP BY douyin_no, to_char(to_timestamp(start_time), 'YYYY-MM-DD')
                ) t GROUP BY douyin_no
            )
            SELECT
                A.douyin_no
                , round(A.gmv, 2) AS gmv
                , round(A.gpm, 2) AS gpm
                , round(A.exposure_view_rate, 2) AS exposure_view_rate
                , round(A.view_product_click_rate, 2) AS view_product_click_rate
                , round(A.view_deal_order_rate, 2) AS view_deal_order_rate
                , round(A.watch_ucnt, 2) AS watch_ucnt
                , round(A.avg_watch_duration) AS avg_watch_duration
                , round(A.watch_cnt, 2) AS watch_cnt
                , round(A.fans_ucnt, 2) AS fans_ucnt
                , round(A.clubs_ucnt, 2) AS clubs_ucnt
                , round(A.view_interact_rate, 2) AS view_interact_rate
                , A.jl_form_submit_cnt
                , A.jl_watch_comment_rate
                , A.jl_watch_share_rate
                , A.jl_watch_like_rate
                , A.jl_card_show_click_rate
                , A.jl_card_click_form_submit_rate
                , A.jl_watch_ucnt_day
                , A.jl_avg_watch_duration
                -- 下面是diff
                , round(COALESCE(B.gmv, 0), 2) AS diff_gmv
                , round(COALESCE(B.gpm, 0), 2) AS diff_gpm
                , round(COALESCE(B.exposure_view_rate, 0), 2) AS diff_exposure_view_rate
                , round(COALESCE(B.view_product_click_rate, 0), 2) AS diff_view_product_click_rate
                , round(COALESCE(B.view_deal_order_rate, 0), 2) AS diff_view_deal_order_rate
                , round(COALESCE(B.watch_ucnt, 0), 2) AS diff_watch_ucnt
                , round(COALESCE(B.avg_watch_duration, 0)) AS diff_avg_watch_duration
                , round(COALESCE(B.watch_cnt, 0), 2) AS diff_watch_cnt
                , round(COALESCE(B.fans_ucnt, 0), 2) AS diff_fans_ucnt
                , round(COALESCE(B.clubs_ucnt, 0), 2) AS diff_clubs_ucnt
                , round(COALESCE(B.view_interact_rate, 0), 2) AS diff_view_interact_rate
                , round(COALESCE(B.jl_form_submit_cnt, 0), 2) AS diff_jl_form_submit_cnt
                , round(COALESCE(B.jl_watch_comment_rate, 0), 2) AS diff_jl_watch_comment_rate
                , round(COALESCE(B.jl_watch_share_rate, 0), 2) AS diff_jl_watch_share_rate
                , round(COALESCE(B.jl_watch_like_rate, 0), 2) AS diff_jl_watch_like_rate
                , round(COALESCE(B.jl_card_show_click_rate, 0), 2) AS diff_jl_card_show_click_rate
                , round(COALESCE(B.jl_card_click_form_submit_rate, 0), 2) AS diff_jl_card_click_form_submit_rate
                , round(COALESCE(B.jl_watch_ucnt_day, 0), 2) AS diff_jl_watch_ucnt_day
                , B.jl_avg_watch_duration AS diff_jl_avg_watch_duration
                -- 下面是计算环比的
                , round((CASE WHEN B.gmv = 0 THEN 0 ELSE (A.gmv - B.gmv) / B.gmv::NUMERIC * 100 END), 2) AS "compare_last_gmv"
                , round((CASE WHEN B.gpm = 0 THEN 0 ELSE (A.gpm - B.gpm) / B.gpm::NUMERIC * 100 END), 2) AS "compare_last_gpm"
                , round((CASE WHEN B.exposure_view_rate = 0 THEN 0 ELSE (A.exposure_view_rate - B.exposure_view_rate) / B.exposure_view_rate::NUMERIC * 100 END), 2) AS "compare_last_exposure_view_rate"
                , round((CASE WHEN B.view_product_click_rate = 0 THEN 0 ELSE (A.view_product_click_rate - B.view_product_click_rate) / B.view_product_click_rate::NUMERIC * 100 END), 2) AS "compare_last_view_product_click_rate"
                , round((CASE WHEN B.view_deal_order_rate = 0 THEN 0 ELSE (A.view_deal_order_rate - B.view_deal_order_rate) / B.view_deal_order_rate::NUMERIC * 100 END), 2) AS "compare_last_view_deal_order_rate"
                , round((CASE WHEN B.watch_ucnt = 0 THEN 0 ELSE (A.watch_ucnt - B.watch_ucnt) / B.watch_ucnt::NUMERIC * 100 END), 2) AS "compare_last_watch_ucnt"
                , round((CASE WHEN round(B.avg_watch_duration) = 0 THEN 0 ELSE (round(A.avg_watch_duration) - round(B.avg_watch_duration)) / round(B.avg_watch_duration)::NUMERIC * 100 END), 2) AS "compare_last_avg_watch_duration"
                , round((CASE WHEN B.watch_cnt = 0 THEN 0 ELSE (A.watch_cnt - B.watch_cnt) / B.watch_cnt::NUMERIC * 100 END), 2) AS "compare_last_watch_cnt"
                , round((CASE WHEN B.fans_ucnt = 0 THEN 0 ELSE (A.fans_ucnt - B.fans_ucnt) / B.fans_ucnt::NUMERIC * 100 END), 2) AS "compare_last_fans_ucnt"
                , round((CASE WHEN round(B.clubs_ucnt, 2) = 0 THEN 0 ELSE (round(A.clubs_ucnt, 2) - round(B.clubs_ucnt, 2)) / round(B.clubs_ucnt, 2)::NUMERIC * 100 END)) AS "compare_last_clubs_ucnt"
                , round((CASE WHEN B.view_interact_rate = 0 THEN 0 ELSE (A.view_interact_rate - B.view_interact_rate) / B.view_interact_rate::NUMERIC * 100 END), 2) AS "compare_last_view_interact_rate"
                , round((CASE WHEN B.jl_form_submit_cnt = 0 THEN 0 ELSE (A.jl_form_submit_cnt - B.jl_form_submit_cnt) / B.jl_form_submit_cnt::NUMERIC * 100 END), 2) AS "compare_last_jl_form_submit_cnt"
                , round((CASE WHEN B.jl_watch_comment_rate = 0 THEN 0 ELSE (A.jl_watch_comment_rate - B.jl_watch_comment_rate) / B.jl_watch_comment_rate::NUMERIC * 100 END), 2) AS "compare_last_jl_watch_comment_rate"
                , round((CASE WHEN B.jl_watch_share_rate = 0 THEN 0 ELSE (A.jl_watch_share_rate - B.jl_watch_share_rate) / B.jl_watch_share_rate::NUMERIC * 100 END), 2) AS "compare_last_jl_watch_share_rate"
                , round((CASE WHEN B.jl_watch_like_rate = 0 THEN 0 ELSE (A.jl_watch_like_rate - B.jl_watch_like_rate) / B.jl_watch_like_rate::NUMERIC * 100 END), 2) AS "compare_last_jl_watch_like_rate"
                , round((CASE WHEN B.jl_card_show_click_rate = 0 THEN 0 ELSE (A.jl_card_show_click_rate - B.jl_card_show_click_rate) / B.jl_card_show_click_rate::NUMERIC * 100 END), 2) AS "compare_last_jl_card_show_click_rate"
                , round((CASE WHEN B.jl_card_click_form_submit_rate = 0 THEN 0 ELSE (A.jl_card_click_form_submit_rate - B.jl_card_click_form_submit_rate) / B.jl_card_click_form_submit_rate::NUMERIC * 100 END), 2) AS "compare_last_jl_card_click_form_submit_rate"
                , round((CASE WHEN B.jl_watch_ucnt_day = 0 THEN 0 ELSE (A.jl_watch_ucnt_day - B.jl_watch_ucnt_day) / B.jl_watch_ucnt_day::NUMERIC * 100 END), 2) AS "compare_last_jl_watch_ucnt_day"
                , round((CASE WHEN B.jl_avg_watch_duration = 0 THEN 0 ELSE (A.jl_avg_watch_duration - B.jl_avg_watch_duration) / B.jl_avg_watch_duration::NUMERIC * 100 END), 2) AS "compare_last_jl_avg_watch_duration"
            FROM this_period A LEFT JOIN last_period B ON A.douyin_no = B.douyin_no


        '''

        print(pg_sql_1)
        df1 = self.execute_sql_stime(pg_sql_1, conn)
        df1.fillna(0, inplace = True)
        # df1.to_csv("111.csv")

        for x in resp['data']:
            print(x)
            metrics = x['metrics']
            if metrics == 'view_fans_rate':
                metrics = 'fans_ucnt'
            elif metrics == 'view_clubs_rate':
                metrics = 'clubs_ucnt'

            _dic = x.get('detail')
            if _dic:
                assert float(_dic['sum_value']) == float(df1[metrics].values[0])
                assert math.isclose(float(_dic['compare_last_cycle'][:-1]), float(df1['compare_last_' + metrics].values[0]), abs_tol = 0.5)
                assert math.isclose(float(_dic['diff_last_cycle']), float(df1['diff_' + metrics].values[0]), abs_tol = 0.5)
                # _dic['evaluate']

                if float(_dic['compare_last_cycle'][:-1]) < 0:
                    assert _dic['evaluate'] == 'D'
                elif float(_dic['compare_last_cycle'][:-1]) >= 0 and float(_dic['compare_last_cycle'][:-1]) < 20:
                    assert _dic['evaluate'] == 'C'
                elif float(_dic['compare_last_cycle'][:-1]) >= 20 and float(_dic['compare_last_cycle'][:-1]) < 50:
                    assert _dic['evaluate'] == 'B'
                elif float(_dic['compare_last_cycle'][:-1]) >= 50:
                    assert _dic['evaluate'] == 'A'


                df_line = pd.DataFrame(_dic['line_chart'])
                if not df_line.empty:
                    df_line['value'] = df_line['value'].astype('float')

                    df_line = df_line.rename(columns={'value': metrics})
                    # print(df_line)
                    # print(df2[['date_time', metrics]])

                    # assert_frame_equal(df_line, df2[['date_time', metrics]])


    def test_check_VAP_API_channel_jl_flow(self, conn):

        api_start_time = 1678896000
        api_end_time = 1679241599
        api_douyin_no = 'CSMDLEXUS'

        body = {
            "douyin_no": api_douyin_no,
            "start_time": api_start_time,
            "end_time": api_end_time,
            "metrics": [
                "jl_flow",
            ],
            "sort_field":"this_value",
            "is_desc":"desc"
        }
        print(json.dumps(body))
        resp = requests.post(self.cast_api_dev + '/vap/api/channel', json.dumps(body)).json()
        print(resp)

        df1 = pd.DataFrame(resp['data'][0]['detail'])
        # print(df1)


        if body['metrics'][0] == 'jl_flow':
            field = 'jl_watch_ucnt'
        elif body['metrics'][0] == 'lp_flow':
            field = 'live_watch_cnt'
        elif body['metrics'][0] == 'lp_gmv':
            field = 'gmv'
        elif body['metrics'][0] == 'lp_gpm':
            field = 'gpm'




        api_sql_1 = f'''
            WITH this_period AS (
                SELECT douyin_no, first_channel_code, first_channel_name, second_channel_code, second_channel_name
                    , SUM(live_watch_cnt) FILTER(WHERE live_watch_cnt <> -1) AS live_watch_cnt
                    , SUM(gmv) FILTER(WHERE gmv <> -1) AS gmv
                    , AVG(NULLIF(gpm, 0)) FILTER(WHERE gpm <> -1) AS gpm
                    , SUM(jl_watch_ucnt) FILTER(WHERE jl_watch_ucnt <> -1) AS jl_watch_ucnt
                FROM vap_channel_analysis
                WHERE douyin_no = '{api_douyin_no}'
                    AND etl_date >= to_char(to_timestamp({api_start_time} + 3600*8), 'YYYY-MM-DD')
                    AND etl_date <= to_char(to_timestamp({api_end_time} + 3600*8), 'YYYY-MM-DD')
                GROUP BY douyin_no, first_channel_code, first_channel_name, second_channel_code, second_channel_name
            ),
            last_period AS (
                SELECT douyin_no, first_channel_code, first_channel_name, second_channel_code, second_channel_name
                    , SUM(live_watch_cnt) FILTER(WHERE live_watch_cnt <> -1) AS live_watch_cnt
                    , SUM(gmv) FILTER(WHERE gmv <> -1) AS gmv
                    , AVG(NULLIF(gpm, 0)) FILTER(WHERE gpm <> -1) AS gpm
                    , SUM(jl_watch_ucnt) FILTER(WHERE jl_watch_ucnt <> -1) AS jl_watch_ucnt
                FROM vap_channel_analysis
                WHERE douyin_no = '{api_douyin_no}'
                    -- 上期的开始时间：start_time - (end_time - start_time) - 1
                    -- 上期的结束时间：end_time - (end_time - start_time) - 1
                    AND etl_date >= to_char(to_timestamp(({api_start_time} - ({api_end_time} - {api_start_time}) - 1) + 3600*8), 'YYYY-MM-DD')
                    AND etl_date <= to_char(to_timestamp(({api_end_time} - ({api_end_time} - {api_start_time}) - 1) + 3600*8), 'YYYY-MM-DD')
                GROUP BY douyin_no, first_channel_code, first_channel_name, second_channel_code, second_channel_name
            ),
            tmp1 AS (
                SELECT *
                    , (CASE WHEN last_rate = 0 THEN 0 ELSE round((this_rate - last_rate)/last_rate * 100, 2) END) AS diff_last_rate
                FROM (
                    SELECT
                        A.douyin_no, A.first_channel_code, A.first_channel_name, A.second_channel_code, A.second_channel_name
                        , round(A.{field}, 2) AS this_value
                        -- 上期值
                        , round(B.{field}, 2) AS "last_value"
                        -- 本期和上期的数值环比
                        , CASE WHEN B.{field} = 0 THEN 0 ELSE round((A.{field} - B.{field})/B.{field}::NUMERIC * 100, 2) END AS diff_last_value
                        -- 本期值和上期值的占比
                        , round(A.{field} / SUM(A.{field}::NUMERIC) OVER(PARTITION BY A.douyin_no) * 100, 2) AS this_rate
                        , round(B.{field} / SUM(B.{field}::NUMERIC) OVER(PARTITION BY A.douyin_no) * 100, 2) AS last_rate
                    FROM this_period A
                    LEFT JOIN last_period B ON A.douyin_no = B.douyin_no AND A.first_channel_code = B.first_channel_code AND A.second_channel_code = B.second_channel_code
                ) t1
            ),
            tmp2 AS (
                SELECT *
                    , round(CASE WHEN "last_rate" = 0 THEN 0 ELSE (this_rate - "last_rate")/"last_rate"::NUMERIC * 100 END, 2) AS diff_last_rate
                FROM (
                    SELECT *
                        , round(this_value / SUM(this_value::NUMERIC) OVER(PARTITION BY douyin_no)::NUMERIC * 100, 2) AS this_rate
                        , round("last_value" / SUM("last_value"::NUMERIC) OVER(PARTITION BY douyin_no)::NUMERIC * 100, 2) AS "last_rate"
                    FROM (
                        SELECT douyin_no, first_channel_code, first_channel_name
                            , '' AS second_channel_code
                            , '' AS second_channel_name
                            , round(SUM(this_value), 2) AS this_value
                            , round(SUM("last_value"), 2) AS "last_value"
                            , CASE WHEN SUM("last_value") = 0 THEN 0 ELSE round((SUM("this_value") - SUM("last_value"))/SUM("last_value")::NUMERIC * 100, 2) END diff_last_value
                        FROM tmp1
                        GROUP BY douyin_no, first_channel_code, first_channel_name
                    ) t
                ) t2
            )
            SELECT * FROM tmp1
            UNION
            SELECT * FROM tmp2

        '''
        print(api_sql_1)

        df2 = self.execute_sql_stime(api_sql_1, conn)
        df2.to_csv("111.csv")


        m_df = pd.merge(df1, df2, how='outer', on=['first_channel_code', 'first_channel_name', 'second_channel_code'])
        print(m_df)
        m_df.fillna(0, inplace = True)

        m_df['this_value_x'] = m_df['this_value_y'].astype(float)
        m_df['last_value_x'] = m_df['last_value_y'].astype(float)
        m_df['diff_last_value_x'] = m_df['diff_last_value_y'].astype(float)
        m_df['this_rate_x'] = m_df['this_rate_y'].astype(float)
        m_df['last_rate_x'] = m_df['last_rate_y'].astype(float)
        m_df['diff_last_rate_x'] = m_df['diff_last_rate_y'].astype(float)

        print(m_df.dtypes)
        print(m_df.columns.values)


        self.assert_info(m_df, 'this_value_x', 'this_value_y', col_list=['douyin_no', 'first_channel_code', 'second_channel_code'])
        self.assert_info(m_df, 'last_value_x', 'last_value_y', col_list=['douyin_no', 'first_channel_code', 'second_channel_code'])
        self.assert_info(m_df, 'diff_last_value_x', 'diff_last_value_y', col_list=['douyin_no', 'first_channel_code', 'second_channel_code'])
        self.assert_info(m_df, 'this_rate_x', 'this_rate_y', col_list=['douyin_no', 'first_channel_code', 'second_channel_code'])
        self.assert_info(m_df, 'last_rate_x', 'last_rate_y', col_list=['douyin_no', 'first_channel_code', 'second_channel_code'])
        self.assert_info(m_df, 'diff_last_rate_x', 'diff_last_rate_y', col_list=['douyin_no', 'first_channel_code', 'second_channel_code'])
        self.assert_info(m_df, 'second_channel_name_x', 'second_channel_name_y', col_list=['douyin_no', 'first_channel_code', 'second_channel_code'])






    #############################
    def test_cast14_room(self, conn):
        pg_sql_1 = '''
            SELECT * FROM vap_review_room WHERE etl_flag = 4
                -- 过滤demo
                AND room_id NOT IN ('7168807164490976039', '7170834714427656991', '7170478833219406605', '7170952050996513551', '7171404776121387787', '7170356399900609312', '7171448161796508454', '7170890953174371087', '7170107858405690148', '7170896899705047816', '7170526333934603021', '7170830906108824356', '7170850002573544199', '7167693946695535398', '7170667914039577375', '7170527760245328670', '7170633577693219584')
                -- 过滤美东
                AND douyin_no NOT IN ('demo', 'PCZZ037160219911', 'zhengkai60219911', 'xmmd6207777', 'CSMDLEXUS', 'PCNCHG')


        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        pg_sql_2 = f'''

            WITH reptile_room_info AS (
                SELECT
                    A.room_id
                    , A.douyin_no
                    , to_timestamp(A.start_time + 3600*8) AS start_time_str
                    , A.start_time
                    , A.end_time
                    , (A.end_time - A.start_time) AS live_duration
                    , B.interact_ratio
                    , B.watch_ucnt
                    , B.avg_watch_duration
                    , B.like_cnt
                    , B.comment_cnt
                    , B.follow_anchor_ucnt AS fans_ucnt
                    , B.pay_amt/100::NUMERIC AS gmv
                    , B.gpm/100::NUMERIC AS gpm
                    , B.update_time
                    , to_timestamp(B.update_time + 3600*8) AS update_time_str
                FROM vap_douyin_live_rooms A
                LEFT JOIN doudian_compass_reptile_room B ON A.room_id = B.room_id AND A.douyin_no = B.douyin_no
                WHERE A.status <> 1
                    -- AND A.is_delete <> 1
                    AND A.start_time > 1659283200
                    AND company_id NOT IN ('demo')
            ),
            reptile_inter AS (
                SELECT room_id
                    , douyin_no
                    , SUM(share_cnt) AS share_cnt
                    , SUM(fans_club_ucnt) AS clubs_ucnt
                FROM doudian_compass_reptile_interaction
                GROUP BY room_id, douyin_no
            ),
            live_core_index AS (
                SELECT * FROM (
                    SELECT
                        A.room_id
                        , A.douyin_no
                        , A.live_show_cnt
                        , B.watch_live_show_cnt_ratio
                        , round(B.watch_live_show_cnt_ratio * A.live_show_cnt) AS watch_cnt
                        , dense_rank() OVER(PARTITION BY A.room_id ORDER BY A."timestamp" DESC) AS s_rank
                        , ROW_NUMBER() OVER(PARTITION BY A.room_id, A."timestamp" ORDER BY A.create_timestamp DESC) AS dup_rank
                    FROM doudian_compass_live_core_index A
                    LEFT JOIN doudian_compass_live_diagnosis B ON A.room_id = B.room_id AND A.douyin_no = B.douyin_no
                ) t WHERE s_rank = 1 AND dup_rank = 1
            ),
            list_detail AS (
                SELECT * FROM (
                    SELECT douyin_no, room_id, timestamp, product_click_cnt, pay_cnt, watch_cnt
                        , ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY timestamp DESC) AS s_rank
                    FROM doudian_compass_live_list_detail
                ) t WHERE s_rank = 1
            ),
            gmv_inter AS (
                SELECT * FROM (
                    SELECT *
                        , ROW_NUMBER() OVER(PARTITION BY douyin_no, room_id ORDER BY timestamp DESC) AS s_rank
                    FROM doudian_compass_live_gmv_interaction
                ) t WHERE s_rank = 1
            ),
            jl_analysis_list AS (
                SELECT room_id
                    , live_watch_count AS jl_watch_cnt
                    , live_comment_count AS jl_comment_cnt
                    , live_share_count AS jl_share_cnt
                    , live_form_submit_count::int4 AS jl_form_submit_cnt
                    , live_card_icon_component_show_count AS jl_card_show_cnt
                    , live_card_icon_component_click_count AS jl_card_click_cnt
                    , live_watch_ucount AS jl_watch_ucnt
                    , live_avg_watch_duration AS jl_avg_watch_duration
                    , live_follow_count AS jl_fans_ucnt
                    , live_fans_count AS jl_clubs_ucnt
                FROM juliangzongheng_live_analysis_list
            ),
            jl_top_card AS (
                SELECT * FROM (
                    SELECT
                        room_id
                        , total_live_like_cnt
                        , ROW_NUMBER() OVER(PARTITION BY room_id ORDER BY create_time DESC ) AS srank
                    FROM juliangzongheng_live_big_screen_top_card
                ) t WHERE srank = 1
            )
            SELECT
                A.*
                , E.interactive_ucnt
                , (CASE WHEN A.start_time > 1669824000 THEN E.interactive_ucnt ELSE round(A.interact_ratio * A.watch_ucnt) END ) AS interact_ucnt
                , B.share_cnt
                , B.clubs_ucnt
                , C.live_show_cnt AS exposure_cnt
                -- 20221201开始取list_detail
                , (CASE WHEN A.start_time > 1669824000 THEN D.watch_cnt ELSE C.watch_cnt END) AS watch_cnt
                , D.product_click_cnt
                , D.pay_cnt AS deal_order_cnt
                , H.jl_watch_cnt
                , H.jl_comment_cnt
                , H.jl_share_cnt
                , H.jl_form_submit_cnt
                , H.jl_card_show_cnt
                , H.jl_card_click_cnt
                , J.total_live_like_cnt AS jl_like_cnt
                , E.live_show_ucnt AS exposure_ucnt
                , E.product_show_ucnt AS product_exposure_ucnt
                , E.product_click_ucnt AS product_click_ucnt
                , E.pay_ucnt AS deal_ucnt
                , H.jl_watch_ucnt
                , H.jl_avg_watch_duration
                , H.jl_fans_ucnt
                , H.jl_clubs_ucnt
            FROM reptile_room_info A
            LEFT JOIN reptile_inter B ON A.room_id = B.room_id AND A.douyin_no = B.douyin_no
            LEFT JOIN live_core_index C ON A.room_id = C.room_id
            LEFT JOIN list_detail D ON A.room_id = D.room_id AND A.douyin_no = D.douyin_no
            LEFT JOIN gmv_inter E ON A.room_id = E.room_id
            LEFT JOIN jl_analysis_list H ON A.room_id = H.room_id
            LEFT JOIN jl_top_card J ON A.room_id = J.room_id
            ORDER BY start_time DESC


        '''
        df2 = self.execute_sql_stime(pg_sql_2, conn)


        # m_df = pd.merge(df1, df2, how='outer', on=['room_id', 'douyin_no'])
        m_df = pd.merge(df1, df2, how='left', on=['room_id', 'douyin_no'])
        # print(m_df)
        print(m_df.columns.values)
        m_df.fillna(0, inplace = True)


        self.assert_info(m_df, 'start_time_x', 'start_time_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'end_time_x', 'end_time_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'live_duration_x', 'live_duration_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'exposure_cnt_x', 'exposure_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'watch_cnt_x', 'watch_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'watch_ucnt_x', 'watch_ucnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'like_cnt_x', 'like_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'comment_cnt_x', 'comment_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'share_cnt_x', 'share_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'interact_ucnt_x', 'interact_ucnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'fans_ucnt_x', 'fans_ucnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'clubs_ucnt_x', 'clubs_ucnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'product_click_cnt_x', 'product_click_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'deal_order_cnt_x', 'deal_order_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'avg_watch_duration_x', 'avg_watch_duration_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'end_time_x', 'end_time_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'live_duration_x', 'live_duration_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'jl_like_cnt_x', 'jl_like_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'jl_comment_cnt_x', 'jl_comment_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'jl_share_cnt_x', 'jl_share_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'jl_form_submit_cnt_x', 'jl_form_submit_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'jl_card_show_cnt_x', 'jl_card_show_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'jl_card_click_cnt_x', 'jl_card_click_cnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'exposure_ucnt_x', 'exposure_ucnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'product_exposure_ucnt_x', 'product_exposure_ucnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'product_click_ucnt_x', 'product_click_ucnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'deal_ucnt_x', 'deal_ucnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'jl_watch_ucnt_x', 'jl_watch_ucnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'jl_avg_watch_duration_x', 'jl_avg_watch_duration_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'jl_fans_ucnt_x', 'jl_fans_ucnt_y', col_list=['room_id', 'start_time_str', 'update_time_str'])
        self.assert_info(m_df, 'jl_clubs_ucnt_x', 'jl_clubs_ucnt_x', col_list=['room_id', 'start_time_str', 'update_time_str'])



    # 爬虫数据会修正
    def test_cast14_room_flow(self, conn):
        pg_room_list = '''
            SELECT room_id
            FROM vap_douyin_live_rooms A
            WHERE start_time > floor(extract(epoch from date_trunc('day', current_date))) - 3600 * 24
                AND end_time > 0
                AND douyin_no <> 'demo'
                AND status <> 1
                AND is_delete <> 1
        '''
        df_roomlist = self.execute_sql_stime(pg_room_list, conn)
        room_list = tuple(df_roomlist['room_id'].tolist())
        print(room_list)


        pg_sql_1 = f'''
            SELECT * FROM vap_review_room WHERE etl_flag = 3 AND room_id IN {room_list}
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)


        pg_sql_2 = f'''
            WITH zhenti AS (
                SELECT * FROM (
                    SELECT
                        room_id
                        , douyin_no
                        , "timestamp"
                        , live_show_cnt AS exposure_cnt
                        , watch_ucnt
                        , avg_watch_duration
                        , round(interact_watch_ucnt_ratio * watch_ucnt) AS interact_ucnt
                        , follow_anchor_ucnt
                        , fans_club_join_ucnt
                        , gmv/100::NUMERIC AS gmv
                        , round(gpm/100::NUMERIC, 2) AS gpm
                        , ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY "timestamp" DESC) AS z_rank
                    FROM doudian_compass_screen_zhengti
                    WHERE room_id IN {room_list}
                ) t WHERE z_rank = 1
            ),
            real_time AS (
                SELECT * FROM (
                    SELECT room_id
                        , douyin_no
                        , "timestamp"
                        , live_room_end_time AS end_time
                        , like_cnt
                        , comment_cnt
                        , ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY "timestamp" DESC) AS rt_rank
                    FROM doudian_compass_reptile_room_real_time
                    WHERE room_id IN {room_list}
                ) t WHERE rt_rank = 1
            ),
            reptile_inter AS (
                    SELECT room_id
                        , douyin_no
                        , SUM(share_cnt) AS share_cnt
                    FROM doudian_compass_reptile_interaction
                    WHERE room_id IN {room_list}
                    GROUP BY room_id, douyin_no
            ),
            list_detail AS (
                SELECT * FROM (
                    SELECT *
                        , ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY timestamp DESC) AS s_rank
                    FROM doudian_compass_live_list_detail
                    WHERE room_id IN {room_list}
                ) t WHERE s_rank = 1
            ),
            gmv_inter AS (
                SELECT * FROM (
                    SELECT *
                        , ROW_NUMBER() OVER(PARTITION BY douyin_no, room_id ORDER BY timestamp DESC) AS s_rank
                    FROM doudian_compass_live_gmv_interaction
                    WHERE room_id IN {room_list}
                ) t WHERE s_rank = 1
            ),
            jl_top_card AS (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id ORDER BY create_time DESC) AS srank
                    FROM juliangzongheng_live_big_screen_top_card
                    WHERE room_id IN {room_list}
                ) t WHERE srank = 1
            ),
            jl_analysis_list AS (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id ORDER BY update_time DESC) AS srank
                    FROM juliangzongheng_live_analysis_list
                    WHERE room_id IN {room_list}
                ) t WHERE srank = 1
            ),
            tmp1 AS (
                SELECT
                    B.room_id AS room_id_tmp
                    , B.exposure_cnt
                    , D.watch_cnt
                    , B.watch_ucnt
                    , B.avg_watch_duration
                    , E.interactive_ucnt AS interact_ucnt
                    , B.follow_anchor_ucnt AS fans_ucnt
                    , B.fans_club_join_ucnt AS clubs_ucnt
                    , B.gmv
                    , B.gpm
                    , A.share_cnt
                    , C.like_cnt
                    , C.comment_cnt
                    , D.product_click_cnt
                    , D.pay_cnt AS deal_order_cnt
                    , C.end_time AS end_time_tmp
                    , E.live_show_ucnt AS exposure_ucnt
                    , E.product_show_ucnt AS product_exposure_ucnt
                    , E.product_click_ucnt AS product_click_ucnt
                    , E.pay_ucnt AS deal_ucnt
                FROM zhenti B
                FULL JOIN reptile_inter A ON B.room_id = A.room_id
                LEFT JOIN real_time C ON B.room_id = C.room_id
                LEFT JOIN list_detail D ON B.room_id = D.room_id
                LEFT JOIN gmv_inter E ON B.room_id = E.room_id
            )
            SELECT
                COALESCE(A.room_id_tmp, H.room_id) AS room_id
                , P.douyin_no
                , A.*
                , P.start_time
                , COALESCE(A.end_time_tmp, M.room_finish_time) AS end_time
                , (COALESCE(A.end_time_tmp, M.room_finish_time) - P.start_time) AS live_duration
                , H.total_live_watch_cnt AS jl_watch_cnt
                , H.total_live_like_cnt AS jl_like_cnt
                , H.total_live_comment_cnt AS jl_comment_cnt
                , M.live_share_count AS jl_share_cnt
                , H.live_form_submit_count AS jl_form_submit_cnt
                , M.live_card_icon_component_show_count AS jl_card_show_cnt
                , H.live_card_icon_component_click_count AS jl_card_click_cnt
                , M.live_watch_ucount AS jl_watch_ucnt
                , H.total_live_avg_watch_duration AS jl_avg_watch_duration
                , H.total_live_follow_cnt AS jl_fans_ucnt
                , M.live_fans_count AS jl_clubs_ucnt
            FROM tmp1 A
            FULL JOIN jl_top_card H ON A.room_id_tmp = H.room_id
            LEFT JOIN jl_analysis_list M ON COALESCE(A.room_id_tmp, H.room_id) = M.room_id
            LEFT JOIN vap_douyin_live_rooms P ON COALESCE(A.room_id_tmp, H.room_id) = P.room_id OR H.room_id = P.room_id


        '''
        print(pg_sql_2)
        df2 = self.execute_sql_stime(pg_sql_2, conn)


        m_df = pd.merge(df1, df2, how='outer', on=['room_id', 'douyin_no'])
        print(m_df)
        print(m_df.columns.values)
        m_df.fillna(0, inplace = True)

        # start_time会修正，校验时需要加上误差
        self.assert_info(m_df, 'start_time_x', 'start_time_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'end_time_x', 'end_time_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'live_duration_x', 'live_duration_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'exposure_cnt_x', 'exposure_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'watch_cnt_x', 'watch_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'watch_ucnt_x', 'watch_ucnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'like_cnt_x', 'like_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'comment_cnt_x', 'comment_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'share_cnt_x', 'share_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'interact_ucnt_x', 'interact_ucnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'fans_ucnt_x', 'fans_ucnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'clubs_ucnt_x', 'clubs_ucnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'product_click_cnt_x', 'product_click_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'deal_order_cnt_x', 'deal_order_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'avg_watch_duration_x', 'avg_watch_duration_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'jl_like_cnt_x', 'jl_like_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'jl_comment_cnt_x', 'jl_comment_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'jl_share_cnt_x', 'jl_share_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'jl_form_submit_cnt_x', 'jl_form_submit_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'jl_card_show_cnt_x', 'jl_card_show_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'jl_card_click_cnt_x', 'jl_card_click_cnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'exposure_ucnt_x', 'exposure_ucnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'product_exposure_ucnt_x', 'product_exposure_ucnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'product_click_ucnt_x', 'product_click_ucnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'deal_ucnt_x', 'deal_ucnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'jl_watch_ucnt_x', 'jl_watch_ucnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'jl_avg_watch_duration_x', 'jl_avg_watch_duration_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'jl_fans_ucnt_x', 'jl_fans_ucnt_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'jl_clubs_ucnt_x', 'jl_clubs_ucnt_y', col_list=['room_id', 'douyin_no'])




    def test_cast14_room_minute(self, conn):
        pg_sql_room = '''
            SELECT room_id, start_time
            FROM vap_review_room_minute
            WHERE etl_flag = 4
            GROUP BY room_id, start_time
            ORDER BY start_time DESC
        '''
        df_roomID = self.execute_sql_stime(pg_sql_room, conn)
        count = 0
        tup_room_list = []

        for k, v in df_roomID.groupby(['room_id'], sort=False):
            room_id = k
            print(room_id)
            # tup_room_list.append(room_id)
            count += 1
            print(room_id, f"遍历的直播间数: {count}", tuple(tup_room_list))

            pg_sql_1 = f'''
                SELECT *
                FROM vap_review_room_minute
                WHERE etl_flag = 4 AND room_id = '{room_id}'
            '''
            df1 = self.execute_sql_stime(pg_sql_1, conn)


            pg_sql_2 = f'''

                WITH generate AS (
                    SELECT room_id, start_time, end_time, (end_time - start_time) / 60 AS live_duration
                        , douyin_no
                        , generate_series(s_start_time, s_end_time, 60) AS "timestamp"
                    FROM (
                        SELECT room_id
                            , douyin_no
                            , start_time
                            , end_time
                            , start_time - start_time % 60 - 60 AS s_start_time
                            , end_time - end_time % 60 + 60 AS s_end_time
                        FROM vap_douyin_live_rooms WHERE room_id = '{room_id}'
                    ) t
                ),
                real_time AS (
                    SELECT * FROM (
                        SELECT *
                            , ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no, "timestamp" ORDER BY create_time DESC) AS sr
                        FROM doudian_compass_reptile_room_real_time
                        WHERE room_id = (SELECT room_id FROM generate LIMIT 1)
                    ) t WHERE sr = 1
                ),
                live_core_index AS (
                    SELECT * FROM (
                        SELECT *
                            , ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no, "timestamp" ORDER BY create_timestamp DESC) AS sr
                        FROM doudian_compass_live_core_index
                        WHERE room_id = (SELECT room_id FROM generate LIMIT 1)
                    ) t WHERE sr = 1
                ),
                jl_minute AS (
                    SELECT room_id, to_timestamp("time_stamp"), "time_stamp"
                        , total_live_like_cnt AS jl_like_cnt
                        , total_live_comment_cnt AS jl_comment_cnt
                        , live_form_submit_count AS jl_form_submit_cnt
                        , live_card_icon_component_click_count AS jl_card_click_cnt
                        , total_live_pcu AS jl_online_ucnt
                        , total_live_watch_cnt AS jl_watch_cnt
                    FROM juliangzongheng_live_per_minute_metrics
                    WHERE room_id = (SELECT room_id FROM generate LIMIT 1)
                ),
                jl_live AS (
                    SELECT room_id
                        , EXTRACT(EPOCH from TO_TIMESTAMP(CONCAT(SUBSTRING(local_time_minute, 1, 10), SUBSTRING(local_time_minute, 22)), 'YYYY-MM-DD HH24:MI')) AS timestamp
                        , live_share_count AS jl_share_cnt
                        , live_card_icon_component_show_count AS jl_card_show_cnt
                    FROM juliangzongheng_live_minute
                )
                SELECT *
                    , (CASE
                        WHEN start_time > 1669824000 THEN watch_cnt_2
                        WHEN start_time BETWEEN 1668614400 AND 1669823999 THEN watch_cnt_1
                        WHEN start_time < 1668614400 THEN 0
                    END) AS watch_cnt
                FROM (
                    SELECT Z.*
                        , Z.timestamp AS pg_timestamp
                        , to_timestamp(Z.timestamp) AS time_str
                        , A.like_cnt
                        , A.comment_cnt
                        , A.share_cnt
                        , A.incr_fans_cnt AS fans_ucnt
                        , A.fans_club_ucnt AS clubs_ucnt
                        , B.live_show_cnt
                        ,(CASE
                            WHEN B.live_show_cnt IS NULL THEN -1
                            WHEN B.live_show_cnt - LAG(B.live_show_cnt, 1, 0) OVER(PARTITION BY B.douyin_no, B.room_id ORDER BY B.timestamp) < 0 THEN 0
                            ELSE B.live_show_cnt - LAG(B.live_show_cnt, 1, 0) OVER(PARTITION BY B.douyin_no, B.room_id ORDER BY B.timestamp)
                        END) AS exposure_cnt
                        , (CASE
                            WHEN round(C.watch_cnt_show_ratio * B.live_show_cnt) IS NULL THEN -1
                            WHEN round(C.watch_cnt_show_ratio * B.live_show_cnt) - LAG(round(C.watch_cnt_show_ratio * B.live_show_cnt), 1, 0::NUMERIC) OVER(PARTITION BY B.room_id, B.douyin_no ORDER BY B.timestamp) < 0 THEN 0
                            ELSE round(C.watch_cnt_show_ratio * B.live_show_cnt) - LAG(round(C.watch_cnt_show_ratio * B.live_show_cnt), 1, 0::NUMERIC) OVER(PARTITION BY B.room_id,
                            B.douyin_no ORDER BY B.timestamp)
                        END) AS watch_cnt_1
                        , (CASE
                            WHEN K.watch_cnt IS NULL THEN -1
                            WHEN K.watch_cnt - LAG(K.watch_cnt, 1, 0::bigint) OVER(PARTITION BY K.douyin_no, K.room_id ORDER BY K."timestamp") < 0 THEN 0
                            ELSE K.watch_cnt - LAG(K.watch_cnt, 1, 0::bigint) OVER(PARTITION BY K.douyin_no, K.room_id ORDER BY K."timestamp")
                        END) AS  watch_cnt_2
                        , (CASE
                            WHEN K.product_click_cnt IS NULL THEN -1
                            WHEN K.product_click_cnt - LAG(K.product_click_cnt, 1, 0::bigint) OVER(PARTITION BY K.douyin_no, K.room_id ORDER BY K.timestamp) < 0 THEN 0
                            ELSE K.product_click_cnt - LAG(K.product_click_cnt, 1, 0::bigint) OVER(PARTITION BY K.douyin_no, K.room_id ORDER BY K.timestamp)
                        END) AS product_click_cnt
                        , (CASE
                            WHEN K.pay_cnt IS NULL THEN -1
                            WHEN K.pay_cnt - LAG(K.pay_cnt, 1, 0::bigint) OVER(PARTITION BY K.douyin_no, K.room_id ORDER BY K.timestamp) < 0 THEN 0
                            ELSE K.pay_cnt - LAG(K.pay_cnt, 1, 0::bigint) OVER(PARTITION BY K.douyin_no, K.room_id ORDER BY K.timestamp)
                        END) AS deal_order_cnt
                        , E.min_gmv / 100::NUMERIC AS gmv
                        , (CASE
                            WHEN D.watch_ucnt IS NULL THEN -1
                            WHEN D.watch_ucnt - LAG(D.watch_ucnt, 1, 0::integer) OVER(PARTITION BY D.douyin_no, D.room_id ORDER BY D.timestamp) < 0 THEN 0
                            ELSE D.watch_ucnt - LAG(D.watch_ucnt, 1, 0::integer) OVER(PARTITION BY D.douyin_no, D.room_id ORDER BY D.timestamp)
                        END) AS watch_ucnt
                        , D.avg_watch_duration
                        , D.gpm/100::NUMERIC AS gpm
                        , J.jl_like_cnt
                        , J.jl_comment_cnt
                        , J.jl_form_submit_cnt
                        , J.jl_card_click_cnt
                        , M.jl_share_cnt
                        , M.jl_card_show_cnt
                        , J.jl_online_ucnt
                        , J.jl_watch_cnt
                    FROM generate Z
                    LEFT JOIN doudian_compass_reptile_interaction A ON A.room_id = Z.room_id AND A."timestamp" = Z."timestamp"
                    LEFT JOIN live_core_index B ON Z.room_id = B.room_id AND Z."timestamp" = B."timestamp"
                    LEFT JOIN doudian_compass_screen_zhengti C ON Z.room_id = C.room_id AND Z."timestamp" = C."timestamp"
                    LEFT JOIN real_time D ON Z.room_id = D.room_id AND Z."timestamp" = D."timestamp"
                    LEFT JOIN doudian_compass_reptile_transaction_amount E ON Z.room_id = E.room_id AND Z."timestamp" = E."timestamp"
                    LEFT JOIN doudian_compass_live_list_detail K ON Z.room_id = K.room_id AND Z."timestamp" = K."timestamp"
                    LEFT JOIN jl_minute J ON Z.room_id = J.room_id AND Z."timestamp" = J."time_stamp"
                    LEFT JOIN jl_live M ON Z.room_id = M.room_id AND Z."timestamp" = M."timestamp"
                ) t
                ORDER BY pg_timestamp


            '''

            df2 = self.execute_sql_stime(pg_sql_2, conn)
            # 上一个有效值
            df2['gpm'] = df2['gpm'].fillna(method='pad')
            df2['avg_watch_duration'] = df2['avg_watch_duration'].fillna(method='pad')

            # 上一个有效值也为NULL时，赋值0（留资视图版本统一成-1）
            # df2['gpm'].fillna(0, inplace = True)
            # df2['avg_watch_duration'].fillna(0, inplace = True)


            m_df = pd.merge(df1, df2, how='left', on=['room_id', 'douyin_no', 'pg_timestamp'])
            m_df.fillna(-1, inplace = True)
            print(m_df)


            self.assert_info(m_df, 'start_time_x', 'start_time_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'end_time_x', 'end_time_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'exposure_cnt_x', 'exposure_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'watch_cnt_x', 'watch_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'like_cnt_x', 'like_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'comment_cnt_x', 'comment_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'share_cnt_x', 'share_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'fans_ucnt_x', 'fans_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'clubs_ucnt_x', 'clubs_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'product_click_cnt_x', 'product_click_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'deal_order_cnt_x', 'deal_order_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'watch_ucnt_x', 'watch_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'avg_watch_duration_x', 'avg_watch_duration_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'jl_like_cnt_x', 'jl_like_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'jl_comment_cnt_x', 'jl_comment_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'jl_form_submit_cnt_x', 'jl_form_submit_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'jl_card_click_cnt_x', 'jl_card_click_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'jl_share_cnt_x', 'jl_share_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'jl_card_show_cnt_x', 'jl_card_show_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'jl_online_ucnt_x', 'jl_online_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])
            self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'time_str'])




    # 历史部分数据爬虫不完整， 导致上下两行的差值会有问题
    def test_cast14_room_minute_flow(self, conn):
        pg_room_list = '''
            SELECT room_id
            FROM vap_douyin_live_rooms A
            WHERE start_time > floor(extract(epoch from date_trunc('day', current_date))) - 3600*24
                AND end_time > 0
                AND douyin_no <> 'demo'
                AND status <> 1
                AND is_delete <> 1
        '''
        df_roomlist = self.execute_sql_stime(pg_room_list, conn)
        room_list = tuple(df_roomlist['room_id'].tolist())
        print(room_list)

        pg_sql_1 = f'''
            SELECT to_timestamp(pg_timestamp), *
            FROM vap_review_room_minute
            WHERE etl_flag = 3
            AND room_id IN {room_list}
            ORDER BY pg_timestamp
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        pg_sql_2 = f'''
            WITH zhenti AS (
                SELECT
                    A.room_id
                    , A.douyin_no
                    , A."timestamp"
                    , A.gpm
                    , (CASE WHEN A.live_show_cnt - lag(A.live_show_cnt, 1, 0::integer) OVER(PARTITION BY A.douyin_no, A.room_id ORDER BY A.timestamp) < 0 THEN 0
                        ELSE A.live_show_cnt - lag(A.live_show_cnt, 1, 0::integer) OVER(PARTITION BY A.douyin_no, A.room_id ORDER BY A.timestamp) END) AS live_show_cnt
                    , A.watch_ucnt - LAG(A.watch_ucnt, 1, 0::bigint) OVER(PARTITION BY A.room_id, A.douyin_no ORDER BY A."timestamp") AS watch_ucnt
                    , A.avg_watch_duration
                    , (A.gmv - LAG(A.gmv, 1, 0::bigint) OVER(PARTITION BY A.room_id, A.douyin_no ORDER BY A."timestamp"))/100::NUMERIC AS gmv
                FROM doudian_compass_screen_zhengti A
                WHERE A.room_id IN {room_list}
            ),
            reptile_inter AS (
                 SELECT
                    room_id
                    , douyin_no
                    , timestamp
                    , like_cnt
                    , comment_cnt
                    , share_cnt
                    , incr_fans_cnt
                    , fans_club_ucnt
                FROM doudian_compass_reptile_interaction
                WHERE room_id IN {room_list}
            ),
            list_detail AS (
                SELECT room_id, douyin_no, "timestamp"
                    , B.watch_cnt - LAG(B.watch_cnt, 1, 0::bigint) OVER(PARTITION BY B.douyin_no, B.room_id ORDER BY B."timestamp") AS watch_cnt
                    , (CASE WHEN B.product_click_cnt - lag(B.product_click_cnt, 1, 0::bigint) OVER(PARTITION BY B.douyin_no, B.room_id ORDER BY B.timestamp) < 0 THEN 0
                        ELSE B.product_click_cnt - lag(B.product_click_cnt, 1, 0::bigint) OVER(PARTITION BY B.douyin_no, B.room_id ORDER BY B.timestamp) END ) AS product_click_cnt
                    , (CASE WHEN pay_cnt - lag(B.pay_cnt, 1, 0::bigint) OVER(PARTITION BY B.douyin_no, B.room_id ORDER BY B.timestamp) < 0 THEN 0
                        ELSE pay_cnt - lag(B.pay_cnt, 1, 0::bigint) OVER(PARTITION BY B.douyin_no, B.room_id ORDER BY B.timestamp) END ) AS deal_order_cnt
                FROM doudian_compass_live_list_detail B
                WHERE room_id IN {room_list}
            ),
            jl_minute AS (
                SELECT room_id, "time_stamp" AS "timestamp"
                    , total_live_like_cnt AS jl_like_cnt
                    , total_live_comment_cnt AS jl_comment_cnt
                    , live_form_submit_count AS jl_form_submit_cnt
                    , live_card_icon_component_click_count AS jl_card_click_cnt
                    , total_live_pcu AS jl_online_ucnt
                    , total_live_watch_cnt AS jl_watch_cnt
                FROM juliangzongheng_live_per_minute_metrics
                WHERE room_id IN {room_list}
            ),
            jl_live AS (
                SELECT room_id
                    , EXTRACT(EPOCH from TO_TIMESTAMP(CONCAT(SUBSTRING(local_time_minute, 1, 10), SUBSTRING(local_time_minute, 22)), 'YYYY-MM-DD HH24:MI')) AS timestamp
                    , live_share_count AS jl_share_cnt
                    , live_card_icon_component_show_count AS jl_card_show_cnt
                FROM juliangzongheng_live_minute
                WHERE room_id IN {room_list}
            ),
            tmp1 AS (
                SELECT
                    COALESCE(A.room_id, B.room_id) AS room_id
                    , COALESCE(A.douyin_no, B.douyin_no) AS douyin_no
                    , COALESCE(A.timestamp, B.timestamp) AS pg_timestamp
                    , A.live_show_cnt
                    , A.watch_ucnt
                    , A.avg_watch_duration
                    , A.gmv
                    , round(A.gpm/100::NUMERIC, 2) AS gpm
                    , B.like_cnt
                    , B.comment_cnt
                    , B.share_cnt
                    , B.incr_fans_cnt
                    , B.fans_club_ucnt
                FROM zhenti A
                FULL JOIN reptile_inter B ON A.room_id = B.room_id AND A.timestamp = B.timestamp
            ),
            tmp2 AS (
                SELECT
                    COALESCE(A.room_id, B.room_id) AS room_id
                    , COALESCE(A.douyin_no, B.douyin_no) AS douyin_no
                    , COALESCE(A.pg_timestamp, B.timestamp) AS pg_timestamp
                    , A.live_show_cnt AS exposure_cnt
                    , A.watch_ucnt
                    , B.watch_cnt
                    , A.avg_watch_duration
                    , A.gmv
                    , A.gpm
                    , A.like_cnt
                    , A.comment_cnt
                    , A.share_cnt
                    , A.incr_fans_cnt AS fans_ucnt
                    , A.fans_club_ucnt AS clubs_ucnt
                    , B.product_click_cnt
                    , B.deal_order_cnt
                FROM tmp1 A
                FULL JOIN list_detail B ON A.room_id = B.room_id AND A.pg_timestamp = B."timestamp"
            )
            SELECT
                COALESCE(A.room_id, B.room_id) AS room_id
                , A.douyin_no
                , COALESCE(A.pg_timestamp, B.timestamp) AS pg_timestamp
                , A.exposure_cnt
                , A.watch_cnt
                , A.like_cnt
                , A.comment_cnt
                , A.share_cnt
                , A.fans_ucnt
                , A.clubs_ucnt
                , A.product_click_cnt
                , A.deal_order_cnt
                , (CASE WHEN A.gmv < 0 THEN 0 ELSE A.gmv END) AS gmv
                , A.watch_ucnt
                , A.avg_watch_duration
                , A.gpm
                , B.jl_like_cnt
                , B.jl_comment_cnt
                , D.jl_share_cnt
                , B.jl_form_submit_cnt
                , D.jl_card_show_cnt
                , B.jl_card_click_cnt
                , B.jl_online_ucnt
                , B.jl_watch_cnt
                , to_timestamp(COALESCE(A.pg_timestamp, B.timestamp))
            FROM tmp2 A
            FULL JOIN jl_minute B ON A.room_id = B.room_id AND A.pg_timestamp = B.timestamp
            LEFT JOIN jl_live D ON A.room_id = D.room_id AND A.pg_timestamp = D.timestamp
            ORDER BY pg_timestamp


        '''

        df2 = self.execute_sql_stime(pg_sql_2, conn)

        m_df = pd.merge(df1, df2, how='outer', on=['room_id', 'douyin_no', 'pg_timestamp'])
        # print(m_df)
        print(m_df.columns.values)
        m_df.fillna(0, inplace = True)

        self.assert_info(m_df, 'exposure_cnt_x', 'exposure_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'watch_cnt_x', 'watch_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'like_cnt_x', 'like_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'comment_cnt_x', 'comment_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'share_cnt_x', 'share_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'fans_ucnt_x', 'fans_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'clubs_ucnt_x', 'clubs_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'product_click_cnt_x', 'product_click_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'deal_order_cnt_x', 'deal_order_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'watch_ucnt_x', 'watch_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'avg_watch_duration_x', 'avg_watch_duration_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'jl_like_cnt_x', 'jl_like_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'jl_comment_cnt_x', 'jl_comment_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'jl_share_cnt_x', 'jl_share_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'jl_form_submit_cnt_x', 'jl_form_submit_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'jl_card_show_cnt_x', 'jl_card_show_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'jl_card_click_cnt_x', 'jl_card_click_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'jl_online_ucnt_x', 'jl_online_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])
        self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp'])



    def test_vap_factor_correlation(self, conn):
        pg_sql_1 = '''
            SELECT *
            FROM vap_factor_correlation_analysis_new
            WHERE room_id = '7169011255443720991'
            -- AND timestamp = 1669168380
            LIMIT 100000
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)


        pg_sql_2 = '''
            WITH index_info AS (
                SELECT *
                    , to_timestamp(timestamp)
                    , live_show_cnt_tmp - LAG(live_show_cnt_tmp, 1, live_show_cnt_tmp) OVER(PARTITION BY room_id, douyin_no ORDER BY timestamp ) AS live_show_cnt
                    , watch_cnt_tmp - LAG(watch_cnt_tmp, 1, watch_cnt_tmp) OVER(PARTITION BY room_id, douyin_no ORDER BY timestamp ) AS watch_cnt
                FROM (
                    SELECT
                        A.company_id
                        , A.douyin_no
                        , A.room_id
                        , A.start_time
                        , F."timestamp"
                        , C.author_nick_name
                        , C.avg_watch_duration
                        , C.watch_ucnt - LAG(C.watch_ucnt, 1, 0) OVER(PARTITION BY C.room_id, C.douyin_no ORDER BY C."timestamp") AS watch_ucnt
                        , B.like_cnt
                        , B.comment_cnt
                        , B.share_cnt
                        , B.incr_fans_cnt
                        , B.fans_club_ucnt
                        , COALESCE(D.live_show_cnt, 0) AS live_show_cnt_tmp
                        , COALESCE(D.live_show_cnt, 0) * COALESCE(E.watch_cnt_show_ratio, 0) AS watch_cnt_tmp
                        , F.min_gmv/100::NUMERIC AS min_gmv
                        , NULL AS marketing_event
                        , NULL AS tag_name
                    FROM vap_douyin_live_rooms A
                    LEFT JOIN doudian_compass_reptile_transaction_amount F ON A.room_id = F.room_id
                    LEFT JOIN doudian_compass_reptile_interaction B ON A.room_id = B.room_id AND F."timestamp" = B."timestamp"
                    LEFT JOIN doudian_compass_reptile_room_real_time C ON A.room_id = C.room_id AND F."timestamp" = C."timestamp"
                    LEFT JOIN doudian_compass_live_core_index D ON A.room_id = D.room_id AND F."timestamp" = D."timestamp"
                    LEFT JOIN doudian_compass_screen_zhengti E ON A.room_id = E.room_id AND F."timestamp" = E."timestamp"
                    WHERE A.start_time < floor(extract(epoch from date_trunc('day', current_date)))
                        AND A.douyin_no <> 'demo'
                        AND A.is_delete <> 1
                        AND A.status <> 1
                        AND A.room_id = '7169011255443720991'
                ) t
            ),
            market_event AS (
                SELECT DISTINCT
                    A.company_id
                    , A.douyin_no
                    , A.room_id
                    , B."timestamp"
                    , A.start_time
                    , to_timestamp(B.timestamp)
                    , NULL AS author_nick_name
                    , 0 AS avg_watch_duration
                    , 0 AS watch_ucnt
                    , 0 AS like_cnt
                    , 0 AS comment_cnt
                    , 0 AS share_cnt
                    , 0 AS incr_fans_cnt
                    , 0 AS fans_club_ucnt
                    , 0 AS live_show_cnt
                    , 0 AS watch_cnt
                    , 0 AS min_gmv
                    -- 一分钟包括多个marketing，拆分多行
                    , unnest(string_to_array(
                        concat_ws(','
                                , (CASE WHEN J.room_id IS NOT NULL THEN '秒杀' END)
                                , (CASE WHEN M.room_id IS NOT NULL THEN '福袋' END)
                                , (CASE WHEN N.room_id IS NOT NULL THEN '上架' END)
                        ), ',')) AS marketing_event
                    , NULL AS tag_name
                FROM vap_douyin_live_rooms A
                LEFT JOIN doudian_compass_reptile_interaction B ON A.room_id = B.room_id
                LEFT JOIN doudian_compass_reptiel_seckill J ON B.room_id = J.room_id AND B."timestamp" = J."timestamp"
                LEFT JOIN doudian_compass_reptile_bless M ON B.room_id = M.room_id AND B."timestamp" = (M."timestamp" - M."timestamp" % 60)
                LEFT JOIN doudian_compass_reptile_goods_shelves N ON B.room_id = N.room_id AND B."timestamp" = (N."timestamp" - N."timestamp" % 60)
                WHERE A.start_time < floor(extract(epoch from date_trunc('day', current_date)))
                    AND A.douyin_no <> 'demo'
                    AND A.is_delete <> 1
                    AND A.status <> 1
                    AND A.room_id = '7169011255443720991'
            )
            SELECT
                company_id
                , douyin_no
                , room_id
                , "timestamp"
                , to_timestamp(timestamp)
                , live_show_cnt AS exposure_cnt
                , round(watch_cnt) AS watch_cnt
                , watch_ucnt
                , avg_watch_duration
                , like_cnt
                , comment_cnt
                , share_cnt
                , fans_club_ucnt AS fans_ucnt
                , incr_fans_cnt AS clubs_ucnt
                , 0 AS product_click_cnt
                , 0 AS deal_order_cnt
                , min_gmv AS gmv
                , marketing_event
                , tag_name
            FROM (
                SELECT
                    company_id
                    , douyin_no
                    , room_id
                    , "timestamp"
                    , start_time
                    , author_nick_name
                    , avg_watch_duration
                    , watch_ucnt
                    , like_cnt
                    , comment_cnt
                    , share_cnt
                    , incr_fans_cnt
                    , fans_club_ucnt
                    , live_show_cnt
                    , watch_cnt
                    , min_gmv
                    , marketing_event
                    , tag_name
                FROM index_info
                UNION ALL
                SELECT
                    company_id
                    , douyin_no
                    , room_id
                    , "timestamp"
                    , start_time
                    , author_nick_name
                    , avg_watch_duration
                    , watch_ucnt
                    , like_cnt
                    , comment_cnt
                    , share_cnt
                    , incr_fans_cnt
                    , fans_club_ucnt
                    , live_show_cnt
                    , watch_cnt
                    , min_gmv
                    , marketing_event
                    , tag_name
                FROM market_event
                UNION ALL
                SELECT
                    correct.company_id
                    , correct.douyin_no
                    , correct.room_id
                    , correct."timestamp"
                    , NULL AS start_time
                    , NULL AS author_nick_name
                    , 0 AS avg_watch_duration
                    , 0 AS watch_ucnt
                    , 0 AS like_cnt
                    , 0 AS comment_cnt
                    , 0 AS share_cnt
                    , 0 AS incr_fans_cnt
                    , 0 AS fans_club_ucnt
                    , 0 AS live_show_cnt
                    , 0 AS watch_cnt
                    , 0 AS min_gmv
                    , NULL AS marketing_event
                    , tag.name AS tag_name
                FROM (
                    SELECT A.company_id, A.douyin_no, A.room_id, (to_timestamp/1000 - to_timestamp/1000 % 60) AS "timestamp"
                        , json_array_elements(tags::json)->>'tag_id' AS tag_id
                    FROM vap_douyin_live_rooms A
                    INNER JOIN vap_correct_result B ON A.room_id = B.room_id
                    WHERE tags <> '[]'
                        AND A.start_time < floor(extract(epoch from date_trunc('day', current_date)))
                        AND A.douyin_no <> 'demo'
                        AND A.is_delete <> 1
                        AND A.status <> 1
                        AND A.room_id = '7169011255443720991'
                ) correct INNER JOIN vap_mango_tag tag ON correct.tag_id = tag.id
            ) t
            -- WHERE timestamp = 1669168380

        '''

        df2 = self.execute_sql_stime(pg_sql_2, conn)


        m_df = pd.merge(df1, df2, how='outer', on=['room_id', 'douyin_no', 'timestamp', 'marketing_event', 'tag_name'])
        print(m_df)
        m_df.to_csv("111.csv")
        print(m_df.columns.values)
        m_df.fillna(0, inplace = True)


        self.assert_info(m_df, 'exposure_cnt_x', 'exposure_cnt_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'watch_cnt_x', 'watch_cnt_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'watch_ucnt_x', 'watch_ucnt_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'avg_watch_duration_x', 'avg_watch_duration_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'like_cnt_x', 'like_cnt_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'comment_cnt_x', 'comment_cnt_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'share_cnt_x', 'share_cnt_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'fans_ucnt_x', 'fans_ucnt_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'clubs_ucnt_x', 'clubs_ucnt_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'product_click_cnt_x', 'product_click_cnt_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'deal_order_cnt_x', 'deal_order_cnt_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['room_id', 'douyin_no', 'timestamp', 'to_timestamp'])



    def test_check_field_control(self, conn):
        pg_sql_roomlist = '''
            SELECT A.room_id
            FROM vap_douyin_live_rooms A
            WHERE start_time > floor(extract(epoch from date_trunc('day', current_date)))
                AND end_time > 0
                AND status <> 1
                AND is_delete <> 1

        '''
        df_roomlist = self.execute_sql_stime(pg_sql_roomlist, conn)
        roomlist = df_roomlist['room_id'].tolist()
        print(roomlist)

        pg_sql_1 = f'''
            SELECT *
            FROM vap_field_control
            WHERE room_id IN {tuple(roomlist)}
            ORDER BY pg_timestamp
        '''

        df1 = self.execute_sql_stime(pg_sql_1, conn)

        pg_sql_2 = f'''
            WITH zhenti AS (
                SELECT * FROM doudian_compass_screen_zhengti WHERE room_id IN {tuple(roomlist)}
            ),
            live_list_detail AS (
                SELECT * FROM doudian_compass_live_list_detail WHERE room_id IN {tuple(roomlist)}
            ),
            live_gmv_interaction AS (
                SELECT * FROM doudian_compass_live_gmv_interaction WHERE room_id IN {tuple(roomlist)}
            ),
            reptile_interaction AS (
                SELECT * FROM doudian_compass_reptile_interaction WHERE room_id IN {tuple(roomlist)}
            ),
            jl_minute AS (
                SELECT room_id
                    , to_timestamp("time_stamp")
                    , "time_stamp" AS timestamp
                    , total_live_like_cnt AS jl_like_cnt
                    , total_live_comment_cnt AS jl_comment_cnt
                    , live_form_submit_count AS incr_jl_form_submit_cnt
                    , live_card_icon_component_click_count AS jl_card_click_cnt
                    , total_live_watch_cnt AS jl_watch_cnt
                FROM juliangzongheng_live_per_minute_metrics
                WHERE room_id IN {tuple(roomlist)}
            ),
            jl_live AS (
                SELECT room_id
                    , EXTRACT(EPOCH from TO_TIMESTAMP(CONCAT(SUBSTRING(local_time_minute, 1, 10), SUBSTRING(local_time_minute, 22)), 'YYYY-MM-DD HH24:MI')) AS timestamp
                    , live_share_count AS jl_share_cnt
                    , live_card_icon_component_show_count AS jl_card_show_cnt
                FROM juliangzongheng_live_minute
                WHERE room_id IN {tuple(roomlist)}
            ),
            control_sum AS (
                SELECT A.room_id
                    , A.create_time - A.create_time%60 AS timestamp
                    , to_timestamp(A.create_time - A.create_time%60)
                    , A.live_form_submit_count AS jl_form_submit_cnt
                    , (CASE WHEN A.total_live_watch_cnt=0 THEN 0 ELSE round(A.total_live_comment_cnt / A.total_live_watch_cnt::NUMERIC * 100, 2) END) AS jl_comment_cnt_ratio
                    , (CASE WHEN A.total_live_watch_cnt=0 THEN 0 ELSE round(B.live_share_count / A.total_live_watch_cnt::NUMERIC * 100, 2) END) AS jl_share_cnt_ratio
                    , (CASE WHEN A.total_live_watch_cnt=0 THEN 0 ELSE round(A.total_live_like_cnt / A.total_live_watch_cnt::NUMERIC * 100, 2) END) AS jl_like_cnt_ratio
                    , (CASE WHEN B.live_card_icon_component_show_count = 0 THEN 0 ELSE round(A.live_card_icon_component_click_count / B.live_card_icon_component_show_count::NUMERIC * 100, 2) END) AS jl_card_click_cnt_ratio
                    , (CASE WHEN A.live_card_icon_component_click_count=0 THEN 0 ELSE round(A.live_form_submit_count / A.live_card_icon_component_click_count::NUMERIC * 100, 2) END) AS jl_form_submit_cnt_ratio
                    , ROW_NUMBER() OVER(PARTITION BY A.room_id, (A.create_time - A.create_time%60) ORDER BY A.create_time) rn
                FROM juliangzongheng_live_big_screen_top_card A
                LEFT JOIN juliangzongheng_live_analysis_list B ON A.room_id = B.room_id
                WHERE A.room_id IN {tuple(roomlist)}
            ),
            juliang AS (
                SELECT
                    COALESCE(A.room_id, B.room_id) AS room_id
                    , COALESCE(A."timestamp", B."timestamp") AS pg_timestamp
                    , A.jl_form_submit_cnt
                    , A.jl_comment_cnt_ratio
                    , jl_like_cnt_ratio
                    , A.jl_form_submit_cnt_ratio
                    , A.jl_share_cnt_ratio
                    , A.jl_card_click_cnt_ratio
                    , B.jl_like_cnt
                    , B.jl_comment_cnt
                    , B.incr_jl_form_submit_cnt
                    , B.jl_card_click_cnt
                    , B.jl_watch_cnt
                    , C.jl_share_cnt
                    , C.jl_card_show_cnt
                FROM control_sum A
                FULL JOIN jl_minute B ON A.room_id = B.room_id AND A.timestamp = B."timestamp"
                LEFT JOIN jl_live C ON COALESCE(A.room_id, B.room_id) = C.room_id AND COALESCE(A."timestamp", B."timestamp") = C."timestamp"
            ),
            tmp_1 AS (
                SELECT
                    COALESCE(A.room_id, B.room_id) AS room_id
                    , COALESCE(A.douyin_no, B.douyin_no) AS douyin_no
                    , COALESCE(A."timestamp", B."timestamp") AS pg_timestamp
                    , to_timestamp(COALESCE(A."timestamp", B."timestamp")::int4)
                    , A.watch_ucnt
                    , A.avg_watch_duration
                    , round((CASE WHEN A.watch_ucnt = 0 THEN 0 ELSE A.follow_anchor_ucnt / A.watch_ucnt::NUMERIC * 100 END), 2) AS add_fans_ratio
                    , round((CASE WHEN A.watch_ucnt = 0 THEN 0 ELSE A.fans_club_join_ucnt / A.watch_ucnt::NUMERIC * 100 END), 2) AS add_club_ratio
                    , A.gmv
                    , A.gpm
                    , A.live_show_cnt - LAG(A.live_show_cnt, 1, 0) OVER(PARTITION BY A.room_id, A.douyin_no ORDER BY A."timestamp") AS exposure_cnt
                    , A.watch_ucnt - LAG(A.watch_ucnt, 1, 0::bigint) OVER(PARTITION BY A.room_id, A.douyin_no ORDER BY A."timestamp") AS incr_watch_ucnt
                    , A.gmv - LAG(A.gmv, 1, 0::bigint) OVER(PARTITION BY A.room_id, A.douyin_no ORDER BY A."timestamp") AS incr_gmv
                    , round((CASE WHEN A.live_show_cnt = 0 THEN 0 ELSE B.watch_cnt / A.live_show_cnt::NUMERIC * 100 END), 2) AS watch_ucnt_ratio
                    , round((CASE WHEN B.watch_cnt = 0 THEN 0 ELSE B.product_click_cnt / B.watch_cnt::NUMERIC * 100 END), 2) AS product_click_ratio
                    , round((CASE WHEN B.watch_cnt = 0 THEN 0 ELSE B.pay_cnt / B.watch_cnt::NUMERIC * 100 END), 2) AS pay_cnt_ratio
                    , B.product_click_cnt - LAG(B.product_click_cnt, 1, 0::bigint) OVER(PARTITION BY B.room_id, B.douyin_no ORDER BY B."timestamp") AS product_click_cnt
                    , B.pay_cnt - LAG(B.pay_cnt, 1, 0::bigint) OVER(PARTITION BY B.room_id, B.douyin_no ORDER BY B."timestamp") AS deal_order_cnt
                FROM zhenti A
                FULL JOIN live_list_detail B ON A.room_id = B.room_id AND A."timestamp" = B."timestamp"
            ),
            tmp_2 AS (
                SELECT A.*, round((CASE WHEN A.watch_ucnt = 0 THEN 0 ELSE C.interactive_ucnt / A.watch_ucnt::NUMERIC * 100 END), 2) AS interactive_ratio
                FROM tmp_1 A
                FULL JOIN live_gmv_interaction C ON A.room_id = C.room_id AND A.pg_timestamp = C."timestamp"
            ),
            tmp_3 AS (
                SELECT A.*
                    , D.incr_fans_cnt AS fans_ucnt
                    , D.fans_club_ucnt AS clubs_ucnt
                FROM tmp_2 A
                FULL JOIN reptile_interaction D ON A.room_id = D.room_id AND A.pg_timestamp = D."timestamp"
            )
            SELECT
                COALESCE(A.room_id, B.room_id) AS room_id
                , COALESCE(A."pg_timestamp", B."pg_timestamp") AS pg_timestamp
                , to_timestamp(COALESCE(A."pg_timestamp", B."pg_timestamp")::int4)
                , (SELECT douyin_no FROM vap_douyin_live_rooms WHERE room_id = COALESCE(A.room_id, B.room_id)) AS douyin_no
                , watch_ucnt
                , avg_watch_duration
                , watch_ucnt_ratio
                , product_click_ratio
                , pay_cnt_ratio
                , add_fans_ratio
                , add_club_ratio
                , interactive_ratio
                , (CASE WHEN gmv < 0 THEN 0 ELSE gmv END) AS gmv
                , gpm
                , (CASE WHEN exposure_cnt < 0 THEN 0 ELSE exposure_cnt END) AS exposure_cnt
                , (CASE WHEN incr_watch_ucnt < 0 THEN 0 ELSE incr_watch_ucnt END) AS incr_watch_ucnt
                , fans_ucnt
                , clubs_ucnt
                , product_click_cnt
                , deal_order_cnt
                , incr_gmv
                , jl_form_submit_cnt
                , jl_comment_cnt_ratio
                , jl_like_cnt_ratio
                , jl_form_submit_cnt_ratio
                , jl_share_cnt_ratio
                , jl_card_click_cnt_ratio
                , jl_like_cnt
                , jl_comment_cnt
                , jl_share_cnt
                , incr_jl_form_submit_cnt
                , jl_card_show_cnt
                , jl_card_click_cnt
                , jl_watch_cnt
            FROM tmp_3 A
            FULL JOIN juliang B ON A.room_id = B.room_id AND A.pg_timestamp = B.pg_timestamp
            ORDER BY COALESCE(A."pg_timestamp", B."pg_timestamp")

        '''
        print(pg_sql_2)
        df2 = self.execute_sql_stime(pg_sql_2, conn)


        m_df = pd.merge(df1, df2, how='left', on=['room_id', 'douyin_no', 'pg_timestamp'])
        print(m_df)
        print(m_df.columns.values)
        m_df.fillna(0, inplace = True)

        self.assert_info(m_df, 'watch_ucnt_x', 'watch_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'avg_watch_duration_x', 'avg_watch_duration_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'watch_ucnt_ratio_x', 'watch_ucnt_ratio_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'product_click_ratio_x', 'product_click_ratio_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'pay_cnt_ratio_x', 'pay_cnt_ratio_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'add_fans_ratio_x', 'add_fans_ratio_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'add_club_ratio_x', 'add_club_ratio_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'interactive_ratio_x', 'interactive_ratio_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'exposure_cnt_x', 'exposure_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'incr_watch_ucnt_x', 'incr_watch_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'fans_ucnt_x', 'fans_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'clubs_ucnt_x', 'clubs_ucnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'product_click_cnt_x', 'product_click_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'deal_order_cnt_x', 'deal_order_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'incr_gmv_x', 'incr_gmv_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_form_submit_cnt_x', 'jl_form_submit_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_comment_cnt_ratio_x', 'jl_comment_cnt_ratio_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_like_cnt_ratio_x', 'jl_like_cnt_ratio_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_form_submit_cnt_ratio_x', 'jl_form_submit_cnt_ratio_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_share_cnt_ratio_x', 'jl_share_cnt_ratio_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_card_click_cnt_ratio_x', 'jl_card_click_cnt_ratio_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_like_cnt_x', 'jl_like_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_comment_cnt_x', 'jl_comment_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_share_cnt_x', 'jl_share_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'incr_jl_form_submit_cnt_x', 'incr_jl_form_submit_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_card_show_cnt_x', 'jl_card_show_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_card_click_cnt_x', 'jl_card_click_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['room_id', 'douyin_no', 'pg_timestamp', 'to_timestamp'])




    def test_check_vap_diagnose_room_alert_avg_flow(self, conn):
        pg_sql_1 = '''
            SELECT * FROM vap_diagnose_room_alert_avg
            WHERE etl_flag= '3' AND start_time  > floor(extract(epoch from date_trunc('day', current_date))) - 3600 * 24
            ORDER BY start_time DESC
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        pg_sql_2 = '''
            WITH alert AS (
                SELECT live_id, MAX(num) AS num FROM stream_asr_alert GROUP BY live_id
            )
            SELECT
                A.company_id
                , A.douyin_no
                , A.room_id
                , A.start_time
                , A.end_time
                , round(C.num/((A.end_time - A.start_time)/60)::NUMERIC * 60) AS alert_havg
                , 3 AS etl_flag
                , A.data_report_status
            FROM vap_douyin_live_rooms A
            LEFT JOIN vap_review_room B ON A.room_id = B.room_id
            LEFT JOIN alert C ON A.id = C.live_id
            WHERE A.start_time  > floor(extract(epoch from date_trunc('day', current_date))) - 3600 * 24
                AND A.end_time > 0
                AND C.num IS NOT NULL

        '''
        df2 = self.execute_sql_stime(pg_sql_2, conn)

        m_df = pd.merge(df1, df2, how='left', on=['room_id', 'douyin_no'])
        print(m_df)
        print(m_df.columns.values)
        m_df.fillna(0, inplace = True)


        self.assert_info(m_df, 'etl_flag_x', 'etl_flag_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'company_id_x', 'company_id_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'start_time_x', 'start_time_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'end_time_x', 'end_time_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'alert_havg_x', 'alert_havg_y', col_list=['room_id', 'douyin_no'])



    def test_check_vap_diagnose_room_alert_avg(self, conn):
        pg_sql_1 = '''
            SELECT * FROM vap_diagnose_room_alert_avg WHERE etl_flag= '4' ORDER BY start_time DESC
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        pg_sql_2 = '''
            WITH alert AS (
                SELECT live_id, MAX(num) AS num FROM stream_asr_alert GROUP BY live_id
            )
            SELECT A.company_id
                , B.douyin_no
                , B.room_id
                , B.start_time
                , B.end_time
                , round(C.num/((A.end_time - A.start_time)/60)::NUMERIC * 60) AS alert_havg
                , 4 AS etl_flag
            FROM vap_douyin_live_rooms A
            LEFT JOIN vap_review_room B ON A.room_id = B.room_id
            LEFT JOIN alert C ON A.id = C.live_id
            WHERE A.start_time > 1668441600 AND B.etl_flag = 4 AND C.live_id IS NOT NULL
            ORDER BY start_time DESC

        '''
        df2 = self.execute_sql_stime(pg_sql_2, conn)

        m_df = pd.merge(df1, df2, how='left', on=['room_id', 'douyin_no'])
        print(m_df)
        print(m_df.columns.values)
        m_df.fillna(0, inplace = True)


        self.assert_info(m_df, 'etl_flag_x', 'etl_flag_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'company_id_x', 'company_id_y', col_list=['room_id', 'douyin_no'])
        # self.assert_info(m_df, 'start_time_x', 'start_time_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'end_time_x', 'end_time_y', col_list=['room_id', 'douyin_no'])
        self.assert_info(m_df, 'alert_havg_x', 'alert_havg_y', col_list=['room_id', 'douyin_no'])




    def test_check_vap_diagnose_room_avg(self, conn):
        pg_sql_1 = '''
            SELECT * FROM vap_diagnose_room_avg
            WHERE etl_flag = '4' AND end_time >= floor(extract(epoch from date_trunc('day', current_date))) - 3600*24*30
            ORDER BY start_time DESC
        '''
        # df1 = self.execute_sql_stime(pg_sql_1, conn)
        df1 = self.execute_sql_stime(pg_sql_1, conn)


        pg_sql_2 = '''
            WITH reptile_bless AS (
                SELECT room_id, SUM(bless_num) AS bless_count FROM doudian_compass_reptile_bless GROUP BY room_id
            ),
            reptiel_seckill AS (
                SELECT room_id, count(*) AS seckill_count FROM doudian_compass_reptiel_seckill GROUP BY room_id
            ),
            reptile_goods_shelves AS (
                SELECT room_id, SUM(shelves_count) AS shelves_count
                FROM (
                    SELECT room_id, "timestamp", create_time, other_num, id, prod_name
                        , (CASE WHEN other_num = 0 THEN 1
                            WHEN other_num > 0 THEN other_num + count(*) OVER(PARTITION BY room_id, "timestamp", create_time)
                        END) AS shelves_count
                        , ROW_NUMBER() OVER(PARTITION BY room_id, "timestamp", create_time ORDER BY create_time) srank
                    FROM doudian_compass_reptile_goods_shelves
                ) t WHERE srank = 1
                GROUP BY room_id
            ),
            reptile_explain AS (
                SELECT room_id, count(*) AS explain_count FROM doudian_compass_reptile_explain GROUP BY room_id
            )
            SELECT douyin_no
                , A.room_id
                , start_time
                , end_time
                , live_duration
                , to_timestamp(start_time)
                , gpm
                , avg_watch_duration
                , round(CASE WHEN live_duration=0 then -1 ELSE exposure_cnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS exposure_cnt_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE watch_ucnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS watch_ucnt_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE fans_ucnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS fans_ucnt_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE clubs_ucnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS clubs_ucnt_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE product_click_cnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS product_click_cnt_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE deal_order_cnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS deal_order_cnt_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE gmv / (A.end_time - A.start_time)::NUMERIC * 3600 END, 2) AS gmv_havg
                , (CASE
                    WHEN live_duration = 0 THEN -1
                    ELSE round(B.bless_count / (A.end_time - A.start_time)::NUMERIC * 3600)
                   END ) AS bless_havg
                , (CASE
                    WHEN live_duration = 0 THEN -1
                    ELSE round(C.seckill_count / (A.end_time - A.start_time)::NUMERIC * 3600)
                   END ) AS seckill_havg
                , (CASE
                    WHEN live_duration = 0 THEN -1
                    ELSE round(shelves_count / (A.end_time - A.start_time)::NUMERIC * 3600)
                   END ) AS shelves_havg
                , (CASE
                    WHEN live_duration = 0 THEN -1
                    ELSE round(E.explain_count / (A.end_time - A.start_time)::NUMERIC * 3600)
                   END) AS explain_havg
                , (CASE WHEN exposure_cnt <= 0 THEN -1 ELSE round(watch_cnt / exposure_cnt::NUMERIC *100, 2) END) AS exposure_watch_ratio
                , (CASE WHEN watch_ucnt <= 0 THEN -1 ELSE round(interact_ucnt / watch_ucnt::NUMERIC *100, 2) END) AS watch_interact_ratio
                , (CASE WHEN watch_ucnt <= 0 THEN -1 ELSE round(fans_ucnt / watch_ucnt::NUMERIC *100, 2) END) AS watch_fans_ratio
                , (CASE WHEN watch_ucnt <= 0 THEN -1 ELSE round(clubs_ucnt / watch_ucnt::NUMERIC *100, 2) END) AS watch_clubs_ratio
                , (CASE WHEN watch_cnt <= 0 THEN -1 ELSE round(product_click_cnt / watch_cnt::NUMERIC *100, 2) END) AS watch_click_ratio
                , (CASE WHEN watch_cnt <= 0 THEN -1 ELSE round(deal_order_cnt / watch_cnt::NUMERIC *100, 2) END) AS watch_deal_ratio
                , jl_avg_watch_duration
                , round(CASE WHEN live_duration=0 then -1 ELSE jl_watch_cnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS jl_watch_cnt_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE jl_like_cnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS jl_like_cnt_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE jl_comment_cnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS jl_comment_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE jl_share_cnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS jl_share_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE jl_form_submit_cnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS jl_form_submit_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE jl_card_show_cnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS jl_card_show_havg
                , round(CASE WHEN live_duration=0 then -1 ELSE jl_card_click_cnt / (A.end_time - A.start_time)::NUMERIC * 3600 END) AS jl_card_click_havg
                , round(CASE WHEN jl_watch_cnt=0 then -1 ELSE jl_comment_cnt / jl_watch_cnt::NUMERIC * 100 END, 2) AS jl_watch_comment_ratio
                , round(CASE WHEN jl_watch_cnt=0 then -1 ELSE jl_share_cnt / jl_watch_cnt::NUMERIC * 100 END, 2) AS jl_watch_share_ratio
                , round(CASE WHEN jl_watch_cnt=0 then -1 ELSE jl_like_cnt / jl_watch_cnt::NUMERIC * 100 END, 2) AS jl_watch_like_ratio
                , round(CASE WHEN jl_watch_cnt=0 then -1 ELSE jl_fans_ucnt / jl_watch_cnt::NUMERIC * 100 END, 2) AS jl_watch_fans_ratio
                , round(CASE WHEN jl_watch_cnt=0 then -1 ELSE jl_clubs_ucnt / jl_watch_cnt::NUMERIC * 100 END, 2) AS jl_watch_clubs_ratio
                , round(CASE WHEN jl_watch_cnt=0 then -1 ELSE jl_card_show_cnt / jl_watch_cnt::NUMERIC * 100 END, 2) AS jl_watch_card_icon_show_ratio
                , round(CASE WHEN jl_watch_cnt=0 then -1 ELSE jl_card_click_cnt / jl_watch_cnt::NUMERIC * 100 END, 2) AS jl_watch_card_icon_click_ratio
                , round(CASE WHEN jl_watch_cnt=0 then -1 ELSE jl_form_submit_cnt / jl_watch_cnt::NUMERIC * 100 END, 2) AS jl_watch_submit_ratio
                , round(CASE WHEN jl_card_click_cnt=0 then -1 ELSE jl_form_submit_cnt / jl_card_click_cnt::NUMERIC * 100 END, 2) AS jl_submit_component_click_ratio
            FROM vap_review_room A
            LEFT JOIN reptile_bless B ON A.room_id = B.room_id
            LEFT JOIN reptiel_seckill C ON A.room_id = C.room_id
            LEFT JOIN reptile_goods_shelves D ON A.room_id = D.room_id
            LEFT JOIN reptile_explain E ON A.room_id = E.room_id
            WHERE etl_flag = 4 AND A.start_time > 1668441600
                AND end_time >= floor(extract(epoch from date_trunc('day', current_date))) - 3600*24*30
            ORDER BY start_time DESC


        '''
        df2 = self.execute_sql_stime(pg_sql_2, conn)


        m_df = pd.merge(df1, df2, how='outer', on=['room_id', 'douyin_no'])
        print(m_df)
        print(m_df.columns.values)
        # m_df['start_time_y'] = m_df['start_time_y'].astype(int)
        # m_df['end_time_y'] = m_df['end_time_y'].astype(int)
        # m_df['live_duration_y'] = m_df['live_duration_y'].astype(int)

        # print(m_df.dtypes)
        m_df.fillna(-1, inplace = True)
        # m_df['room_id'] = m_df['room_id'].map(lambda x: str(x) + '\t')


        # self.assert_info(m_df, 'start_time_x', 'start_time_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        # self.assert_info(m_df, 'end_time_x', 'end_time_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        # self.assert_info(m_df, 'live_duration_x', 'live_duration_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'avg_watch_duration_x', 'avg_watch_duration_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'exposure_cnt_havg_x', 'exposure_cnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_ucnt_havg_x', 'watch_ucnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'fans_ucnt_havg_x', 'fans_ucnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'clubs_ucnt_havg_x', 'clubs_ucnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'product_click_cnt_havg_x', 'product_click_cnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'deal_order_cnt_havg_x', 'deal_order_cnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'gmv_havg_x', 'gmv_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'bless_havg_x', 'bless_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'seckill_havg_x', 'seckill_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'shelves_havg_x', 'shelves_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'explain_havg_x', 'explain_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'exposure_watch_ratio_x', 'exposure_watch_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_interact_ratio_x', 'watch_interact_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_fans_ratio_x', 'watch_fans_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_clubs_ratio_x', 'watch_clubs_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_click_ratio_x', 'watch_click_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_deal_ratio_x', 'watch_deal_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_avg_watch_duration_x', 'jl_avg_watch_duration_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_cnt_havg_x', 'jl_watch_cnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_like_cnt_havg_x', 'jl_like_cnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_comment_havg_x', 'jl_comment_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_share_havg_x', 'jl_share_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_form_submit_havg_x', 'jl_form_submit_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_card_show_havg_x', 'jl_card_show_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_card_click_havg_x', 'jl_card_click_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_comment_ratio_x', 'jl_watch_comment_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_share_ratio_x', 'jl_watch_share_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_like_ratio_x', 'jl_watch_like_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        # self.assert_info(m_df, 'jl_watch_fans_ratio_x', 'jl_watch_fans_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        # self.assert_info(m_df, 'jl_watch_clubs_ratio_x', 'jl_watch_clubs_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_card_icon_show_ratio_x', 'jl_watch_card_icon_show_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_card_icon_click_ratio_x', 'jl_watch_card_icon_click_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_submit_ratio_x', 'jl_watch_submit_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_submit_component_click_ratio_x', 'jl_submit_component_click_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])






    def test_check_vap_diagnose_room_avg_flow(self, conn):
        pg_sql_1 = '''
            SELECT * FROM vap_diagnose_room_avg
            WHERE etl_flag = '3' AND start_time > floor(extract(epoch from date_trunc('day', current_date))) - 3600 * 24
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        pg_room_list = '''
            SELECT room_id
            FROM vap_douyin_live_rooms A
            WHERE start_time > floor(extract(epoch from date_trunc('day', current_date))) - 3600 * 24
                AND end_time > 0
                AND douyin_no <> 'demo'
                AND status <> 1
                AND is_delete <> 1
        '''
        df_roomlist = self.execute_sql_stime(pg_room_list, conn)
        room_list = tuple(df_roomlist['room_id'].tolist())
        print(room_list)


        pg_sql_2 = f'''
            WITH reptile_bless AS (
                SELECT room_id, SUM(bless_num) AS bless_count FROM doudian_compass_reptile_bless WHERE room_id IN ('7202131313183165221', '7202501376566397752', '7202411137676118844', '7202040840406010683', '7202468170718874424') GROUP BY room_id
            ),
            reptiel_seckill AS (
                SELECT room_id, count(*) AS seckill_count FROM doudian_compass_reptiel_seckill WHERE room_id IN ('7202131313183165221', '7202501376566397752', '7202411137676118844', '7202040840406010683', '7202468170718874424') GROUP BY room_id
            ),
            reptile_goods_shelves AS (
                SELECT room_id, SUM(shelves_count) AS shelves_count
                FROM (
                    SELECT room_id, "timestamp", create_time, other_num, id, prod_name
                        , (CASE WHEN other_num = 0 THEN 1
                            WHEN other_num > 0 THEN other_num + count(*) OVER(PARTITION BY room_id, "timestamp", create_time)
                        END) AS shelves_count
                        , ROW_NUMBER() OVER(PARTITION BY room_id, "timestamp", create_time ORDER BY create_time) srank
                    FROM doudian_compass_reptile_goods_shelves
                    WHERE room_id IN {room_list}
                ) t WHERE srank = 1
                GROUP BY room_id
            ),
            reptile_explain AS (
                SELECT room_id, count(*) AS explain_count FROM doudian_compass_reptile_explain WHERE room_id IN ('7202131313183165221', '7202501376566397752', '7202411137676118844', '7202040840406010683', '7202468170718874424') GROUP BY room_id
            ),
            list_detail AS (
                SELECT * FROM (
                    SELECT douyin_no, room_id, timestamp, product_click_cnt, pay_cnt, watch_cnt
                        , ROW_NUMBER() OVER(PARTITION BY room_id, douyin_no ORDER BY timestamp DESC) AS s_rank
                    FROM doudian_compass_live_list_detail
                    WHERE room_id IN {room_list}
                ) t WHERE s_rank = 1
            ),
            gmv_inter AS (
                SELECT * FROM (
                    SELECT douyin_no, room_id, interactive_ucnt
                        , ROW_NUMBER() OVER(PARTITION BY douyin_no, room_id ORDER BY timestamp DESC) AS s_rank
                    FROM doudian_compass_live_gmv_interaction
                    WHERE room_id IN {room_list}
                ) t WHERE s_rank = 1
            ),
            zhengti AS (
                SELECT * FROM (
                    SELECT A.douyin_no
                        , A.room_id
                        , ROW_NUMBER() OVER(PARTITION BY A.room_id, A.douyin_no ORDER BY A."timestamp" DESC) AS srank
                        , A.gpm/100::NUMERIC AS gpm
                        , A.avg_watch_duration
                        , A.live_show_cnt
                        , A.watch_ucnt
                        , A.follow_anchor_ucnt
                        , A.fans_club_join_ucnt
                        , A.gmv
                    FROM doudian_compass_screen_zhengti A
                    WHERE room_id IN {room_list}
                ) t WHERE srank = 1
            ),
            jl_top_card AS (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id ORDER BY create_time DESC) AS srank
                    FROM juliangzongheng_live_big_screen_top_card
                    WHERE room_id IN {room_list}
                ) t WHERE srank = 1
            ),
            jl_analysis_list AS (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id ORDER BY update_time DESC) AS srank
                    FROM juliangzongheng_live_analysis_list
                    WHERE room_id IN {room_list}
                ) t WHERE srank = 1
            )
            SELECT A.room_id
                , A.douyin_no
                , A.start_time
                , A.end_time
                , A.live_duration AS live_duration
                , to_timestamp(A.start_time)
                , B.gpm
                , B.avg_watch_duration
                , round(B.live_show_cnt/(A.end_time - A.start_time)::NUMERIC * 3600) AS exposure_cnt_havg
                , round(B.watch_ucnt/(A.end_time - A.start_time)::NUMERIC * 3600) AS watch_ucnt_havg
                , round(B.follow_anchor_ucnt/(A.end_time - A.start_time)::NUMERIC * 3600) AS fans_ucnt_havg
                , round(B.fans_club_join_ucnt/(A.end_time - A.start_time)::NUMERIC * 3600) AS clubs_ucnt_havg
                , round(M.product_click_cnt/(A.end_time - A.start_time)::NUMERIC * 3600) AS product_click_cnt_havg
                , round(M.pay_cnt/(A.end_time - A.start_time)::NUMERIC * 3600) AS deal_order_cnt_havg
                , round((B.gmv/(A.end_time - A.start_time)::NUMERIC * 3600)/100, 2) AS gmv_havg
                , (CASE
                    WHEN round(F.bless_count / (A.end_time - A.start_time)::NUMERIC * 3600) IS NULL THEN 0
                    ELSE round(F.bless_count / (A.end_time - A.start_time)::NUMERIC * 3600)
                   END ) AS bless_havg
                , (CASE
                    WHEN round(C.seckill_count / (A.end_time - A.start_time)::NUMERIC * 3600) IS NULL THEN 0
                    ELSE round(C.seckill_count / (A.end_time - A.start_time)::NUMERIC * 3600)
                   END ) AS seckill_havg
                , (CASE
                    WHEN round(shelves_count / (A.end_time - A.start_time)::NUMERIC * 3600) IS NULL THEN 0
                    ELSE round(shelves_count / (A.end_time - A.start_time)::NUMERIC * 3600)
                   END ) AS shelves_havg
                , (CASE
                    WHEN round(E.explain_count / (A.end_time - A.start_time)::NUMERIC * 3600) IS NULL THEN 0
                    ELSE round(E.explain_count / (A.end_time - A.start_time)::NUMERIC * 3600)
                   END) AS explain_havg
                , (CASE WHEN B.live_show_cnt = 0 THEN -1 else round(M.watch_cnt / B.live_show_cnt::NUMERIC * 100, 2) END) AS exposure_watch_ratio
                , (CASE WHEN B.watch_ucnt = 0 THEN -1 else round(N.interactive_ucnt / B.watch_ucnt::NUMERIC * 100, 2) END) AS watch_interact_ratio
                , (CASE WHEN B.watch_ucnt = 0 THEN -1 else round(B.follow_anchor_ucnt / B.watch_ucnt::NUMERIC * 100, 2) END) AS watch_fans_ratio
                , (CASE WHEN B.watch_ucnt = 0 THEN -1 else round(B.fans_club_join_ucnt / B.watch_ucnt::NUMERIC * 100, 2) END) AS watch_clubs_ratio
                , (CASE WHEN M.watch_cnt = 0 THEN -1 else round(M.product_click_cnt / M.watch_cnt::NUMERIC * 100, 2) END) AS watch_click_ratio
                , (CASE WHEN M.watch_cnt = 0 THEN -1 else round(M.pay_cnt / M.watch_cnt::NUMERIC * 100, 2) END) AS watch_deal_ratio
                , total_live_avg_watch_duration AS jl_avg_watch_duration
                , round(O.total_live_watch_cnt/(A.end_time - A.start_time)::NUMERIC * 3600) AS jl_watch_cnt_havg
                , round(O.total_live_like_cnt/(A.end_time - A.start_time)::NUMERIC * 3600) AS jl_like_cnt_havg
                , round(O.total_live_comment_cnt/(A.end_time - A.start_time)::NUMERIC * 3600) AS jl_comment_havg
                , round(P.live_share_count/(A.end_time - A.start_time)::NUMERIC * 3600) AS jl_share_havg
                , round(O.live_form_submit_count/(A.end_time - A.start_time)::NUMERIC * 3600) AS jl_form_submit_havg
                , round(P.live_card_icon_component_show_count/(A.end_time - A.start_time)::NUMERIC * 3600) AS jl_card_show_havg
                , round(O.live_card_icon_component_click_count/(A.end_time - A.start_time)::NUMERIC * 3600) AS jl_card_click_havg
                , (CASE WHEN O.total_live_watch_cnt = 0 THEN -1 else round(O.total_live_comment_cnt / O.total_live_watch_cnt::NUMERIC * 100, 2) END) AS jl_watch_comment_ratio
                , (CASE WHEN O.total_live_watch_cnt = 0 THEN -1 else round(P.live_share_count / O.total_live_watch_cnt::NUMERIC * 100, 2) END) AS jl_watch_share_ratio
                , (CASE WHEN O.total_live_watch_cnt = 0 THEN -1 else round(O.total_live_like_cnt / O.total_live_watch_cnt::NUMERIC * 100, 2) END) AS jl_watch_like_ratio
                , (CASE WHEN O.total_live_watch_cnt = 0 THEN -1 else round(O.total_live_follow_cnt / O.total_live_watch_cnt::NUMERIC * 100, 2) END) AS jl_watch_fans_ratio
                , (CASE WHEN O.total_live_watch_cnt = 0 THEN -1 else round(P.live_fans_count / O.total_live_watch_cnt::NUMERIC * 100, 2) END) AS jl_watch_clubs_ratio
                , (CASE WHEN O.total_live_watch_cnt = 0 THEN -1 else round(P.live_card_icon_component_show_count / O.total_live_watch_cnt::NUMERIC * 100, 2) END) AS jl_watch_card_icon_show_ratio
                , (CASE WHEN O.total_live_watch_cnt = 0 THEN -1 else round(O.live_card_icon_component_click_count / O.total_live_watch_cnt::NUMERIC * 100, 2) END) AS jl_watch_card_icon_click_ratio
                , (CASE WHEN O.total_live_watch_cnt = 0 THEN -1 else round(O.live_form_submit_count / O.total_live_watch_cnt::NUMERIC * 100, 2) END) AS jl_watch_submit_ratio
                , (CASE WHEN O.total_live_watch_cnt = 0 THEN -1 else round(O.live_form_submit_count / O.live_card_icon_component_click_count::NUMERIC * 100, 2) END) AS jl_submit_component_click_ratio
            FROM vap_douyin_live_rooms A
            LEFT JOIN zhengti B ON A.room_id = B.room_id
            LEFT JOIN reptiel_seckill C ON A.room_id = C.room_id
            LEFT JOIN reptile_goods_shelves D ON A.room_id = D.room_id
            LEFT JOIN reptile_explain E ON A.room_id = E.room_id
            LEFT JOIN reptile_bless F ON A.room_id = F.room_id
            LEFT JOIN list_detail M ON A.room_id = M.room_id
            LEFT JOIN gmv_inter N ON A.room_id = N.room_id
            LEFT JOIN jl_top_card O ON A.room_id = O.room_id
            LEFT JOIN jl_analysis_list P ON A.room_id = P.room_id
            WHERE A.start_time > floor(extract(epoch from date_trunc('day', current_date))) - 3600 * 24
                    AND A.end_time > 0


        '''
        print(pg_sql_2)
        df2 = self.execute_sql_stime(pg_sql_2, conn)
        df2.fillna(0, inplace = True)


        m_df = pd.merge(df1, df2, how='outer', on=['room_id', 'douyin_no'])
        print(m_df)
        print(m_df.columns.values)
        # print(m_df.dtypes)
        # m_df.fillna(-1, inplace = True)
        # m_df['room_id'] = m_df['room_id'].map(lambda x: str(x) + '\t')
        # m_df.to_csv("111.csv")



        # self.assert_info(m_df, 'start_time_x', 'start_time_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        # self.assert_info(m_df, 'end_time_x', 'end_time_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'live_duration_x', 'live_duration_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'avg_watch_duration_x', 'avg_watch_duration_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'exposure_cnt_havg_x', 'exposure_cnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_ucnt_havg_x', 'watch_ucnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'fans_ucnt_havg_x', 'fans_ucnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'clubs_ucnt_havg_x', 'clubs_ucnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'product_click_cnt_havg_x', 'product_click_cnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'deal_order_cnt_havg_x', 'deal_order_cnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'gmv_havg_x', 'gmv_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'bless_havg_x', 'bless_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'seckill_havg_x', 'seckill_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'shelves_havg_x', 'shelves_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'explain_havg_x', 'explain_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'exposure_watch_ratio_x', 'exposure_watch_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_interact_ratio_x', 'watch_interact_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_fans_ratio_x', 'watch_fans_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_clubs_ratio_x', 'watch_clubs_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_click_ratio_x', 'watch_click_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'watch_deal_ratio_x', 'watch_deal_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_avg_watch_duration_x', 'jl_avg_watch_duration_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_cnt_havg_x', 'jl_watch_cnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_like_cnt_havg_x', 'jl_like_cnt_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_comment_havg_x', 'jl_comment_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_share_havg_x', 'jl_share_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_form_submit_havg_x', 'jl_form_submit_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_card_show_havg_x', 'jl_card_show_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_card_click_havg_x', 'jl_card_click_havg_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_comment_ratio_x', 'jl_watch_comment_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_share_ratio_x', 'jl_watch_share_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_like_ratio_x', 'jl_watch_like_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_fans_ratio_x', 'jl_watch_fans_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_clubs_ratio_x', 'jl_watch_clubs_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_card_icon_show_ratio_x', 'jl_watch_card_icon_show_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_card_icon_click_ratio_x', 'jl_watch_card_icon_click_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_watch_submit_ratio_x', 'jl_watch_submit_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])
        self.assert_info(m_df, 'jl_submit_component_click_ratio_x', 'jl_submit_component_click_ratio_y', col_list=['room_id', 'douyin_no', 'to_timestamp'])




    def test_check_vap_diagnose_room_past_avg_review(self, conn):
        pass






    def test_check_vap_diagnose_room_past_avg_daily(self, conn):
        pg_sql_1 = '''
            SELECT * FROM (
                SELECT *, DENSE_RANK() OVER(ORDER BY etl_date DESC) AS rn
                FROM vap_diagnose_room_past_avg WHERE flag = 'daily' ORDER BY etl_date DESC
            ) t WHERE rn = 1

        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)
        print(df1)


        pg_sql_2 = '''
            WITH reptile_bless AS (
                SELECT room_id, count(*) AS bless_num FROM doudian_compass_reptile_bless GROUP BY room_id
            ),
            reptiel_seckill AS (
                SELECT room_id, count(*) AS seckill_num FROM doudian_compass_reptiel_seckill GROUP BY room_id
            ),
            reptile_goods_shelves AS (
                SELECT room_id, SUM(shelves_count) AS shelves_num
                FROM (
                    SELECT room_id, "timestamp", create_time, other_num, id, prod_name
                        , (CASE WHEN other_num = 0 THEN 1
                            WHEN other_num > 0 THEN other_num + count(*) OVER(PARTITION BY room_id, "timestamp", create_time)
                        END) AS shelves_count
                        , ROW_NUMBER() OVER(PARTITION BY room_id, "timestamp", create_time ORDER BY create_time) srank
                    FROM doudian_compass_reptile_goods_shelves
                ) t WHERE srank = 1
                GROUP BY room_id
            ),
            reptile_explain AS (
                SELECT room_id, count(*) AS explain_num FROM doudian_compass_reptile_explain GROUP BY room_id
            )
            SELECT douyin_no
                , "interval"
                , round(AVG(live_duration)) AS live_duration
                , round(AVG(gpm), 2) AS gpm
                , round(AVG(avg_watch_duration)) AS avg_watch_duration
                , round(AVG(exposure_cnt_havg)) AS exposure_cnt_havg
                , round(AVG(watch_ucnt_havg)) AS watch_ucnt_havg
                , round(AVG(fans_ucnt_havg)) AS fans_ucnt_havg
                , round(AVG(clubs_ucnt_havg)) AS clubs_ucnt_havg
                , round(AVG(product_click_cnt_havg)) AS product_click_cnt_havg
                , round(AVG(deal_order_cnt_havg)) AS deal_order_cnt_havg
                , round(AVG(gmv_havg), 2) AS gmv_havg
                , round(AVG(bless_havg))  AS bless_havg
                , round(AVG(seckill_havg)) AS seckill_havg
                , round(AVG(shelves_havg)) AS shelves_havg
                , round(AVG(explain_havg)) AS explain_havg
                , round(AVG(exposure_watch_ratio), 2) AS exposure_watch_ratio
                , round(AVG(watch_interact_ratio), 2) AS watch_interact_ratio
                , round(AVG(watch_fans_ratio), 2) AS watch_fans_ratio
                , round(AVG(watch_clubs_ratio), 2) AS watch_clubs_ratio
                , round(AVG(watch_click_ratio), 2) AS watch_click_ratio
                , round(AVG(watch_deal_ratio), 2) AS watch_deal_ratio
                , round(AVG(jl_avg_watch_duration)) AS jl_avg_watch_duration
                , round(AVG(jl_watch_cnt_havg)) AS jl_watch_cnt_havg
                , round(AVG(jl_like_cnt_havg)) AS jl_like_cnt_havg
                , round(AVG(jl_comment_havg)) AS jl_comment_havg
                , round(AVG(jl_share_havg)) AS jl_share_havg
                , round(AVG(jl_form_submit_havg)) AS jl_form_submit_havg
                , round(AVG(jl_card_show_havg)) AS jl_card_show_havg
                , round(AVG(jl_card_click_havg)) AS jl_card_click_havg
                , round(AVG(jl_watch_comment_ratio), 2) AS jl_watch_comment_ratio
                , round(AVG(jl_watch_share_ratio), 2) AS jl_watch_share_ratio
                , round(AVG(jl_watch_like_ratio), 2) AS jl_watch_like_ratio
                , round(AVG(jl_watch_fans_ratio), 2) AS jl_watch_fans_ratio
                , round(AVG(jl_watch_clubs_ratio), 2) AS jl_watch_clubs_ratio
                , round(AVG(jl_watch_card_icon_show_ratio), 2) AS jl_watch_card_icon_show_ratio
                , round(AVG(jl_watch_card_icon_click_ratio), 2) AS jl_watch_card_icon_click_ratio
                , round(AVG(jl_watch_submit_ratio), 2) AS jl_watch_submit_ratio
                , round(AVG(jl_submit_component_click_ratio), 2) AS jl_submit_component_click_ratio
            FROM (
                SELECT douyin_no
                    , to_char(to_timestamp(start_time), 'YYYY-MM-DD')
                    , "interval"
                    , SUM(end_time - start_time) AS live_duration
                    , AVG(NULLIF(gpm, 0)) AS gpm
                    , AVG(avg_watch_duration) AS avg_watch_duration
                    , (CASE WHEN SUM(end_time - start_time) = 0 THEN -1 ELSE round(SUM(exposure_cnt) / SUM(end_time - start_time)::NUMERIC * 3600) END) AS exposure_cnt_havg
                    , (CASE WHEN SUM(end_time - start_time) = 0 THEN -1 ELSE round(SUM(watch_ucnt) / SUM(end_time - start_time)::NUMERIC * 3600) END) AS watch_ucnt_havg
                    , (CASE WHEN SUM(end_time - start_time) = 0 THEN -1 ELSE round(SUM(fans_ucnt) / SUM(end_time - start_time)::NUMERIC * 3600) END) AS fans_ucnt_havg
                    , (CASE WHEN SUM(end_time - start_time) = 0 THEN -1 ELSE round(SUM(clubs_ucnt) / SUM(end_time - start_time)::NUMERIC * 3600) END) AS clubs_ucnt_havg
                    , (CASE WHEN SUM(end_time - start_time) = 0 THEN -1 ELSE round(SUM(product_click_cnt) / SUM(end_time - start_time)::NUMERIC * 3600) END) AS product_click_cnt_havg
                    , (CASE WHEN SUM(end_time - start_time) = 0 THEN -1 ELSE round(SUM(deal_order_cnt) / SUM(end_time - start_time)::NUMERIC * 3600) END) AS deal_order_cnt_havg
                    , (CASE WHEN SUM(end_time - start_time) = 0 THEN -1 ELSE round(SUM(gmv) / SUM(end_time - start_time)::NUMERIC * 3600, 2) END) AS gmv_havg
                    , (CASE WHEN SUM(end_time - start_time) = 0 THEN -1 ELSE round(SUM(bless_num) / SUM(end_time - start_time)::NUMERIC * 3600) END) AS bless_havg
                    , (CASE WHEN SUM(end_time - start_time) = 0 THEN -1 ELSE round(SUM(seckill_num) / SUM(end_time - start_time)::NUMERIC * 3600) END) AS seckill_havg
                    , (CASE WHEN SUM(end_time - start_time) = 0 THEN -1 ELSE round(SUM(shelves_num) / SUM(end_time - start_time)::NUMERIC * 3600) END) AS shelves_havg
                    , (CASE WHEN SUM(end_time - start_time) = 0 THEN -1 ELSE round(SUM(explain_num) / SUM(end_time - start_time)::NUMERIC * 3600) END) AS explain_havg
                    , (CASE WHEN SUM(exposure_cnt) = 0 THEN -1 ELSE round(SUM(watch_cnt) / SUM(exposure_cnt) *100, 2) END) AS exposure_watch_ratio
                    , (CASE WHEN SUM(watch_ucnt) = 0 THEN -1 ELSE round(SUM(interact_ucnt) / SUM(watch_ucnt) *100, 2) END) AS watch_interact_ratio
                    , (CASE WHEN SUM(watch_ucnt) = 0 THEN -1 ELSE round(SUM(fans_ucnt) / SUM(watch_ucnt) *100, 2) END) AS watch_fans_ratio
                    , (CASE WHEN SUM(watch_ucnt) = 0 THEN -1 ELSE round(SUM(clubs_ucnt) / SUM(watch_ucnt) *100, 2) END) AS watch_clubs_ratio
                    , (CASE WHEN SUM(watch_cnt) = 0 THEN -1 ELSE round(SUM(product_click_cnt) / SUM(watch_cnt) *100, 2) END) AS watch_click_ratio
                    , (CASE WHEN SUM(watch_cnt) = 0 THEN -1 ELSE round(SUM(deal_order_cnt) / SUM(watch_cnt) *100, 2) END) AS watch_deal_ratio
                    , AVG(jl_avg_watch_duration) AS jl_avg_watch_duration
                    , (CASE WHEN SUM(live_duration) = 0 THEN -1 ELSE round(SUM(jl_watch_cnt) / SUM(live_duration) *3600, 2) END) AS jl_watch_cnt_havg
                    , (CASE WHEN SUM(live_duration) = 0 THEN -1 ELSE round(SUM(jl_like_cnt) / SUM(live_duration) *3600, 2) END) AS jl_like_cnt_havg
                    , (CASE WHEN SUM(live_duration) = 0 THEN -1 ELSE round(SUM(jl_comment_cnt) / SUM(live_duration) *3600, 2) END) AS jl_comment_havg
                    , (CASE WHEN SUM(live_duration) = 0 THEN -1 ELSE round(SUM(jl_share_cnt) / SUM(live_duration) *3600, 2) END) AS jl_share_havg
                    , (CASE WHEN SUM(live_duration) = 0 THEN -1 ELSE round(SUM(jl_form_submit_cnt) / SUM(live_duration) *3600, 2) END) AS jl_form_submit_havg
                    , (CASE WHEN SUM(live_duration) = 0 THEN -1 ELSE round(SUM(jl_card_show_cnt) / SUM(live_duration) *3600, 2) END) AS jl_card_show_havg
                    , (CASE WHEN SUM(live_duration) = 0 THEN -1 ELSE round(SUM(jl_card_click_cnt) / SUM(live_duration) *3600, 2) END) AS jl_card_click_havg
                    , (CASE WHEN SUM(jl_watch_cnt) = 0 THEN -1 ELSE round(SUM(jl_comment_cnt) / SUM(jl_watch_cnt) *100, 2) END) AS jl_watch_comment_ratio
                    , (CASE WHEN SUM(jl_watch_cnt) = 0 THEN -1 ELSE round(SUM(jl_share_cnt) / SUM(jl_watch_cnt) *100, 2) END) AS jl_watch_share_ratio
                    , (CASE WHEN SUM(jl_watch_cnt) = 0 THEN -1 ELSE round(SUM(jl_like_cnt) / SUM(jl_watch_cnt) *100, 2) END) AS jl_watch_like_ratio
                    , (CASE WHEN SUM(jl_watch_cnt) = 0 THEN -1 ELSE round(SUM(jl_fans_ucnt) / SUM(jl_watch_cnt) *100, 2) END) AS jl_watch_fans_ratio
                    , (CASE WHEN SUM(jl_watch_cnt) = 0 THEN -1 ELSE round(SUM(jl_clubs_ucnt) / SUM(jl_watch_cnt) *100, 2) END) AS jl_watch_clubs_ratio
                    , (CASE WHEN SUM(jl_watch_cnt) = 0 THEN -1 ELSE round(SUM(jl_card_show_cnt) / SUM(jl_watch_cnt) *100, 2) END) AS jl_watch_card_icon_show_ratio
                    , (CASE WHEN SUM(jl_watch_cnt) = 0 THEN -1 ELSE round(SUM(jl_card_click_cnt) / SUM(jl_watch_cnt) *100, 2) END) AS jl_watch_card_icon_click_ratio
                    , (CASE WHEN SUM(jl_watch_cnt) = 0 THEN -1 ELSE round(SUM(jl_form_submit_cnt) / SUM(jl_watch_cnt) *100, 2) END) AS jl_watch_submit_ratio
                    , (CASE WHEN SUM(jl_card_click_cnt) = 0 THEN -1 ELSE round(SUM(jl_form_submit_cnt) / SUM(jl_card_click_cnt) *100, 2) END) AS jl_submit_component_click_ratio
                FROM (
                    SELECT *
                        , ROW_NUMBER() OVER(PARTITION BY A.room_id ORDER BY A.etl_flag DESC) rn
                        , (CASE WHEN A.start_time >= floor(extract(epoch from date_trunc('day', current_date))) - 3600*24 THEN '1'
                            WHEN A.start_time <= floor(extract(epoch from date_trunc('day', current_date))) - 3600*24 THEN '30'
                        END) AS "interval"
                    FROM vap_review_room A
                    LEFT JOIN reptile_bless B ON A.room_id = B.room_id
                    LEFT JOIN reptiel_seckill C ON A.room_id = C.room_id
                    LEFT JOIN reptile_goods_shelves D ON A.room_id = D.room_id
                    LEFT JOIN reptile_explain E ON A.room_id = E.room_id
                    WHERE A.start_time >= floor(extract(epoch from date_trunc('day', current_date))) - 3600*24*32
                        AND A.start_time < floor(extract(epoch from date_trunc('day', current_date)))
                ) t
                WHERE rn = 1 AND "interval" IS NOT NULL
                GROUP BY douyin_no, to_char(to_timestamp(start_time), 'YYYY-MM-DD'), "interval"
            ) t1
            GROUP BY douyin_no, "interval"


        '''
        df2 = self.execute_sql_stime(pg_sql_2, conn)
        print(df2)


        m_df = pd.merge(df1, df2, how='outer', on=['douyin_no', 'interval'])
        print(m_df)
        print(m_df.columns.values)
        m_df.fillna(-1, inplace = True)


        self.assert_info(m_df, 'live_duration_x', 'live_duration_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'avg_watch_duration_x', 'avg_watch_duration_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'exposure_cnt_havg_x', 'exposure_cnt_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'watch_ucnt_havg_x', 'watch_ucnt_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'fans_ucnt_havg_x', 'fans_ucnt_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'clubs_ucnt_havg_x', 'clubs_ucnt_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'product_click_cnt_havg_x', 'product_click_cnt_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'deal_order_cnt_havg_x', 'deal_order_cnt_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'gmv_havg_x', 'gmv_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'bless_havg_x', 'bless_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'seckill_havg_x', 'seckill_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'shelves_havg_x', 'shelves_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'explain_havg_x', 'explain_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'exposure_watch_ratio_x', 'exposure_watch_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'watch_interact_ratio_x', 'watch_interact_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'watch_fans_ratio_x', 'watch_fans_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'watch_clubs_ratio_x', 'watch_clubs_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'watch_click_ratio_x', 'watch_click_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'watch_deal_ratio_x', 'watch_deal_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_avg_watch_duration_x', 'jl_avg_watch_duration_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_watch_cnt_havg_x', 'jl_watch_cnt_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_like_cnt_havg_x', 'jl_like_cnt_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_comment_havg_x', 'jl_comment_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_share_havg_x', 'jl_share_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_form_submit_havg_x', 'jl_form_submit_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_card_show_havg_x', 'jl_card_show_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_card_click_havg_x', 'jl_card_click_havg_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_watch_comment_ratio_x', 'jl_watch_comment_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_watch_share_ratio_x', 'jl_watch_share_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_watch_like_ratio_x', 'jl_watch_like_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_watch_fans_ratio_x', 'jl_watch_fans_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_watch_clubs_ratio_x', 'jl_watch_clubs_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_watch_card_icon_show_ratio_x', 'jl_watch_card_icon_show_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_watch_card_icon_click_ratio_x', 'jl_watch_card_icon_click_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_watch_submit_ratio_x', 'jl_watch_submit_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])
        self.assert_info(m_df, 'jl_submit_component_click_ratio_x', 'jl_submit_component_click_ratio_y', col_list=['douyin_no', 'interval', 'etl_date'])






    def test_check_vap_channel_conversion_funnel(self, conn):
        pg_sql_1 = '''
            SELECT * FROM vap_channel_conversion_funnel WHERE etl_date = '2023-03-14'
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)
        print(df1)

        df_lp_pay = df1[df1['channel_code'] == "lp_pay"]
        for k, v in df_lp_pay.groupby(['etl_date']):
            print(k)
            # print(v)
            etl_date = k
            df_result = v

            pg_sql_2 = f'''

                WITH live_douyin AS (
                    SELECT DISTINCT douyin_no FROM vap_douyin_live_rooms WHERE to_char(to_timestamp(start_time), 'YYYY-MM-DD') = '{etl_date}'
                ),
                tmp1 AS (
                    SELECT ies_id, st
                        , SUM(A.live_watch_count) FILTER(WHERE first_flow_category <> '自然流量') AS jl_watch_cnt
                        , SUM(A.live_card_icon_component_click_count) FILTER(WHERE first_flow_category <> '自然流量') AS jl_card_click_cnt
                        , SUM(A.live_form_submit_count) FILTER(WHERE first_flow_category <> '自然流量') AS jl_form_submit_cnt
                    FROM juliangzongheng_live_traffic_source A
                    WHERE st = '{etl_date}'
                    GROUP BY ies_id, st
                ),
                tmp2 AS (
                    SELECT
                        t.douyin_no, s_time
                        , SUM(ad_live_show_cnt) AS jl_exposure_cnt
                        , SUM(jl_card_show_cnt) AS jl_card_show_cnt
                    FROM (
                        SELECT A.douyin_no, to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') AS s_time, A.room_id
                            , SUM(B.live_card_icon_component_show_count) FILTER(WHERE first_flow_category = '全部')
                                -  SUM(B.live_card_icon_component_show_count) FILTER(WHERE first_flow_category = '自然流量')
                            AS jl_card_show_cnt
                        FROM vap_douyin_live_rooms A
                        LEFT JOIN juliangzongheng_live_flow_comparison B ON A.room_id = B.room_id
                        WHERE A.company_id <> 'demo' AND to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') = '{etl_date}'
                            AND A.start_time >= 1676390400
                        GROUP BY douyin_no,  to_char(to_timestamp(A.start_time), 'YYYY-MM-DD'), A.room_id
                    ) t LEFT JOIN juliangzongheng_live_analysis_list C ON t.room_id = C.room_id
                    GROUP BY t.douyin_no, s_time
                ),
                tmp3 AS (
                    SELECT to_char(to_timestamp(date_time), 'YYYY-MM-DD') AS s_time, A.douyin_no
                            , SUM(vertical) FILTER(WHERE index_name='live_show_ucnt' AND first_channel_code='all') - SUM(vertical) FILTER(WHERE index_name='live_show_ucnt' AND first_channel_code='all_natural' AND channel_code IS NULL) AS exposure_ucnt
                            , SUM(vertical) FILTER(WHERE index_name='watch_ucnt' AND first_channel_code='all') - SUM(vertical) FILTER(WHERE index_name='watch_ucnt' AND first_channel_code='all_natural' AND channel_code IS NULL) AS watch_ucnt
                            , SUM(vertical) FILTER(WHERE index_name='product_show_ucnt' AND first_channel_code='all') - SUM(vertical) FILTER(WHERE index_name='product_show_ucnt' AND first_channel_code='all_natural' AND channel_code IS NULL) AS product_exposure_ucnt
                            , SUM(vertical) FILTER(WHERE index_name='product_click_ucnt' AND first_channel_code='all') - SUM(vertical) FILTER(WHERE index_name='product_click_ucnt' AND first_channel_code='all_natural' AND channel_code IS NULL) AS product_click_ucnt
                            , SUM(vertical) FILTER(WHERE index_name='pay_ucnt' AND first_channel_code='all') - SUM(vertical) FILTER(WHERE index_name='pay_ucnt' AND first_channel_code='all_natural' AND channel_code IS NULL) AS deal_ucnt
                    FROM doudian_compass_channel_analysis A
                    INNER JOIN live_douyin B ON A.douyin_no = B.douyin_no
                    WHERE to_char(to_timestamp(date_time), 'YYYY-MM-DD') = '{etl_date}'
                    GROUP BY to_char(to_timestamp(date_time), 'YYYY-MM-DD'), A.douyin_no
                ),
                full_tmp AS (
                    SELECT COALESCE(ies_id, douyin_no) AS douyin_no
                        , COALESCE(st, s_time) AS etl_date
                        , jl_watch_cnt
                        , jl_card_click_cnt
                        , jl_form_submit_cnt
                        , jl_exposure_cnt
                        , jl_card_show_cnt
                    FROM tmp1
                    INNER JOIN tmp2 ON tmp1.ies_id = tmp2.douyin_no AND tmp1.st = tmp2.s_time
                )
                SELECT COALESCE(A.douyin_no, B.douyin_no) AS douyin_no
                    , COALESCE(A.s_time, B.etl_date) AS etl_date
                    , jl_watch_cnt
                    , jl_card_click_cnt
                    , jl_form_submit_cnt
                    , jl_exposure_cnt
                    , jl_card_show_cnt
                    , exposure_ucnt
                    , watch_ucnt
                    , product_exposure_ucnt
                    , product_click_ucnt
                    , deal_ucnt
                FROM tmp3 A LEFT JOIN full_tmp B ON A.douyin_no = B.douyin_no AND A.s_time = B.etl_date

            '''
            df2 = self.execute_sql_stime(pg_sql_2, conn)
            print(df2)

            m_df = pd.merge(df_result, df2, how='outer', on=['douyin_no', 'etl_date'])
            print(m_df)
            print(m_df.columns.values)
            m_df.fillna(0, inplace = True)


            self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['douyin_no', 'etl_date'])
            self.assert_info(m_df, 'jl_card_click_cnt_x', 'jl_card_click_cnt_y', col_list=['douyin_no', 'etl_date'])
            self.assert_info(m_df, 'jl_form_submit_cnt_x', 'jl_form_submit_cnt_y', col_list=['douyin_no', 'etl_date'])
            self.assert_info(m_df, 'jl_exposure_cnt_x', 'jl_exposure_cnt_y', col_list=['douyin_no', 'etl_date'])
            self.assert_info(m_df, 'jl_card_show_cnt_x', 'jl_card_show_cnt_y', col_list=['douyin_no', 'etl_date'])
            self.assert_info(m_df, 'exposure_ucnt_x', 'exposure_ucnt_y', col_list=['douyin_no', 'etl_date'])
            self.assert_info(m_df, 'watch_ucnt_x', 'watch_ucnt_y', col_list=['douyin_no', 'etl_date'])
            self.assert_info(m_df, 'product_exposure_ucnt_x', 'product_exposure_ucnt_y', col_list=['douyin_no', 'etl_date'])
            self.assert_info(m_df, 'product_click_ucnt_x', 'product_click_ucnt_y', col_list=['douyin_no', 'etl_date'])
            self.assert_info(m_df, 'deal_ucnt_x', 'deal_ucnt_y', col_list=['douyin_no', 'etl_date'])










    def test_check_vap_vap_channel_analysis(self, conn):
        pg_sql_1 = '''
            SELECT * FROM vap_channel_analysis
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        df_lp_free = df1[df1['first_channel_code'] == "lp_free"]
        for k, v in df_lp_free.groupby(['etl_date']):
            print(k)
            # print(v)
            etl_date = k
            df_result = v
            assert set(df_result['jl_watch_ucnt'].tolist()) == {-1}


            pg_sql_2 = f'''
                WITH live_douyin_no AS (
                        SELECT A.douyin_no, to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') AS s_time
                        FROM vap_douyin_live_rooms A
                        WHERE A.company_id <> 'demo' AND to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') = '{etl_date}'
                                AND A.start_time >= 1676390400
                        GROUP BY douyin_no, to_char(to_timestamp(A.start_time), 'YYYY-MM-DD')
                )
                SELECT *
                        , (CASE WHEN live_watch_cnt = 0 THEN 0 ELSE round(gmv/live_watch_cnt::NUMERIC, 2) END) AS gpm
                FROM (
                        SELECT A.douyin_no, channel_code AS second_channel_code
                                , SUM(vertical) FILTER(WHERE index_name = 'watch_cnt') AS live_watch_cnt
                                , round(SUM(vertical) FILTER(WHERE index_name = 'pay_amt')/100::NUMERIC, 2) AS gmv
                        FROM live_douyin_no A
                        INNER JOIN doudian_compass_channel_analysis B ON A.douyin_no = B.douyin_no AND A.douyin_no = B.aweme_id
                                AND to_char(to_timestamp(B.date_time), 'YYYY-MM-DD') = A.s_time
                        WHERE first_channel_code = 'all_natural' AND channel_code IS NOT NULL
                        GROUP BY A.douyin_no, channel_code
                ) t
            '''
            df2 = self.execute_sql_stime(pg_sql_2, conn)

            m_df = pd.merge(df_result, df2, how='outer', on=['douyin_no', 'second_channel_code'])
            print(m_df)
            print(m_df.columns.values)
            m_df.fillna(-1, inplace = True)

            self.assert_info(m_df, 'live_watch_cnt_x', 'live_watch_cnt_y', col_list=['douyin_no', 'etl_date', 'first_channel_code'])
            self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['douyin_no', 'etl_date', 'first_channel_code'])
            self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['douyin_no', 'etl_date', 'first_channel_code'])


        df_lp_pay = df1[df1['first_channel_code'] == "lp_pay"]
        for k, v in df_lp_pay.groupby(['etl_date']):
            print(k)
            # print(v)
            etl_date = k
            df_result = v

            assert set(df_result['jl_watch_ucnt'].tolist()) == {-1}

            pg_sql_2 = f'''
                WITH live_douyin_no AS (
                        SELECT A.douyin_no, to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') AS s_time
                        FROM vap_douyin_live_rooms A
                        WHERE A.company_id <> 'demo' AND to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') = '{etl_date}'
                                AND A.start_time >= 1676390400
                        GROUP BY douyin_no, to_char(to_timestamp(A.start_time), 'YYYY-MM-DD')
                )
                SELECT *
                        , (CASE WHEN live_watch_cnt = 0 THEN 0 ELSE round(gmv/live_watch_cnt::NUMERIC, 2) END) AS gpm
                FROM (
                        SELECT A.douyin_no, channel_code AS second_channel_code
                                , SUM(vertical) FILTER(WHERE index_name = 'watch_cnt') AS live_watch_cnt
                                , round(SUM(vertical) FILTER(WHERE index_name = 'pay_amt')/100::NUMERIC, 2) AS gmv
                        FROM live_douyin_no A
                        INNER JOIN doudian_compass_channel_analysis B ON A.douyin_no = B.douyin_no AND A.douyin_no = B.aweme_id
                                AND to_char(to_timestamp(B.date_time), 'YYYY-MM-DD') = A.s_time
                        WHERE first_channel_code = 'all_pay' AND channel_code IS NOT NULL
                        GROUP BY A.douyin_no, channel_code
                ) t
            '''
            df3 = self.execute_sql_stime(pg_sql_2, conn)

            m_df = pd.merge(df_result, df3, how='outer', on=['douyin_no', 'second_channel_code'])
            print(m_df)
            print(m_df.columns.values)
            m_df.fillna(-1, inplace = True)

            self.assert_info(m_df, 'live_watch_cnt_x', 'live_watch_cnt_y', col_list=['douyin_no', 'etl_date', 'first_channel_code'])
            self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['douyin_no', 'etl_date', 'first_channel_code'])
            self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['douyin_no', 'etl_date', 'first_channel_code'])

        df_jl_free = df1[df1['first_channel_code'] == "jl_free"]
        for k, v in df_jl_free.groupby(['etl_date']):
            print(k)
            # print(v)
            etl_date = k
            df_result = v

            assert set(df_result['live_watch_cnt'].tolist()) == {-1}
            assert set(df_result['gmv'].tolist()) == {-1}
            assert set(df_result['gpm'].tolist()) == {-1}

            pg_sql_2 = f'''
                SELECT douyin_no
                        , unnest(array['1', '2', '3', '4', '5', '6', '7']) as second_channel_code
                        , unnest(array["1", "2", "3", "4", "5", "6", "7"]) as jl_watch_ucnt
                        , -1 AS live_watch_cnt
                        , -1 AS gmv
                        , -1 AS gpm
                FROM (
                        SELECT A.douyin_no, to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') AS s_time
                            , SUM(live_broadcast_recommendation_unfans_live_watch_ucount + live_broadcast_recommendation_fans_live_watch_ucount) AS "1"
                            , SUM(attention_unfans_watch_ucnt + attention_fans_watch_ucnt) AS "2"
                            , SUM(homepage_unfans_watch_ucnt + homepage_fans_watch_ucnt) AS "3"
                            , SUM(search_unfans_watch_ucnt + search_fans_watch_ucnt) AS "4"
                            , SUM(same_city_unfans_watch_ucnt + same_city_fans_watch_ucnt) AS "5"
                            , SUM(video_recommendation_unfans_watch_ucnt + video_recommendation_fans_watch_ucnt) AS "6"
                            , SUM(other_unfans_watch_ucnt + other_fans_watch_ucnt) AS "7"
                        FROM vap_douyin_live_rooms A
                        LEFT JOIN juliangzongheng_live_flow_cate_fan B ON A.room_id = B.room_id
                        WHERE A.company_id <> 'demo' AND to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') = '{etl_date}'
                            AND A.start_time >= 1676390400
                            AND B.room_id IS NOT NULL
                        GROUP BY A.douyin_no, to_char(to_timestamp(A.start_time), 'YYYY-MM-DD')
                )t

            '''
            df3 = self.execute_sql_stime(pg_sql_2, conn)

            m_df = pd.merge(df_result, df3, how='outer', on=['douyin_no', 'second_channel_code'])
            print(m_df)
            print(m_df.columns.values)
            m_df.fillna(-1, inplace = True)

            self.assert_info(m_df, 'jl_watch_ucnt_x', 'jl_watch_ucnt_y', col_list=['douyin_no', 'etl_date', 'first_channel_code'])



        df_jl_free = df1[df1['first_channel_code'] == "jl_pay"]
        for k, v in df_jl_free.groupby(['etl_date']):
            print(k)
            # print(v)
            etl_date = k
            df_result = v

            assert set(df_result['live_watch_cnt'].tolist()) == {-1}
            assert set(df_result['gmv'].tolist()) == {-1}
            assert set(df_result['gpm'].tolist()) == {-1}

            pg_sql_2 = f'''
                SELECT douyin_no
                        , unnest(array['1', '2', '3']) as second_channel_code
                        , unnest(array["1", "2", "3"]) as jl_watch_ucnt
                        , -1 AS live_watch_cnt
                        , -1 AS gmv
                        , -1 AS gpm
                FROM (
                        SELECT A.douyin_no, to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') AS s_time
                            , SUM(bid_advertising_unfans_live_watch_ucount + bid_advertising_fans_live_watch_ucount) AS "1"
                            , SUM(dou_add_unfans_live_watch_ucount + dou_add_fans_live_watch_ucount) AS "2"
                            , SUM(brand_advertising_unfans_live_watch_ucount + brand_advertising_fans_live_watch_ucount) AS "3"
                        FROM vap_douyin_live_rooms A
                        LEFT JOIN juliangzongheng_live_flow_cate_fan B ON A.room_id = B.room_id
                        WHERE A.company_id <> 'demo' AND to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') = '{etl_date}'
                            AND A.start_time >= 1676390400
                            AND B.room_id IS NOT NULL
                        GROUP BY A.douyin_no, to_char(to_timestamp(A.start_time), 'YYYY-MM-DD')
                )t

            '''
            df3 = self.execute_sql_stime(pg_sql_2, conn)

            m_df = pd.merge(df_result, df3, how='outer', on=['douyin_no', 'second_channel_code'])
            print(m_df)
            print(m_df.columns.values)
            m_df.fillna(-1, inplace = True)

            self.assert_info(m_df, 'jl_watch_ucnt_x', 'jl_watch_ucnt_y', col_list=['douyin_no', 'etl_date', 'first_channel_code'])


    def test_get_juliang(self, conn):
        pg_sql_1 = '''
            WITH tmp AS (
                SELECT room_id AS "直播间id"
                    , douyin_no
                    , jl_avg_watch_duration AS "whale-观众平均停留时长"
                    , jl_card_show_cnt AS "whale-组件展示数"
                    , jl_card_click_cnt AS "whale-组件点击数"
                FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY room_id ORDER BY etl_flag DESC) rn
                    FROM vap_review_room
                    WHERE douyin_no IN ('PCZZ037160219911', 'CSMDLEXUS', 'PCNCHG')
                        AND (to_char(to_timestamp(start_time), 'YYYY-MM-DD') >= '2023-03-01'
                            OR to_char(to_timestamp(end_time), 'YYYY-MM-DD') >= '2023-03-01')
                ) t WHERE rn = 1
            )
            SELECT A.*
                , live_card_icon_component_show_count AS "爬虫-组件展示数"
                , live_card_icon_component_click_count AS "爬虫-组件点击数"
                , live_avg_watch_duration AS "爬虫-平均停留时常"
                , to_char(to_timestamp(create_time), 'YYYY-MM-DD HH24:MI:SS') AS "爬虫-create_time"
                , to_char(to_timestamp(update_time), 'YYYY-MM-DD HH24:MI:SS') AS "爬虫-update_time"
            FROM tmp A LEFT JOIN juliangzongheng_live_analysis_list B ON A.直播间id = B.room_id

        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)
        print(df1)

        df2 = pd.read_excel("/Users/edy/Downloads/广告投放_直播报表_21011_2023_03_13_17_13_25.xls")
        print(df2)
        df2['直播间id'] = df2['直播间id'].astype(str)


        m_df = pd.merge(df2, df1, how='left', on=['直播间id'])
        print(m_df)
        print(m_df.columns.values)

        m_df['直播间id'] = m_df['直播间id'].map(lambda x: str(x) + '\t')
        m_df['time'] = pd.to_datetime(m_df['开播时间'], format='%Y-%m-%d %H:%M')
        m_df = m_df.sort_values(by='time', ascending=True)
        m_df.style.hide_index()

        # def highlight_diff1(x):
        #     # 如果 A 和 B 不相等，则返回一组样式
        #     if x['whale-观众平均停留时长'] != x['观众平均停留时长']:
        #         return ['color: red', 'color: red']
        #     # 如果 A 和 B 相等，则返回空样式
        #     return ['', '']

        # def highlight_diff2(x):
        #     # 如果 A 和 B 不相等，则返回一组样式
        #     if x['whale-组件展示数'] != x['组件展示数']:
        #         return ['color: orange', 'color: orange']
        #     # 如果 A 和 B 相等，则返回空样式
        #     return ['', '']

        # def highlight_diff3(x):
        #     # 如果 A 和 B 不相等，则返回一组样式
        #     if x['whale-组件点击数'] != x['组件点击数']:
        #         return ['color: blue', 'color: blue']
        #     # 如果 A 和 B 相等，则返回空样式
        #     return ['', '']

        # # 使用 apply 方法将样式应用到 DataFrame 中的每个单元格
        # styled_df = m_df.style.apply(
        #     highlight_diff1, subset=['whale-观众平均停留时长', '观众平均停留时长'], axis=1).apply(
        #     highlight_diff2, subset=['whale-组件展示数', '组件展示数'], axis=1).apply(
        #     highlight_diff3, subset=['whale-组件点击数', '组件点击数'], axis=1)

        m_df = m_df[["直播间id", "douyin_no", "开播时间", "下播时间", "观众平均停留时长", "组件展示数", "组件点击数", "whale-观众平均停留时长",
            "whale-组件展示数", "whale-组件点击数",  "爬虫-create_time", "爬虫-update_time"
            , "爬虫-平均停留时常", "爬虫-组件展示数", "爬虫-组件点击数"
            ]]

        m_df['观众平均停留时长差异值'] = m_df['观众平均停留时长'] - m_df['whale-观众平均停留时长']
        m_df['组件展示数差异值'] = m_df['组件展示数'] - m_df['whale-组件展示数']
        m_df['组件点击数差异值'] = m_df['组件点击数'] - m_df['whale-组件点击数']

        m_df['爬虫对比数据源-平均停留时长差异'] = m_df['观众平均停留时长'] - m_df['爬虫-平均停留时常']
        m_df['爬虫对比数据源-组件展示数差异'] = m_df['组件展示数'] - m_df['爬虫-组件展示数']
        m_df['爬虫对比数据源-组件点击数差异'] = m_df['组件点击数'] - m_df['爬虫-组件点击数']

        from functools import partial

        def highlight_nonzero(val):
            color = 'red' if val != 0 else ''
            return f'color: {color}'

        def highlight_diff(x, cola, colb):
            # 如果 A 和 B 不相等，则返回一组样式
            if x[cola] != x[colb]:
                return ['color: red', 'color: red']
            # 如果 A 和 B 相等，则返回空样式
            return ['', '']

        # styled_df = m_df.style.applymap(highlight_nonzero, subset=['观众平均停留时长差异值', '组件展示数差异值', '组件点击数差异值']).apply(
        #     partial(highlight_diff, cola='观众平均停留时长差异值', colb='爬虫对比数据源-平均停留时长差异'), subset=['观众平均停留时长差异值','爬虫对比数据源-平均停留时长差异'], axis=1).apply(
        #     partial(highlight_diff, cola='组件展示数差异值', colb='爬虫对比数据源-组件展示数差异'), subset=['观众平均停留时长差异值','爬虫对比数据源-平均停留时长差异'], axis=1).apply(
        #     partial(highlight_diff, cola='组件点击数差异值', colb='爬虫对比数据源-组件点击数差异'), subset=['观众平均停留时长差异值','爬虫对比数据源-平均停留时长差异'], axis=1)

        # styled_df = m_df.style.applymap(highlight_nonzero, subset=['观众平均停留时长差异值', '组件展示数差异值', '组件点击数差异值'])

        styled_df = m_df.style.applymap(highlight_nonzero, subset=['观众平均停留时长差异值', '组件展示数差异值', '组件点击数差异值'
            , '爬虫对比数据源-平均停留时长差异', '爬虫对比数据源-组件展示数差异', '爬虫对比数据源-组件点击数差异'])

        styled_df.to_excel('巨量数据对比.xlsx', index=False)




    def test_vap_channel_analysis_room_lp(self, conn):
        pg_sql_1 = '''
            SELECT *
            FROM vap_channel_analysis_room
            WHERE etl_flag = 4 AND first_channel_code IN ('lp_free', 'lp_pay')
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)


        pg_sql_2 = '''
            WITH channel_flow_detail AS (
                SELECT douyin_no
                    , room_id
                    , (CASE
                    WHEN channel_index = '推荐feed' THEN 'homepage_hot'
                    WHEN channel_index = '直播广场' THEN 'live_merge'
                    WHEN channel_index = '同城' THEN 'homepage_fresh'
                    WHEN channel_index = '其他推荐场景' THEN 'other_recommend_live'
                    WHEN channel_index = '短视频引流' THEN 'video_to_live'
                    WHEN channel_index = '关注' THEN 'homepage_follow'
                    WHEN channel_index = '搜索' THEN 'general_search'
                    WHEN channel_index = '个人主页&店铺&橱窗' THEN 'others_homepage'
                    WHEN channel_index = '抖音商城推荐' THEN 'douyin_shopping_center'
                    WHEN channel_index = '活动页' THEN 'activity'
                    WHEN channel_index = '头条西瓜' THEN 'touxi_saas'
                    WHEN channel_index = '其他' THEN 'other'
                    WHEN channel_index = '千川PC版' THEN 'qianchuan_pc'
                    WHEN channel_index = '千川品牌广告' THEN 'qianchuan_brand'
                    WHEN channel_index = '品牌广告' THEN 'brand'
                    WHEN channel_index = '小店随心推' THEN 'suixintui'
                    WHEN channel_index = '其他广告' THEN 'other_advertisement'
                    END
                    ) AS second_channel_code
                    , CASE WHEN channel_index = '其他推荐场景' THEN '直播推荐-其他推荐场景' ELSE channel_index END AS second_channel_name
                    , CASE WHEN channel_index IN ('千川PC版', '千川品牌广告', '品牌广告', '小店随心推', '其他广告') THEN 'lp_pay' ELSE 'lp_free' END AS first_channel_code
                    , CASE WHEN channel_index IN ('千川PC版', '千川品牌广告', '品牌广告', '小店随心推', '其他广告') THEN '直播带货-罗盘-付费渠道' ELSE '直播带货-罗盘-自然渠道' END AS first_channel_name
                    , watch_cnt AS live_watch_cnt
                    , pay_amt/100::NUMERIC AS gmv
                    , round((CASE WHEN watch_cnt = 0 THEN 0 ELSE (pay_amt/100::NUMERIC) / watch_cnt::NUMERIC * 1000 END), 2) AS gpm
                FROM doudian_compass_channel_flow_detail
            --  WHERE room_id = '7208346635582081853'
            )
            SELECT B.*
                , A.start_time
                , A.end_time
                , 4 AS etl_flag
            FROM vap_douyin_live_rooms A
            INNER JOIN channel_flow_detail B ON A.room_id = B.room_id AND A.douyin_no = B.douyin_no
            WHERE A.company_id NOT IN ('3641931322505348352', '3641951028980126720', 'telecom_company_id', 'telecom_company_prod', 'demo')
                AND A.is_delete <> 1
                AND to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') >= '2023-03-08'

        '''
        df2 = self.execute_sql_stime(pg_sql_2, conn)

        m_df = pd.merge(df1, df2, how='outer', on=['douyin_no', 'room_id', 'second_channel_code', 'second_channel_name'])
        print(m_df)
        print(m_df.columns.values)
        m_df.fillna(-1, inplace = True)

        self.assert_info(m_df, 'etl_flag_x', 'etl_flag_y', col_list=['douyin_no', 'room_id', 'second_channel_name'])
        self.assert_info(m_df, 'start_time_x', 'start_time_y', col_list=['douyin_no', 'room_id', 'second_channel_name'])
        self.assert_info(m_df, 'end_time_x', 'end_time_y', col_list=['douyin_no', 'room_id', 'second_channel_name'])
        self.assert_info(m_df, 'first_channel_code_x', 'first_channel_code_y', col_list=['douyin_no', 'room_id', 'second_channel_name'])
        self.assert_info(m_df, 'first_channel_name_x', 'first_channel_name_y', col_list=['douyin_no', 'room_id', 'second_channel_name'])
        self.assert_info(m_df, 'live_watch_cnt_x', 'live_watch_cnt_y', col_list=['douyin_no', 'room_id', 'second_channel_name'])
        self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['douyin_no', 'room_id', 'second_channel_name'])
        self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['douyin_no', 'room_id', 'second_channel_name'])



    def test_vap_channel_analysis_room_jl(self, conn):
        pg_sql_1 = '''
            SELECT *, second_channel_code, second_channel_name
            FROM vap_channel_analysis_room
            WHERE etl_flag = 4 AND first_channel_code IN ('jl_free', 'jl_pay')
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        df_jl_free = df1[df1['first_channel_code'] == "jl_free"]
        for k, v in df_jl_free.groupby(['first_channel_code']):
            pg_sql_2 = '''
                WITH viewership AS (
                    SELECT douyin_no, room_id
                        , SUM(data_value) FILTER(WHERE data_name='自然流量') AS jl_watch_cnt
                        , 'jl_free' AS first_channel_code
                        , '直播留资-巨量-自然渠道'AS first_channel_name
                        , NULL AS second_channel_code
                        , NULL AS second_channel_name
                        , 0 AS live_watch_cnt
                        , 0 AS gmv
                        , 0 AS gpm
                    FROM juliangzongheng_live_big_screen_viewership
                    WHERE data_name = '自然流量'
                    GROUP BY douyin_no, room_id
                )
                SELECT B.*
                    , A.start_time
                    , A.end_time
                    , 4 AS etl_flag
                FROM vap_douyin_live_rooms A
                INNER JOIN viewership B ON A.room_id = B.room_id AND A.douyin_no = B.douyin_no
                WHERE A.company_id NOT IN ('3641931322505348352', '3641951028980126720', 'telecom_company_id', 'telecom_company_prod', 'demo')
                    AND A.is_delete <> 1
                    AND to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') >= '2023-03-08'

            '''
            df2 = self.execute_sql_stime(pg_sql_2, conn)
            print(df2)

            m_df = pd.merge(v, df2, how='outer', on=['douyin_no', 'room_id'])
            print(m_df)
            print(m_df.columns.values)
            m_df.fillna(-1, inplace = True)

            self.assert_info(m_df, 'etl_flag_x', 'etl_flag_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'start_time_x', 'start_time_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'end_time_x', 'end_time_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'first_channel_code_x', 'first_channel_code_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'first_channel_name_x', 'first_channel_name_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'live_watch_cnt_x', 'live_watch_cnt_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['douyin_no', 'room_id'])



        df_jl_pay = df1[df1['first_channel_code'] == "jl_pay"]
        for k, v in df_jl_pay.groupby(['first_channel_code']):
            pg_sql_3 = '''
                WITH viewership AS (
                    SELECT douyin_no, room_id, data_name
                        , (CASE WHEN data_name = '竞价广告' THEN 1
                            WHEN data_name = 'Dou+' THEN 2
                            WHEN data_name = '品牌广告' THEN 3
                        END) AS second_channel_code
                        , SUM(data_value) AS jl_watch_cnt
                        , 'jl_pay' AS first_channel_code
                        , '直播留资-巨量-付费渠道'AS first_channel_name
                        , NULL AS second_channel_code
                        , NULL AS second_channel_name
                        , 0 AS live_watch_cnt
                        , 0 AS gmv
                        , 0 AS gpm
                    FROM juliangzongheng_live_big_screen_viewership
                    WHERE data_name NOT IN ('全部', '自然流量', '其他')
                    GROUP BY douyin_no, room_id, data_name
                )
                SELECT B.*
                    , A.start_time
                    , A.end_time
                    , 4 AS etl_flag
                FROM vap_douyin_live_rooms A
                INNER JOIN viewership B ON A.room_id = B.room_id AND A.douyin_no = B.douyin_no
                WHERE A.company_id NOT IN ('3641931322505348352', '3641951028980126720', 'telecom_company_id', 'telecom_company_prod', 'demo')
                    AND A.is_delete <> 1
                    AND to_char(to_timestamp(A.start_time), 'YYYY-MM-DD') >= '2023-03-08'

            '''
            df2 = self.execute_sql_stime(pg_sql_3, conn)

            m_df = pd.merge(v, df2, how='outer', on=['douyin_no', 'room_id'])
            print(m_df)
            print(m_df.columns.values)
            m_df.fillna(-1, inplace = True)

            self.assert_info(m_df, 'etl_flag_x', 'etl_flag_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'start_time_x', 'start_time_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'end_time_x', 'end_time_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'first_channel_code_x', 'first_channel_code_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'first_channel_name_x', 'first_channel_name_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'live_watch_cnt_x', 'live_watch_cnt_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['douyin_no', 'room_id'])
            self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['douyin_no', 'room_id'])




    def test_vap_channel_analysis_room_minute_jl_free(self, conn):
        pg_sql_1 = '''
            SELECT to_timestamp(pg_timestamp) AS str_time, *
            FROM vap_channel_analysis_room_minute
            WHERE etl_flag = 4 AND first_channel_code = 'jl_free'
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        for k, v in df1.groupby(['room_id']):
            room_id = k

            pg_sql_2 = f'''
                WITH generate AS (
                    SELECT room_id, start_time, end_time, (end_time - start_time) / 60 AS live_duration
                        , douyin_no
                        , generate_series(s_start_time, s_end_time, 60) AS "timestamp"
                    FROM (
                        SELECT room_id
                            , douyin_no
                            , start_time
                            , end_time
                            , start_time - start_time % 60 - 60 AS s_start_time
                            , end_time - end_time % 60 + 60 AS s_end_time
                        FROM vap_douyin_live_rooms
                        WHERE company_id NOT IN ('3641931322505348352', '3641951028980126720', 'telecom_company_id', 'telecom_company_prod', 'demo')
                            AND is_delete <> 1
                            AND to_char(to_timestamp(start_time), 'YYYY-MM-DD') >= '2023-03-08'
                    ) t
                )
                SELECT
                    B.douyin_no, B.room_id
                    , to_timestamp(B.data_time_stamp)
                    , B.data_time_stamp AS pg_timestamp
                    , 4 AS etl_flag
                    , 'jl_free' AS first_channel_code
                    , '直播留资-巨量-自然渠道' AS first_channel_name
                    , '' AS second_channel_code
                    , '' AS second_channel_name
                    , B.data_value AS jl_watch_cnt
                    , -1 AS live_watch_cnt
                    , -1 AS gmv
                    , -1 AS gpm
                    , B.data_name
                FROM generate A
                LEFT JOIN juliangzongheng_live_big_screen_viewership B ON A.douyin_no = B.douyin_no AND A.room_id = B.room_id AND A."timestamp" = B.data_time_stamp
                WHERE B.data_name = '自然流量' AND A.room_id = '{room_id}'
                ORDER BY B.data_time_stamp

            '''
            df2 = self.execute_sql_stime(pg_sql_2, conn)


            m_df = pd.merge(v, df2, how='right', on=['douyin_no', 'room_id', 'pg_timestamp'])
            # print(m_df)
            print(m_df.columns.values)
            # m_df.fillna(-1, inplace = True)


            self.assert_info(m_df, 'etl_flag_x', 'etl_flag_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'first_channel_code_x', 'first_channel_code_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'first_channel_name_x', 'first_channel_name_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'second_channel_code_x', 'second_channel_code_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'second_channel_name_x', 'second_channel_name_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'live_watch_cnt_x', 'live_watch_cnt_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])




    def test_vap_channel_analysis_room_minute_jl_pay(self, conn):
        pg_sql_1 = '''
            SELECT to_timestamp(pg_timestamp) AS str_time, *
            FROM vap_channel_analysis_room_minute
            WHERE etl_flag = 4 AND first_channel_code = 'jl_pay'
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        for k, v in df1.groupby(['room_id']):
            room_id = k

            pg_sql_2 = f'''
                WITH generate AS (
                    SELECT room_id, start_time, end_time, (end_time - start_time) / 60 AS live_duration
                        , douyin_no
                        , generate_series(s_start_time, s_end_time, 60) AS "timestamp"
                    FROM (
                        SELECT room_id
                            , douyin_no
                            , start_time
                            , end_time
                            , start_time - start_time % 60 - 60 AS s_start_time
                            , end_time - end_time % 60 + 60 AS s_end_time
                        FROM vap_douyin_live_rooms
                        WHERE company_id NOT IN ('3641931322505348352', '3641951028980126720', 'telecom_company_id', 'telecom_company_prod', 'demo')
                            AND is_delete <> 1
                            AND to_char(to_timestamp(start_time), 'YYYY-MM-DD') >= '2023-03-08'
                    ) t
                )
                SELECT
                    B.douyin_no, B.room_id
                    , to_timestamp(B.data_time_stamp)
                    , B.data_time_stamp AS pg_timestamp
                    , 4 AS etl_flag
                    , 'jl_pay' AS first_channel_code
                    , '直播留资-巨量-付费渠道' AS first_channel_name
                    , (CASE WHEN data_name = '竞价广告' THEN '1'
                        WHEN data_name = 'Dou+' THEN '2'
                        WHEN data_name = '品牌广告' THEN '3'
                    END ) AS second_channel_code
                    , data_name AS second_channel_name
                    , B.data_value AS jl_watch_cnt
                    , -1 AS live_watch_cnt
                    , -1 AS gmv
                    , -1 AS gpm
                    , B.data_name
                FROM generate A
                LEFT JOIN juliangzongheng_live_big_screen_viewership B ON A.douyin_no = B.douyin_no AND A.room_id = B.room_id AND A."timestamp" = B.data_time_stamp
                WHERE B.data_name NOT IN ('全部' ,'自然流量' , '其他') AND A.room_id = '{room_id}'
                ORDER BY B.data_time_stamp

            '''
            df2 = self.execute_sql_stime(pg_sql_2, conn)


            m_df = pd.merge(v, df2, how='outer', on=['douyin_no', 'room_id', 'pg_timestamp'])
            # print(m_df)
            print(m_df.columns.values)
            # m_df.fillna(-1, inplace = True)


            self.assert_info(m_df, 'etl_flag_x', 'etl_flag_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'first_channel_code_x', 'first_channel_code_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'first_channel_name_x', 'first_channel_name_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'second_channel_code_x', 'second_channel_code_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'second_channel_name_x', 'second_channel_name_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'live_watch_cnt_x', 'live_watch_cnt_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'gmv_x', 'gmv_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'gpm_x', 'gpm_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])
            self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['douyin_no', 'room_id', 'str_time', 'pg_timestamp'])


    def test_vap_channel_analysis_room_minute_lp_free(self, conn):
        pg_sql_1 = '''
            SELECT to_timestamp(pg_timestamp), *
            FROM vap_channel_analysis_room_minute
            WHERE etl_flag = 4 AND first_channel_code = 'lp_free'
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        for k, v in df1.groupby(['room_id']):
            print(k)
            room_id = k

            pg_sql_2 = f'''
                WITH generate AS (
                    SELECT room_id, start_time, end_time, (end_time - start_time) / 60 AS live_duration
                        , douyin_no
                        , generate_series(s_start_time, s_end_time, 60) AS "timestamp"
                    FROM (
                        SELECT room_id
                            , douyin_no
                            , start_time
                            , end_time
                            , start_time - start_time % 60 - 60 AS s_start_time
                            , end_time - end_time % 60 + 60 AS s_end_time
                        FROM vap_douyin_live_rooms
                        WHERE company_id NOT IN ('3641931322505348352', '3641951028980126720', 'telecom_company_id', 'telecom_company_prod', 'demo')
                            AND is_delete <> 1
                            AND to_char(to_timestamp(start_time), 'YYYY-MM-DD') >= '2023-03-08'
                            AND room_id = '{room_id}'
                    ) t
                ),
                source_trend AS (
                    SELECT * FROM doudian_compass_traffic_source_trend WHERE room_id = '{room_id}'
                ),
                flow_distribution AS (
                    SELECT * FROM doudian_compass_big_screen_flow_distribution WHERE room_id = '{room_id}'
                )
                SELECT
                    B.douyin_no, B.room_id
                    , to_timestamp(B.date_time)
                    , B.date_time AS pg_timestamp
                    , 4 AS etl_flag
                    , 'lp_free' AS first_channel_code
                    , '直播带货-罗盘-自然渠道' AS first_channel_name
                    , (CASE
                    WHEN point_name = '推荐feed' THEN 'homepage_hot'
                    WHEN point_name = '直播广场' THEN 'live_merge'
                    WHEN point_name = '同城' THEN 'homepage_fresh'
                    WHEN point_name = '其他推荐场景' THEN 'other_recommend_live'
                    WHEN point_name = '短视频引流' THEN 'video_to_live'
                    WHEN point_name = '关注' THEN 'homepage_follow'
                    WHEN point_name = '搜索' THEN 'general_search'
                    WHEN point_name = '个人主页&店铺&橱窗' THEN 'others_homepage'
                    WHEN point_name = '抖音商城推荐' THEN 'douyin_shopping_center'
                    WHEN point_name = '活动页' THEN 'activity'
                    WHEN point_name = '头条西瓜' THEN 'touxi_saas'
                    WHEN point_name = '其他' THEN 'other'
                    WHEN point_name = '千川PC版' THEN 'qianchuan_pc'
                    WHEN point_name = '千川品牌广告' THEN 'qianchuan_brand'
                    WHEN point_name = '品牌广告' THEN 'brand'
                    WHEN point_name = '小店随心推' THEN 'suixintui'
                    WHEN point_name = '其他广告' THEN 'other_advertisement'
                    ELSE 'other_recommend_live'
                    END
                    ) AS second_channel_code
                    , CASE WHEN point_name = '其他推荐场景' THEN '直播推荐-其他推荐场景' ELSE point_name END AS second_channel_name
                    , -1 AS jl_watch_cnt
                    , vertical AS live_watch_cnt
                FROM generate A
                LEFT JOIN source_trend B ON A.room_id = B.room_id AND A.timestamp = B.date_time
                WHERE display_name LIKE '自然流量%' AND point_name <> '自然流量'

            '''

            df2 = self.execute_sql_stime(pg_sql_2, conn)

            m_df = pd.merge(df1, df2, how='right', on=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            # print(m_df)
            print(m_df.columns.values)
            # m_df.fillna(-1, inplace = True)


            self.assert_info(m_df, 'etl_flag_x', 'etl_flag_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(m_df, 'first_channel_code_x', 'first_channel_code_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(m_df, 'first_channel_name_x', 'first_channel_name_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(m_df, 'second_channel_code_x', 'second_channel_code_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(m_df, 'live_watch_cnt_x', 'live_watch_cnt_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])


            pg_sql_3 = f'''
                SELECT *
                    , (CASE WHEN pay_amt - LAG(pay_amt, 1, 0::NUMERIC) OVER(PARTITION BY douyin_no, room_id, second_channel_code ORDER BY pg_timestamp) < 0 THEN 0
                    ELSE pay_amt - LAG(pay_amt, 1, 0::NUMERIC) OVER(PARTITION BY douyin_no, room_id, second_channel_code ORDER BY pg_timestamp) END) AS gmv
                FROM (
                    SELECT
                        douyin_no, room_id
                        , to_timestamp(create_time - create_time%60)
                        , create_time
                        , create_time - create_time%60 AS pg_timestamp
                        , ROW_NUMBER() OVER(PARTITION BY douyin_no, room_id, (create_time - create_time%60), channel_name, child_channel_name ORDER BY create_time) AS rn
                        , 4 AS etl_flag
                        , 'lp_free' AS first_channel_code
                        , '直播带货-罗盘-自然渠道' AS first_channel_name
                        , (CASE
                        WHEN channel_name = '推荐feed' OR child_channel_name ='推荐feed' THEN 'homepage_hot'
                        WHEN channel_name = '直播广场' OR child_channel_name = '直播广场' THEN 'live_merge'
                        WHEN channel_name = '同城' OR child_channel_name = '同城' THEN 'homepage_fresh'
                        WHEN channel_name = '其他推荐场景' OR child_channel_name = '其他推荐场景' THEN 'other_recommend_live'
                        WHEN channel_name = '短视频引流' THEN 'video_to_live'
                        WHEN channel_name = '关注' THEN 'homepage_follow'
                        WHEN channel_name = '搜索' THEN 'general_search'
                        WHEN channel_name = '个人主页&店铺&橱窗' THEN 'others_homepage'
                        WHEN channel_name = '抖音商城推荐' THEN 'douyin_shopping_center'
                        WHEN channel_name = '活动页' THEN 'activity'
                        WHEN channel_name = '头条西瓜' THEN 'touxi_saas'
                        WHEN channel_name = '其他' THEN 'other'
                        WHEN channel_name = '千川PC版' THEN 'qianchuan_pc'
                        WHEN channel_name = '千川品牌广告' THEN 'qianchuan_brand'
                        WHEN channel_name = '品牌广告' THEN 'brand'
                        WHEN channel_name = '小店随心推' THEN 'suixintui'
                        WHEN channel_name = '其他广告' THEN 'other_advertisement'
                        END
                        ) AS second_channel_code
                        , CASE WHEN channel_name = '其他推荐场景' OR child_channel_name = '其他推荐场景' THEN '直播推荐-其他推荐场景'
                        ELSE (CASE WHEN channel_name = '直播推荐' THEN child_channel_name ELSE channel_name END) END AS second_channel_name
                        , (CASE
                        WHEN channel_name = '推荐feed' OR child_channel_name ='推荐feed' THEN child_pay_amt/100::NUMERIC
                        WHEN channel_name = '直播广场' OR child_channel_name = '直播广场' THEN child_pay_amt/100::NUMERIC
                        WHEN channel_name = '同城' OR child_channel_name = '同城' THEN child_pay_amt/100::NUMERIC
                        WHEN channel_name = '其他推荐场景' OR child_channel_name = '其他推荐场景' THEN child_pay_amt/100::NUMERIC
                        ELSE pay_amt/100::NUMERIC
                        END ) AS pay_amt
                        , round(CASE
                        WHEN channel_name = '推荐feed' OR child_channel_name ='推荐feed' THEN child_gpm/100::NUMERIC
                        WHEN channel_name = '直播广场' OR child_channel_name = '直播广场' THEN child_gpm/100::NUMERIC
                        WHEN channel_name = '同城' OR child_channel_name = '同城' THEN child_gpm/100::NUMERIC
                        WHEN channel_name = '其他推荐场景' OR child_channel_name = '其他推荐场景' THEN child_gpm/100::NUMERIC
                        ELSE gpm/100::NUMERIC
                        END, 2) AS gpm
                    FROM doudian_compass_big_screen_flow_distribution
                    WHERE room_id = '{room_id}' AND flow_type = 'nature'
                ) t WHERE rn = 1

            '''
            df3 = self.execute_sql_stime(pg_sql_3, conn)

            merge_df = pd.merge(df1, df3, how='right', on=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            # print(m_df)
            print(merge_df.columns.values)
            # m_df.fillna(-1, inplace = True)

            self.assert_info(merge_df, 'etl_flag_x', 'etl_flag_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(merge_df, 'first_channel_code_x', 'first_channel_code_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(merge_df, 'first_channel_name_x', 'first_channel_name_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(merge_df, 'second_channel_code_x', 'second_channel_code_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(merge_df, 'gmv_x', 'gmv_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(merge_df, 'gpm_x', 'gpm_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])



    def test_vap_channel_analysis_room_minute_lp_pay(self, conn):
        pg_sql_1 = '''
            SELECT to_timestamp(pg_timestamp), *
            FROM vap_channel_analysis_room_minute
            WHERE etl_flag = 4 AND first_channel_code = 'lp_pay'
        '''
        df1 = self.execute_sql_stime(pg_sql_1, conn)

        for k, v in df1.groupby(['room_id']):
            print(k)
            room_id = k

            pg_sql_2 = f'''
                WITH generate AS (
                    SELECT room_id, start_time, end_time, (end_time - start_time) / 60 AS live_duration
                        , douyin_no
                        , generate_series(s_start_time, s_end_time, 60) AS "timestamp"
                    FROM (
                        SELECT room_id
                            , douyin_no
                            , start_time
                            , end_time
                            , start_time - start_time % 60 - 60 AS s_start_time
                            , end_time - end_time % 60 + 60 AS s_end_time
                        FROM vap_douyin_live_rooms
                        WHERE company_id NOT IN ('3641931322505348352', '3641951028980126720', 'telecom_company_id', 'telecom_company_prod', 'demo')
                            AND is_delete <> 1
                            AND to_char(to_timestamp(start_time), 'YYYY-MM-DD') >= '2023-03-08'
                            AND room_id = '{room_id}'
                    ) t
                ),
                source_trend AS (
                    SELECT * FROM doudian_compass_traffic_source_trend WHERE room_id = '{room_id}'
                ),
                flow_distribution AS (
                    SELECT * FROM doudian_compass_big_screen_flow_distribution WHERE room_id = '{room_id}'
                )
                SELECT
                    B.douyin_no, B.room_id
                    , to_timestamp(B.date_time)
                    , B.date_time AS pg_timestamp
                    , 4 AS etl_flag
                    , 'lp_pay' AS first_channel_code
                    , '直播带货-罗盘-付费渠道' AS first_channel_name
                    , (CASE
                    WHEN point_name = '推荐feed' THEN 'homepage_hot'
                    WHEN point_name = '直播广场' THEN 'live_merge'
                    WHEN point_name = '同城' THEN 'homepage_fresh'
                    WHEN point_name = '其他推荐场景' THEN 'other_recommend_live'
                    WHEN point_name = '短视频引流' THEN 'video_to_live'
                    WHEN point_name = '关注' THEN 'homepage_follow'
                    WHEN point_name = '搜索' THEN 'general_search'
                    WHEN point_name = '个人主页&店铺&橱窗' THEN 'others_homepage'
                    WHEN point_name = '抖音商城推荐' THEN 'douyin_shopping_center'
                    WHEN point_name = '活动页' THEN 'activity'
                    WHEN point_name = '头条西瓜' THEN 'touxi_saas'
                    WHEN point_name = '其他' THEN 'other'
                    WHEN point_name = '千川PC版' THEN 'qianchuan_pc'
                    WHEN point_name = '千川品牌广告' THEN 'qianchuan_brand'
                    WHEN point_name = '品牌广告' THEN 'brand'
                    WHEN point_name = '小店随心推' THEN 'suixintui'
                    WHEN point_name = '其他广告' THEN 'other_advertisement'
                    ELSE 'other_recommend_live'
                    END
                    ) AS second_channel_code
                    , CASE WHEN point_name = '其他推荐场景' THEN '直播推荐-其他推荐场景' ELSE point_name END AS second_channel_name
                    , -1 AS jl_watch_cnt
                    , vertical AS live_watch_cnt
                FROM generate A
                LEFT JOIN source_trend B ON A.room_id = B.room_id AND A.timestamp = B.date_time
                WHERE display_name LIKE '付费流量%' AND point_name <> '付费流量'

            '''

            df2 = self.execute_sql_stime(pg_sql_2, conn)

            m_df = pd.merge(df1, df2, how='right', on=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            # print(m_df)
            print(m_df.columns.values)
            # m_df.fillna(-1, inplace = True)


            # self.assert_info(m_df, 'etl_flag_x', 'etl_flag_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(m_df, 'first_channel_code_x', 'first_channel_code_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(m_df, 'first_channel_name_x', 'first_channel_name_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(m_df, 'second_channel_code_x', 'second_channel_code_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(m_df, 'live_watch_cnt_x', 'live_watch_cnt_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(m_df, 'jl_watch_cnt_x', 'jl_watch_cnt_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])


            pg_sql_3 = f'''
                SELECT *
                    , (CASE WHEN pay_amt - LAG(pay_amt, 1, 0::NUMERIC) OVER(PARTITION BY douyin_no, room_id, second_channel_code ORDER BY pg_timestamp) < 0 THEN 0
                    ELSE pay_amt - LAG(pay_amt, 1, 0::NUMERIC) OVER(PARTITION BY douyin_no, room_id, second_channel_code ORDER BY pg_timestamp) END) AS gmv
                FROM (
                    SELECT
                        douyin_no, room_id
                        , to_timestamp(create_time - create_time%60)
                        , create_time
                        , create_time - create_time%60 AS pg_timestamp
                        , ROW_NUMBER() OVER(PARTITION BY douyin_no, room_id, (create_time - create_time%60), channel_name, child_channel_name ORDER BY create_time) AS rn
                        , 4 AS etl_flag
                        , 'lp_pay' AS first_channel_code
                        , '直播带货-罗盘-付费渠道' AS first_channel_name
                        , (CASE
                        WHEN channel_name = '推荐feed' OR child_channel_name ='推荐feed' THEN 'homepage_hot'
                        WHEN channel_name = '直播广场' OR child_channel_name = '直播广场' THEN 'live_merge'
                        WHEN channel_name = '同城' OR child_channel_name = '同城' THEN 'homepage_fresh'
                        WHEN channel_name = '其他推荐场景' OR child_channel_name = '其他推荐场景' THEN 'other_recommend_live'
                        WHEN channel_name = '短视频引流' THEN 'video_to_live'
                        WHEN channel_name = '关注' THEN 'homepage_follow'
                        WHEN channel_name = '搜索' THEN 'general_search'
                        WHEN channel_name = '个人主页&店铺&橱窗' THEN 'others_homepage'
                        WHEN channel_name = '抖音商城推荐' THEN 'douyin_shopping_center'
                        WHEN channel_name = '活动页' THEN 'activity'
                        WHEN channel_name = '头条西瓜' THEN 'touxi_saas'
                        WHEN channel_name = '其他' THEN 'other'
                        WHEN channel_name = '千川PC版' THEN 'qianchuan_pc'
                        WHEN channel_name = '千川品牌广告' THEN 'qianchuan_brand'
                        WHEN channel_name = '品牌广告' THEN 'brand'
                        WHEN channel_name = '小店随心推' THEN 'suixintui'
                        WHEN channel_name = '其他广告' THEN 'other_advertisement'
                        END
                        ) AS second_channel_code
                        , CASE WHEN channel_name = '其他推荐场景' OR child_channel_name = '其他推荐场景' THEN '直播推荐-其他推荐场景'
                        ELSE (CASE WHEN channel_name = '直播推荐' THEN child_channel_name ELSE channel_name END) END AS second_channel_name
                        , (CASE
                        WHEN channel_name = '推荐feed' OR child_channel_name ='推荐feed' THEN child_pay_amt/100::NUMERIC
                        WHEN channel_name = '直播广场' OR child_channel_name = '直播广场' THEN child_pay_amt/100::NUMERIC
                        WHEN channel_name = '同城' OR child_channel_name = '同城' THEN child_pay_amt/100::NUMERIC
                        WHEN channel_name = '其他推荐场景' OR child_channel_name = '其他推荐场景' THEN child_pay_amt/100::NUMERIC
                        ELSE pay_amt/100::NUMERIC
                        END ) AS pay_amt
                        , round(CASE
                        WHEN channel_name = '推荐feed' OR child_channel_name ='推荐feed' THEN child_gpm/100::NUMERIC
                        WHEN channel_name = '直播广场' OR child_channel_name = '直播广场' THEN child_gpm/100::NUMERIC
                        WHEN channel_name = '同城' OR child_channel_name = '同城' THEN child_gpm/100::NUMERIC
                        WHEN channel_name = '其他推荐场景' OR child_channel_name = '其他推荐场景' THEN child_gpm/100::NUMERIC
                        ELSE gpm/100::NUMERIC
                        END, 2) AS gpm
                    FROM doudian_compass_big_screen_flow_distribution
                    WHERE room_id = '{room_id}' AND flow_type = 'pay'
                ) t WHERE rn = 1

            '''
            df3 = self.execute_sql_stime(pg_sql_3, conn)

            merge_df = pd.merge(df1, df3, how='right', on=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            # print(m_df)
            print(merge_df.columns.values)
            # m_df.fillna(-1, inplace = True)

            # self.assert_info(merge_df, 'etl_flag_x', 'etl_flag_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(merge_df, 'first_channel_code_x', 'first_channel_code_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(merge_df, 'first_channel_name_x', 'first_channel_name_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(merge_df, 'second_channel_code_x', 'second_channel_code_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(merge_df, 'gmv_x', 'gmv_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])
            self.assert_info(merge_df, 'gpm_x', 'gpm_y', col_list=['douyin_no', 'room_id', 'pg_timestamp', 'second_channel_name'])





if __name__ == '__main__':

    # report_xml = os.getcwd() + '/report/xml/'
    # report_html = os.getcwd() + '/report/html/'
    # report_html_result = report_html + "report_time_{}".format(time.strftime("%Y%m%d%H%M%S", time.localtime(time.time())))

    pytest.main([
                '-s'
                , '-v'
                , 'test_vap.py::Test_Vap_Data::test_check_VAP_API_channel_jl_flow'
                # , '--alluredir'
                # , report_xml
                # , '--clean-alluredir'
                ])

    # allure_report = glob.glob(report_html + "*")
    # if allure_report.__len__() > 5:
    #     shutil.rmtree(allure_report[0])


    # os.popen(f'allure generate {report_xml} -o {report_html_result} --clean').read()


    # test_check_vap_douyin_live_rooms()
    # test_check_vap_anchor_live_time_period()
    # test_check_vap_anchor_live_time_period_products()
    # test_check_vap_topic_detail()



