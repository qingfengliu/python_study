# -*- coding: utf-8 -*-
sql_full_tuangou="""
select `deal_id`,`dt`,`shop_id`,`end_time`,`shop_name`,`start_time`,`hits`,
`today_hits`,`old_price`,`shop_num`,`description`,`description_m`,`new_price`,`日核消费量` as `sales_check`,`sales`,`biz_name_m`,`biz_name`,
`district_name_m`,`district_name`,`city_name_m`,`city_name`,`category2_name_m`,`category2_name`,`title`,concat('http://t.dianping.com/deal/',`deal_id`) as `check_url`
 from 
(
	(
			select * from
			(
				SELECT aaa.*, bbb.first_dt,bbb.last_dt, bbb.shop_num, aaa.sales/bbb.shop_num AS 单店销量,ccc.日核消费量
				FROM
				(
					SELECT * FROM o2o.t_hh_dianping_tuangou_deal_info WHERE dt > '%(start_time)s' and `dt`<='%(end_time)s'
				) aaa
		#日核销量
				LEFT JOIN 
				(
                    select aaa.`deal_id`,aaa.`消费日期`,count(*) as `日核消费量` from (
                    select `deal_id`,`shopId`,date(`consume_time`)  as `消费日期` ,`Serial`
                    from ``.test_ChongYiShengConsume group by `Serial`) aaa group by `deal_id`
				) ccc
				ON aaa.deal_id = ccc.deal_id and aaa.dt=ccc.消费日期
				JOIN
				(
					SELECT deal_id, MAX(dt) AS last_dt, MIN(dt) AS first_dt, COUNT(DISTINCT shop_id) AS shop_num
					FROM o2o.t_hh_dianping_tuangou_deal_info 
					WHERE  dt > '%(start_time)s' and `dt`<='%(end_time)s' AND end_time >= dt GROUP BY deal_id,dt
				) bbb
				ON aaa.deal_id = bbb.deal_id AND aaa.dt = last_dt
			) tuangou_info
			INNER JOIN
			(
			SELECT `dpsi_id`,`shop_id` as id_shop,`shop_name`,`category1_id`,`category1_name`,`category2_id`,`category2_name`,`category3_id`,
			`category3_name`,`group_id`,`group_name`,`create_dt`,`last_update_dt`,`district_id`,
			`district_name`,`biz_id`,`biz_name`,`address`,`lng`,`lat`,`avg_price`,`shop_power`,`shop_power_title`,`branch_total`,
			`dish_tags`,`display_score`,`display_score1_name`,`display_score1`,`display_score2_name`,
			`display_score2`,`display_score3_name`,`display_score3`,`hits`,`month_hits`,`phone_no`,`pic_total`,
			`popularity`,`primary_tag`,`shop_tags`,`shop_type`,`vote_total`,`vote_default`,`wish_total`,`vote_tag`,`shop_status`,
			`gaode_id`,`dt` as `td`,`prev_weekly_hits`,`today_hits`,`weekly_hits`
			FROM o2o.t_hh_dianping_shop_info_pet_hospital WHERE  dt > '%(start_time)s' and `dt`<='%(end_time)s' 
		) shop_info
		ON tuangou_info.dt=shop_info.td and tuangou_info.shop_id = shop_info.id_shop
		) eee
		inner join(
			select `deal_id` as `hebing_deal_id`,group_concat(distinct `商圈` order by `商圈` separator '/') as `biz_name_m`,group_concat(distinct `城区` order by `城区` separator '/') as `district_name_m`,
			group_concat(distinct `城市` order by `城市` separator '/') as `city_name_m`,group_concat(distinct `店铺类型` order by `店铺类型` separator '/') as `category2_name_m`
			from (select `deal_id`,aaa.`shop_id`,aaa.`dt`,`商圈`,`城区`,`城市`,`店铺类型` from 
					(SELECT `deal_id`,`shop_id`,`dt` FROM o2o.t_hh_dianping_tuangou_deal_info WHERE 
						dt > '%(start_time)s' and `dt`<='%(end_time)s' group by `deal_id`,`shop_id`,`dt`
					) aaa
					inner join
					(select `shop_id` ,`dt`,`district_name` as `城区`,`biz_name` as `商圈`,`city_name` as `城市`,
						`category2_name` as `店铺类型` from o2o.t_hh_dianping_shop_info_pet_hospital group by `shop_id` ,`dt`
					) bbb
					on aaa.shop_id=bbb.shop_id and aaa.dt=bbb.dt
				) aaa group by `deal_id`
		) fff
		on fff.hebing_deal_id=eee.deal_id
        inner join(
			select `deal_id` as `description_deal_id`,group_concat(distinct `description` order by `description` separator '/') as `description_m` from o2o.t_hh_dianping_tuangou_deal_info group by `deal_id`
        ) ggg
        on ggg.description_deal_id=eee.deal_id
);
"""

sql_full_liuliang="""
select aaa.`shop_id`,aaa.`dt`,bbb.`pv`,ccc.`click_count` from (
(
    (select `shop_id`,`dt` from ``.t_app_pet_hospital_traffic_data where `dt`>'%(start_time)s' and `dt`<='%(end_time)s')
union
    (select `shop_id`,`dt` from ``.t_app_pet_hospital_merchantpage_click_info where `dt`>'%(start_time)s' and `dt`<='%(end_time)s')
)
aaa
left join
    (
        select `shop_id`,`dt`,`pv` from ``.t_app_pet_hospital_traffic_data where `plat`='全部平台' and `dt`>'%(start_time)s' and `dt`<='%(end_time)s' 
        group by `shop_id`,`dt`
    ) bbb
    on bbb.shop_id=aaa.shop_id and bbb.dt=aaa.dt
left join
    (
        select aaa.`shop_id`,aaa.`dt`,sum(aaa.`click_count`) as `click_count` from 
        (select `shop_id`,`dt`,`click_count`
        from ``.t_app_pet_hospital_merchantpage_click_info where `click_module`='团购' and `dt`>'%(start_time)s' and `dt`<='%(end_time)s' 
        group by `shop_id`,`dt`,`click_module` 
        ) aaa group by aaa.`shop_id`,aaa.`dt`
    ) ccc
    on ccc.shop_id=aaa.shop_id and ccc.dt=aaa.dt
) group by aaa.`shop_id`,aaa.`dt`;
"""

sql_source_meituan="""
select aaa.*,bbb.shop_num,ccc.`category2_name`,ccc.`city_name`,ccc.`district_name`,concat('http://',ccc.`host`,'/deal/',aaa.`deal_id`,'.html') as `check_url`,ddd.`category2_name_m`,
ddd.`city_name_m`,ddd.`district_name_m`,eee.description_m from 
	(
		select `mtdealid` as `deal_id`,`shop_id`,`dt`,`start_time`,`end_time`,`shop_name`,`title` as `description`,`originalPrice` as `old_price`,
		`price` as `new_price`,`solds` as `sales`,'美团' as `platform`,`dpDealGroupId` as `con_deal_id`,
		`dpShopId` as `con_shop_id` from .tuangou_pet_meituan_to_dianping_info where `dt`>"%(start_time)s" and `dt`<="%(end_time)s"
	) aaa 
	join  
	(
		SELECT deal_id,dt,count(*) as shop_num FROM .tuangou_meituan_deal_id where `dt`>"%(start_time)s" and `dt`<="%(end_time)s" group by `deal_id`,`dt`
	) bbb 
	on aaa.deal_id=bbb.deal_id and aaa.dt=bbb.dt
	join(
		select `mtshop_id`,`dt`,`type` as `category2_name`,`city_name`,`distract` as `district_name`,`host` from .tuangou_meituan_shop_info where `dt`>"%(start_time)s" and `dt`<="%(end_time)s"
    )ccc
	on ccc.mtshop_id=aaa.shop_id and ccc.dt=aaa.dt
	join (
		select aaa.`deal_id`,aaa.dt,group_concat(distinct `category2_name` order by `category2_name` separator '/') as `category2_name_m`,
		group_concat(distinct `city_name` order by `city_name` separator '/') as `city_name_m`,
		group_concat(distinct `distract` order by `distract` separator '/') as `district_name_m`
		from (
			select aaa.*,bbb.deal_id from (select `mtshop_id`,`type` as `category2_name`,`city_name`,`distract`,`dt` from .tuangou_meituan_shop_info where `dt`>"%(start_time)s" and `dt`<="%(end_time)s"
		) aaa
		join (
			select `deal_id`,`shop_id`,`dt` from .tuangou_meituan_deal_id where `dt`>"%(start_time)s" and `dt`<="%(end_time)s"
		) bbb 
		on aaa.mtshop_id=bbb.shop_id and aaa.`dt`=bbb.`dt`) aaa group by aaa.`deal_id`,aaa.dt
	)ddd
	on ddd.`deal_id`=aaa.`deal_id` and ddd.`dt`=`aaa`.`dt`
	join (
		select `mtdealid`,concat(shop_name,',',group_concat(distinct `title` order by `title` separator '/')) as `description_m` from .tuangou_pet_meituan_to_dianping_info where `dt`>"%(start_time)s" and `dt`<="%(end_time)s" group by `mtdealid`
	)eee 
	on eee.`mtdealid`=aaa.`deal_id`
 ;
"""

sql_source_meituan_and_dianping='''
select aaa.*,ccc.check_url from (
	(
		select aaa.*,bbb.hits_meitun,bbb.today_hits_meitun,bbb.sales_meitun,bbb.click_count_meitun,
		bbb.pv_meitun,bbb.shop_num_meitun,bbb.start_time_meitun
		from(
			select * from o2o.tuangou_pet_source_data where platform='大众点评' and `dt`>"%(start_time)s" and `dt`<="%(end_time)s"
		) aaa
		left join 
		(
			select `con_deal_id`,`start_time` as `start_time_meitun`,`hits` as `hits_meitun`,`today_hits` as `today_hits_meitun`,`sales` as `sales_meitun`,
			`click_count` as `click_count_meitun`,`pv` as `pv_meitun`,`shop_num`as `shop_num_meitun`,`dt`
			from o2o.tuangou_pet_source_data where platform='美团' and `dt`>"%(start_time)s" and `dt`<="%(end_time)s"
            group by `con_deal_id`,`dt`
		) bbb
		on aaa.`deal_id`=bbb.con_deal_id and aaa.`dt`=bbb.`dt`
	) aaa
	left join(
		select con_deal_id as con_deal_x,`check_url` from o2o.tuangou_pet_source_data aaa where 
			platform='美团' and TO_DAYS("%(end_time)s") - TO_DAYS(`dt`)<= 30 and exists(
			select `deal_id` from (select `con_deal_id`,max(`dt`) as `dt` from o2o.tuangou_pet_source_data where TO_DAYS("%(end_time)s") - TO_DAYS(`dt`)<= 30 
					group by `con_deal_id` 
					) bbb where aaa.con_deal_id=bbb.`con_deal_id` and aaa.dt=bbb.dt
		) group by con_deal_id
	)ccc on aaa.`con_deal_id`=ccc.`con_deal_x`
);
'''