# flink-arctic-demo


在 Flink Standalone 模式下，使用 FlinkCDC 从 MySQL8.x 实时读取数据写入 Kafka(Upset-kafka), 再从 Kafka 实时读取写入 Arctic。


# 一、环境准备

## 1.1 MySQL
### 1.1.1 搭建
```bash
# 编译 docker-compose 文件
$ vim docker-compose.yaml
version: "3"
services:
    zookeeper:
        image: wurstmeister/zookeeper:3.4.6
        ports:
            - "2181:2181"
    kafka:
        container_name: kafka
        image: wurstmeister/kafka:2.12-2.2.1
        ports:
            - "9092:9092"
            - "9094:9094"
        depends_on:
             - zookeeper
        environment:
            - KAFKA_INTER_BROKER_LISTENER_NAME=INSIDE
            - KAFKA_ADVERTISED_LISTENERS=INSIDE://:9094,OUTSIDE://localhost:9092
            - KAFKA_LISTENERS=INSIDE://:9094,OUTSIDE://:9092
            - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT
            - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
            - KAFKA_CREATE_TOPICS:"users_topic:4:2,pageviews_topic:4:2,pageviews_per_region_5_miniutes_topic:4:1,pageviews_per_region_topic:4:1"
        volumes:
            - kafka:/var/run/docker.sock
    debz-mysql:
        container_name: mysql
        image: mysql:8.0
        ports:
            - "3306:3306"
        environment:
            - MYSQL_ROOT_PASSWORD=123456
    schema-registry:
        image: confluentinc/cp-schema-registry:5.5.2
        ports:
            - "8082:8082"
        depends_on:
            - zookeeper
            - kafka
        environment:
            - SCHEMA_REGISTRY_HOST_NAME=schema-registry
            - SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:9094
            - SCHEMA_REGISTRY_DEBUG=true
            - SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8082

volumes:
    kafka:

---
# 安装
$ docker-compose up -d
```
### 1.1.2 开启Binlog
MySQL8 默认已经开启了 binlog，如果没有开启，可以添加如下文件
```bash
$ vim /app/mysql8/conf/custom.cnf
[mysql]
default-character-set=utf8

[client]
default-character-set=utf8

[mysqld]
character_set_server=utf8
default-storage-engine=InnoDB
# MySQL5.7 开启 binlog 需要添加该参数
server_id=2
# 表示开启 binlog, 并指定 binlog 文件名
log-bin=mysql-bin
# 默认参数
binlog_format = ROW
# binlog 保留天数
expire_logs_days = 7
```
然后重启MySQL8
```bash
$ docker restart mysql8
```
```mysql
mysql> show variables like 'log_bin';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| log_bin       | ON    |
+---------------+-------+
1 row in set (0.00 sec)
```



# 二、示例
## 2.1 测试数据准备
进入 MySQL 容器
```mysql
$ docker exec -it mysql bash -c 'mysql -uroot -p123456'
```
在 MySQL 中执行建表语句
```mysql
drop table if exists dwd_dim_store_item;
CREATE TABLE if not exists `dwd_dim_store_item` (
  `id` int(1) primary key auto_increment COMMENT "主键",
  `tenant_id` bigint(200) NOT NULL COMMENT "商户id",
  `store_id` varchar(200) NOT NULL COMMENT "门店id",
  `shop_id` varchar(200) NOT NULL DEFAULT "-999" COMMENT "商铺ID",
  `item_id` varchar(500) NOT NULL COMMENT "商品id",
  `item_code` varchar(500) NULL COMMENT "商品编码",
  `item_name` varchar(200) NULL COMMENT "商品名称",
  `item_class` varchar(200) NULL COMMENT "",
  `category_id_level1` bigint(20) NULL COMMENT "大类id",
  `category_id_level2` bigint(20) NULL COMMENT "中类id",
  `category_id_level3` bigint(20) NULL COMMENT "小类id",
  `normal_price` double(16, 2) NULL COMMENT "正常售价",
  `price` double(16, 2) NULL COMMENT "执行价格",
  `logistics` double NULL COMMENT "送货方式",
  `flag` double NULL COMMENT "销售状态",
  `pool_goods_type` int(11) NULL COMMENT "是否联营",
  `promotion_type` varchar(200) NULL COMMENT "促销状态",
  `out_date` varchar(200) NULL COMMENT "清理日期",
  `good_date` varchar(300) NULL COMMENT "启用日期",
  `stock_type` varchar(200) NULL COMMENT "库存管理类型（0=数量金额,1=不管理库存,2=金额库存",
  `sales_flag` varchar(200) NULL COMMENT "负库存销售状态:0或空-允许销售,1-不允许销售",
  `dc_shop_id` varchar(200) NULL COMMENT "默认配送中心",
  `modify_time` datetime NULL COMMENT "最后修改时间",
  `etl_time` datetime NULL COMMENT ""
);

drop table if exists dwd_dim_item;
CREATE TABLE if not exists `dwd_dim_item` (
  `id` int primary key auto_increment COMMENT "主键",
  `tenant_id` bigint(20) NOT NULL COMMENT "商户id",
  `item_id` varchar(50) NOT NULL COMMENT "商品id",
  `item_code` varchar(50) NULL COMMENT "商品编码",
  `barcode` varchar(20) NULL COMMENT "商品条码，如无国际条码则记为[-],多条码商品仅记录默认条码",
  `item_name` varchar(200) NULL COMMENT "商品名称",
  `item_class` varchar(200) NULL COMMENT "商品级别 A/B/C/D...",
  `category_id_level1` bigint(20) NULL COMMENT "大类",
  `category_id_level2` bigint(20) NULL COMMENT "中类",
  `category_id_level3` bigint(20) NULL COMMENT "小类",
  `supplier` varchar(100) NULL COMMENT "供应商",
  `brand` varchar(100) NULL COMMENT "品牌",
  `specification` varchar(100) NULL COMMENT "规格",
  `unit_name` varchar(200) NULL COMMENT "售卖单位",
  `status` varchar(200) NULL COMMENT "状态: 0=正常, N=新品, P=暂停, S=失效",
  `fresh_type` int(11) NULL COMMENT "是否生鲜",
  `sales_flag` varchar(200) NULL COMMENT "销售状态",
  `shelf_date` date NULL COMMENT "上架日期",
  `modify_time` varchar(200) NULL COMMENT "最后修改时间",
  `enable_date` varchar(200) NULL COMMENT "启用日期",
  `out_date` varchar(200) NULL COMMENT "清理日期",
  `in_date` varchar(200) NULL COMMENT "商品引入日期",
  `etl_time` datetime NULL COMMENT ""
);
```

## 2.2 自动生成数据
```bash
# 运行造数程序
$ nohup java -jar arctic-produce.jar > arctic-produce.log 2>&1 &
# 调用接口
curl localhost:8080/kino/write
```

## 2.3 在 FlinkSql client 中创建相对应的表
```sql
FlinkSQL> DROP TABLE IF EXISTS dwd_dim_store_item;
FlinkSQL> CREATE TABLE IF NOT EXISTS dwd_dim_store_item(
    `id` bigint
  , `tenant_id` bigint
  , `store_id` STRING
  , `shop_id` STRING
  , `item_id` STRING
  , `item_code` STRING
  , `item_name` STRING
  , `item_class` STRING
  , `category_id_level1` bigint
  , `category_id_level2` bigint
  , `category_id_level3` bigint
  , `normal_price` double
  , `price` double
  , `logistics` double
  , `flag` double
  , `pool_goods_type` int
  , `promotion_type` STRING
  , `out_date` STRING
  , `good_date` STRING
  , `stock_type` STRING
  , `sales_flag` STRING
  , `dc_shop_id` STRING
  , `modify_time` STRING
  , `etl_time` STRING
  , ts AS TO_TIMESTAMP(out_date)
  , WATERMARK FOR ts as ts - INTERVAL '60' SECOND,
  primary key (`id`) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '192.168.1.80',
  'port' = '12360',
  'database-name' = 'flink',
  'table-name' = 'dwd_dim_store_item',
  'username' = 'root',
  'password' = '123456'
);

FlinkSQL> drop table if exists dwd_dim_item;
FlinkSQL> CREATE TABLE if not exists `dwd_dim_item` (
    `id` bigint
  , `tenant_id` bigint
  , `item_id` STRING
  , `item_code` STRING
  , `barcode` STRING
  , `item_name` STRING
  , `item_class` STRING
  , `category_id_level1` bigint
  , `category_id_level2` bigint
  , `category_id_level3` bigint
  , `supplier` STRING
  , `brand` STRING
  , `specification` STRING
  , `unit_name` STRING
  , `status` STRING
  , `fresh_type` int
  , `sales_flag` STRING
  , `shelf_date` STRING
  , `modify_time` STRING
  , `enable_date` STRING
  , `out_date` STRING
  , `in_date` STRING
  , `etl_time` STRING
  , ts AS TO_TIMESTAMP(out_date)
  , primary key (`id`) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '192.168.1.80',
  'port' = '12360',
  'database-name' = 'flink',
  'table-name' = 'dwd_dim_item',
  'username' = 'root',
  'password' = '123456'
);
```
## 2.4 将 left join 的结果写入 Kafka
```sql
-- 创建 kafka table
FlinkSQL> drop table if exists dwd_dim_merge;
FlinkSQL> create table if not exists dwd_dim_merge (
    `id` bigint 
  , `dim_id` bigint
  , `tenant_id` bigint
  , `store_id` STRING
  , `shop_id` STRING
  , `item_id` STRING
  , `item_code` STRING
  , `item_name` STRING
  , `item_class` STRING
  , `category_id_level1` bigint
  , `category_id_level2` bigint
  , `category_id_level3` bigint
  , `normal_price` double
  , `price` double
  , `pool_goods_type` int
  , `promotion_type` STRING
  , `l_out_date` STRING
  , `sales_flag` STRING
  , `dc_shop_id` STRING
  , `barcode` STRING
  , `r_out_date` STRING
  , `supplier` STRING
  , `brand` STRING
  , `specification` STRING
  , `unit_name` STRING
  , `status` STRING
  , `fresh_type` int
  ,PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_dim_merge',
  'properties.bootstrap.servers' = 'jz-desktop-07:30092',
  'key.format' = 'json',
  'key.json.ignore-parse-errors' = 'true',
  'value.format' = 'json',
  'value.json.fail-on-missing-field' = 'false',
  'value.fields-include' = 'EXCEPT_KEY'
);

-- insert 
FlinkSQL> insert into dwd_dim_merge
select 
    s.`id` as id
  , d.`id` as dim_id   
  , s.`tenant_id` 
  , s.`store_id` 
  , s.`shop_id` 
  , s.`item_id` 
  , s.`item_code` 
  , s.`item_name` 
  , s.`item_class` 
  , s.`category_id_level1` 
  , s.`category_id_level2` 
  , s.`category_id_level3` 
  , s.`normal_price` 
  , s.`price` 
  , s.`pool_goods_type` 
  , s.`promotion_type` 
  , s.`out_date` as l_out_date
  , s.`sales_flag` 
  , s.`dc_shop_id` 
  , d.`barcode` 
  , d.`out_date`  as r_out_date
  , d.`supplier` 
  , d.`brand` 
  , d.`specification` 
  , d.`unit_name` 
  , d.`status` 
  , d.`fresh_type` 
from dwd_dim_store_item s
left join dwd_dim_item d
on s.item_id = d.item_id and s.ts between d.ts - INTERVAL '60' SECOND and d.ts;
```
查看 Kafka 中的数据
```bash
## 进入容器中
$ docker exec -it kafka bash
## 查看数据
kakfa> ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "dwd_dim_merge" --from-beginning --property print.key=true
```

## 2.5 将 Kafka 数据写入 Arctic
在 Arctic 中创建表
```bash
drop table if exists arctic_db.arctic_kafka_dwd_dim_merge;
CREATE TABLE if not exists arctic_db.arctic_kafka_dwd_dim_merge (
    id bigint
  , dim_id bigint
  , tenant_id bigint
  , store_id STRING
  , shop_id STRING
  , item_id STRING
  , item_code STRING
  , item_name STRING
  , item_class STRING
  , category_id_level1 bigint
  , category_id_level2 bigint
  , category_id_level3 bigint
  , normal_price double
  , price double
  , pool_goods_type int
  , promotion_type STRING
  , l_out_date STRING
  , sales_flag STRING
  , dc_shop_id STRING
  , barcode STRING
  , r_out_date STRING
  , supplier STRING
  , brand STRING
  , specification STRING
  , unit_name STRING
  , status STRING
  , fresh_type int
  ,PRIMARY KEY (id)
) using arctic;
```
在 FlinkSQL client 中执行
```sql
CREATE CATALOG arctic_catalog WITH(
  'type'='arctic',
  'metastore.url'='thrift://192.168.1.80:1261/hadoop_catalog'
);
insert into arctic_catalog.arctic_db.arctic_kafka_dwd_dim_merge
select * from dwd_dim_merge;
```














