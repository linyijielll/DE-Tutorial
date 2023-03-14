# SQL深层理解 — 数据类型、设计、索引、保护

## 数据类型

#### 字符串类型String Types

`CHAR(x)` ：固定长度x的字符串

`VARCHAR(x)` ：可变字符串，VARCHAR 最多能储存 64KB(约 65k 个字符)，超出部分会被截断

`MEDIUMTEXT` ：最大储存16MB，适合JSON对象/中短篇书籍

`LONGTEXT` ：最大储存4GB，适合存书籍/日志

`TINYTEXT` ：最大储存 255B

`TEXT` ：最大储存 64KB，最大储存长度和 VARCHAR 一样，但最好用 VARCHAR，因为 VARCHAR 可以使用索引

#### 整数类型 Interger Types

| 整数类型      | 占用储存 | 记录的数字范围     |
| --------- | ---- | ----------- |
| TINYINT   | 1B   | \[-128,127  |
| SMALLINT  | 2B   | \[-32K,32K] |
| MEDIUMINT | 3B   | \[-8M,8M]   |
| INT       | 4B   | \[-2B,2B]   |
| BIGINT    | 8B   | \[-9Z,9Z]   |

属性1） UNSIGNED ：只储存非负数，例如UNSIGNED TINYINT也是1B，存储范围\[0,255]

属性2）ZEROFILL ：在括号中定义显示大小，例如INT(4)中1表示为0001

#### 定点数类型和浮点数类型Fixedpoint and Floatingpoint Types

`DECIMAL(p, s)` ：p最大的有效数字位数，s小数点后小数位数，此外它还有几个别名分别是DEC / NUMERIC / FIXED

`FLOAT` ：浮点数类型，占用4B

`DOUBLE` ：双精度浮点数，占用8B，显然能比前者储存更大范围的数值

定点数类型记录精确的数字。浮点数不是精确值而是近似值，所以能表示更大范围数值。

#### 布尔类型Boolean Types

MySQL中布尔类型：`BOOL` / `BOOLEAN`

```sql
UPDATE posts 
SET is_published = TRUE / FALSE
或
SET is_published = 1 / 0
```

#### 枚举和集合类型Enum and Set Types

ENUM只可以从一系列值中选取一个，SET可以从固定一系列值中取多个

添加或修改一个ENUM/SET类型，MySQL会重建整张表 —> 可以选择建 “查询表” (lookup table)来替代ENUM/SET

#### 日期和时间类型Date and Time Types

`DATE` ：日期值

`TIME` ：时间值 &#x20;

`DATETIME` ：日期+时间，8B

`TIMESTAMP`： 时间戳，4B(只能存储2038年前的日期)

`YEAR`：四位年份

#### Blob类型Blob Types

MySQL用 BLOB类型来储存大的二进制数据，包括PDF，图像，视频等等几乎所有的二进制的文件。

`TINYBLOB` ：255B

`BLOB` ：65KB

`MEDIUMBLOB` ：16MB

`LONGBLOB` ：4GB

将文件存储在关系型数据库中会有如下问题

1.  数据库大小增加
2.  备份变慢
3.  性能下降，从数据库读取文件比从文件系统读取慢
4.  需要写额外处理代码处理文件（如图片）

#### JSON类型 JSON Types

新增 {}或者JSON\_OBJECT

```sql
-- 方法1
USE sql_store;
UPDATE products
SET properties = '
{
    "dimensions": [1, 2, 3], 
    "weight": 10,
    "manufacturer": {"name": "sony"}
}
'
WHERE product_id = 1;

-- 方法2
UPDATE products
SET properties = JSON_OBJECT(
    'weight', 10,
    'dimensions', JSON_ARRAY(1, 2, 3),
    'manufacturer', JSON_OBJECT('name', 'sony')
)
WHERE product_id = 1;

```

查询 JSON\_EXTRACT或 ->/->>

```sql
-- 方法1 
SELECT product_id, JSON_EXTRACT(properties, '$.weight') AS weight 
FROM products
WHERE product_id = 1;
-- 返回10

-- 方法2 
SELECT properties -> '$.dimensions' 
FROM products
WHERE product_id = 1;
-- 返回[1, 2, 3]
SELECT properties -> '$.dimensions[0]'
FROM products
WHERE product_id = 1; 
-- 返回1
SELECT properties -> '$.manufacturer'
FROM products
WHERE product_id = 1;
-- 返回{"name": "sony"}
SELECT properties -> '$.manufacturer.name'
FROM products
WHERE product_id = 1;
-- 返回"sony"
SELECT properties ->> '$.manufacturer.name'
FROM products
WHERE product_id = 1;
-- 返回sony  (注意这里没有双引号)

-- 也可以放在where条件中
SELECT 
    product_id, 
    properties ->> '$.manufacturer.name' AS manufacturer_name
FROM products
WHERE properties ->/->> '$.manufacturer.name' = 'sony'

```

修改JSON\_SET

```sql
UPDATE products
SET properties = JSON_OBJECT(
    'weight', 10,
    -- 注意用函数的话，键值对中间是逗号而非冒号
    'dimensions', JSON_ARRAY(1, 2, 3),
    'manufacturer', JSON_OBJECT('name', 'sony')
)
WHERE product_id = 1;
```

删除JSON\_REMOVE

```sql
USE sql_store;
UPDATE products
SET properties = JSON_REMOVE(
    properties,
    '$.weight',
    '$.age'
)
WHERE product_id = 1;
```

## 数据库设计

#### 数据建模Data Modelling&#x20;

数据建模步骤：

1.  理解需求
2.  建立概念模型 → 确认业务中的 实体/事物/概念 以及它们之间的关系 （ER图、UML）
3.  建立逻辑模型 → 确认表/列/数据类型(这里的数据类型与具体数据库无关)
4.  建立实体模型

#### 主键/外键/外键约束

**主键**就是能唯一标识表中每条记录的字段

**外键**是子表里对父表主键的引用

**外键约束**

1.  CASCADE：随着主键改变而改变
2.  RESTRICT / NO ACTION：禁止更改或删除主键（除非先删子表数据）
3.  SET NULL：当主键更改或删除时，相应的外键变为空

#### 范式

**第一范式**：数据库表的每一列都是不可分割的原子数据项

**第二范式**：在1NF的基础上，非主属性完全依赖于主属性（每个表表示一个实体）

**第三范式**：在2NF的基础上，不存在传递函数依赖（一个表中的字段不应该是由表中其他字段推导而来）

**专注于避免重复性，不用刻意注意违反第几范式**

**不要对什么都建模，简洁才是终极哲学**

#### 创建和删除数据库

```sql
CREATE DATABASE IF NOT EXISTS test;
DROP DATABASE IF EXISTS test
```

#### 创建表

```sql
USE test;
CREATE TABLE IF NOT EXISTS customers
(
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50) NOT NULL, 
    points INT NOT NULL DEFAULT 0,
    email VARCHAR(255) NOT NULL UNIQUE
);
```

#### 更改表

```sql
ALTER TABLE customers
    ADD last_name VARCHAR(50) NOT NULL AFTER first_name,
    ADD city VARCHAR(50) NOT NULL,
    MODIFY first_name VARCHAR(60) DEFAULT '',
    DROP points;
```

#### 创建关系

```sql
CREATE TABLE IF NOT EXISTS orders
(
    order_id    INT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date  DATE NOT NULL,
    -- 在添加完所有列之后添加外键
    -- 外键名称命名习惯 fk_子表名_父表名
    FOREIGN KEY fk_orders_customers (customer_id) 
        REFERENCES customers (customer_id)
        ON UPDATE CASCADE
        ON DELETE NO ACTION
)
```

#### 修改关系

```sql
ALTER TABLE orders 
    DROP PRIMARY KEY,  -- 删除主键不用写列名称，会直接删除所有主键
    ADD PRIMARY KEY (order_id), 
    DROP FOREIGN KEY fk_orders_customers, 
    ADD FOREIGN KEY fk_orders_customers (customer_id)
        REFERENCES customers (customer_id)
        ON UPDATE CASCADE
        ON DELETE NO ACTION;
```

#### 字符集和排序规则

```sql
SHOW CHARSET; -- 查看MySQL支持的字符集
```

设置字符集

```sql
-- 数据库级别
 CREATE/ALTER DATABASE db_name 
    CHARACTER SET latin1

-- 表级别
CREATE/ALTER TABLE table1
    CHARACTER SET latin1
 
-- 字段级别
ALTER TABLE customers
    MODIFY first_name VARCHAR(50) CHARACTER SET latin1 NOT NULL；

```

#### 存储引擎

MySQL中有许多存储引擎，存储引擎决定了我们数据的储存方式以及可用的功能，现在一般为InnoDB

```sql
SHOW ENGINES; -- 查看MySQL支持的存储引擎
```

设置存储引擎

```sql
ALTER TABLE customers
ENGINE = InnoDB;
```

改变引擎是一个代价极高（expensive）的操作，它会重建整个表，在此期间无法方法访问数据

&#x20;

## 索引

索引的作用：

在数据量大的时候提升查询/排序的速度

索引的代价：

1.  增加数据库大小
2.  降低性能（增删改数据时都会更新对应的索引）

#### 创建索引

为customers的state字段创建索引，命名为idx\_state&#x20;

```sql
CREATE INDEX idx_state ON customers (state);

-- 一般MySQL会自动选额索引，但是也可以利用USE INDEX 强制选择
SELECT customer_id
FROM customers
USE INDEX (idx_lastname)
WHERE state = 'CA' AND last_name LIKE 'A%';

```

#### 查看索引

查看customers表的所有索引信息

```sql
SHOW INDEXES IN customers; 

```

1.  clustered index 聚合索引：每当我们为表创建主键时，MySQL 就会自动为其创建索引，每张表最多油一个聚合索引
2.  secondary index 从属索引：手动创建时会走动为其添加主键，这样就可以通过索引快速找到对应记录。MySQL会自动为外键添加从属索引，这样就能快速就行表连接了

#### 删除索引

删除索引idx\_state

```sql
DROP INDEX idx_state ON customers;
```

#### 前缀索引Prefix Indexes

为 customers 表的 last\_name 字段建立索引并且只使用其前20个字符，命名为idx\_lastname

```sql
CREATE INDEX idx_lastname ON customers (last_name(20));
```

这个字符数的设定对于 CHAR 和 VARCHAR 是可选的，但对于 TEXT 和 BLOG 是必须的

#### 全文索引Full-text Indexes

全文索引对相应列的所有字符串建立索引，它会剔除掉停用词并记录其他所有出现过的词汇以及每一个词汇出现过的一系列位置。

全文检索有两个模式：\*\*自然语言模式 \*\*和 **BOOLEAN模式**

```sql
-- 建立全文索引
CREATE FULLTEXT INDEX idx_title_body ON posts (title, body);

-- 使用 MATCH 和 AGAINST 进行搜索
-- 注意 MATCH 后必须包含全文索引建立时所有相关列，不然会报错
SELECT *
FROM posts
WHERE MATCH(title, body) AGAINST('react redux');

-- 把 MATCH() AGAINST() 包含在选择语句里， 可展示相关性得分(0-1之前，降序排列)
SELECT *, MATCH(title, body) AGAINST('react redux')
FROM posts
WHERE MATCH(title, body) AGAINST('react redux');

-- BOOLEAN模式功能1: 尽量有react，不要有redux，必须有form
SELECT *
FROM posts
WHERE MATCH(title, body) AGAINST('react -redux +form' IN BOOLEAN MODE);

-- BOOLEAN模式功能1: 精确匹配 handling a form
SELECT *
FROM posts
WHERE MATCH(title, body) AGAINST('"handling a form"' IN BOOLEAN MODE);

```

#### 复合索引Composite Indexes

组合索引原理：例如idx\_state\_points ，先对 state 建立分类排序的索引，然后再在同一state内建立 points的分类排序索引

MySQL组合索引最多可以组合16列，具体结合多少列需要根据实际的查询需求和数据量来考虑

```sql
CREATE INDEX idx_state_points ON customers (state, points);
```

组合索引字段顺序建议：

1.  将最常使用的列放在前面
2.  将基数（Cardinality）最大的列放在前面
3.  考虑查询本身

#### 索引查询优化

案例1

```sql
-- INDEX: idx_state_points, idx_points
-- 优化前：有OR需要进行全索引扫描，虽然比全表扫描快，但是还可以优化
SELECT customer_id FROM customers
WHERE state = 'CA' OR points > 1000;
--优化后
SELECT customer_id FROM customers
WHERE state = 'CA'
UNION
SELECT customer_id FROM customers
WHERE points > 1000;

```

案例2

```sql
-- INDEX : idx_points
-- 优化前：where中有列表达式，不能有效利用索引
EXPLAIN SELECT customer_id FROM customers
WHERE points + 10 > 2010;
-- 优化后
EXPLAIN SELECT customer_id FROM customers
WHERE points > 2000;

```

#### 使用索引排序

以索引 idx\_state\_points举例，以下排序是索引有效的

```sql
-- 需要从头开始并按索引顺序
ORDER BY state
ORDER BY state, points
ORDER BY points WHERE state = 'CA'
-- 需要同向
ORDER BY state DESC
ORDER BY state DESC, points DESC

```

以下是索引无效的

```sql
ORDER BY points -- points不在第一个
ORDER BY points, state  -- 顺序相反
ORDER BY state, first_name, points  --中间有别的
ORDER BY state, points DESC
ORDER BY state DESC, points

```

#### 覆盖索引

如果WHERE 子句，ORDER BY子句， SELECT 子句里用到的列被索引覆盖，整个查询就可以在只使用索引不碰原表的情况下完成，这叫作覆盖索引（covering index）

#### 维护索引

建议先查看已有索引再建立新的索引，需要删除下列三种索引

1.  重复索引：(A,B,C) , (A,B,C) 两者重复
2.  冗余索引：(A,B), (A)，(A,B)已经可以包含(A)这个索引，(A)没必要存在
3.  无用索引：用不到的

## 保护数据库

#### 创建用户

```sql
-- 创建用户john，无限制(可从任何位置访问) 
CREATE USER john  

-- 创建用户john,限制ip地址(某台电脑或者某web服务器)
CREATE USER john@127.0.0.1;  

-- 创建用户john,限制限制主机名
CREATE USER john@localhost;  

--创建用户john, 限制域名，但子域名则不行
CREATE USER john@'codewithmosh.com';  
 
--创建用户john, 限制域名，可以是该域名及其子域名下的任意计算机
CREATE USER john@'%.codewithmosh.com'; 

--创建用户john，设置密码
CREATE USER john IDENTIFIED BY '1234'; 

```

#### 查看用户

```sql
SELECT * FROM mysql.user; 
```

#### 删除用户

```sql
CREATE USER bob@codewithmosh.com IDENTIFIED BY '1234';
DROP bob@codewithmosh.com;

```

#### 更改密码

```sql
SET PASSWORD FOR john = '321'; -- 修改john的密码
SET PASSWORD = '321'; -- 修改当前用户的密码

```

#### 授予权限

```sql
--1. for web/desktop application
CREATE USER moon_app IDENTIFIED BY '1234';
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE
ON sql_store.* 
TO moon_app;

--2. for admin
GRANT ALL
ON s*.*
TO john;

```

#### 查看权限

```sql
SHOW GRANTS FOR john; --查看john的权限
SHOW GRANTS; --查看当前用户的权限

```

#### 撤销权限

```sql
REVOKE CREATE VIEW 
ON sql_store.*
FROM moon_app;
```
