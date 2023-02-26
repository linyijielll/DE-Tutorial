# SQL基础进阶 — 汇总、复杂查询、内置函数

## 汇总数据

#### 聚合函数 Aggregate Function

聚合函数有**MAX( ), MIN( ), AVG( ), SUM( ), COUNT( )**

聚合函数只对非空值操作，如果有空值会跳过

```sql
SELECT 
    MAX(invoice_total) AS highest,
    MIN(invoice_total) AS lowest,
    AVG(invoice_total) AS average,
    SUM(invoice_total) AS total,
    COUNT(*) AS total_records, 
    --COUNT(invoice_total) AS num_of_invoices, --17
    --COUNT(payment_date) AS count_of_payments, --有空值，返回15
    --CCOUNT(client_id) AS num_of_clients, --5
    --COUNT(DISTINCT client_id) AS num_of_clients --3 用 DISTINCT 来去掉重复值
FROM invoices
WHERE invoice_data > '2019-07-01'
    

```

#### GROUP BY子句

```sql
SELECT state, city, SUM(invoice_total) AS total_sales
FROM invoices 
JOIN clients USING (client_id)
GROUP BY state, city

```

#### Having 子句

**where**子句可以在**分组之前**筛选数据，where子句可以用任何列（不一定是SELECT选择的）

**having**子句可以在**分组之后**删选数据, 且having子句用到的变量必须在SELECT中

```sql
SELECT 
    client_id, 
    SUM(invoice_total) As total_sales,
    COUNT (*) AS number_of_invoices 
FROM invoices 
GROUP BY client_id
HAVING total_sales > 500 AND number_of_invoices > 5
```

#### ROLLUP运算符

ROLLUP 只能应用于聚合值的列，这个运算符仅在MySQL中才可以使用，SQL server 或oracle中无法执行

```sql
SELECT state, city, SUM(invoice_total) AS total_sales
FROM invoices 
JOIN clients USING (client_id)
GROUP BY state, city WITH ROLLUP
```

![运行结果如图](<image/截屏2022-10-11 00.57.00_cZ0O0IixSG.png> "运行结果如图")

## 复杂查询

#### 子查询

-   WHERE中的子查询

```sql
SELECT *
FROM products
WHERE unit_price >（
    SELECT unit_price 
    FROM products
    WHERE product_id=3
)

-- IN运算子查询
SELECT *
FROM products
WHERE product_id NOT IN(
    SELECT DISTINCT product_id
    FROM order_items
)

```

子查询和连接可以相互转换

```sql
-- 子查询
SELECT *
FROM clients
WHERE client_id NOT IN (
    SELECT DISTINCT client_id
    FROM invoices
)

--连接
SELECT *
FROM clients
LEFT JOIN invoices USING (client_id)
WHERE invoice_id IS NULL
```

-   SELECT 中的子查询

```sql
-- [TARGET] 返回 id , total , avg_total， 和 total-avg_total
SELECT 
    inovice_id,
    invoice_total,
    -- 直接用AVG只能出现一行,如果单独查询AVG(invoice_total)可以，但是这里有inovice_id，需要不止一行
    (SELECT AVG(invoice_total)
        FROM invoice) AS invoice_average,
    -- 表达式中不能出现别名，所以 invoice_total - invoice_average 是不正确的
    -- Solution1 : 复制上一行整个 (SELECT...FROM...)子查询
    -- Solution2 : 把发票平均值转换为一段子查询  SELECT invoice_average
    invoice_total - (SELECT invoice_average)  AS difference
FROM invoices

   
```

-   FROM中的子查询

每当在FROM子句中使用子查询，我们**必须**给子查询一个别名

FROM 子句中的子查询应该是简单的，否则最好用视图来解决这个问题

```sql
SELECT *
FROM (
    SELECT ...
    FROM ...
) AS xxx
```

#### ALL关键字 / ANY关键字

ALL

```sql
  -- [TARGET] select invoices lager than all invoices of client 3
  -- 不用ALL的做法
  SELECT *
  FROM invoices
  WHERE invoice_total > (
      SELECT MAX(invoice_total)
      FROM invoices
      WHERE client_id = 3 
  )
  --用ALL的做法
   SELECT *
   FROM invoices
   WHERE invoice_total > ALL(
      SELECT invoice_total
      FROM invoices
      WHERE client_id = 3 
  )
```

ANY

```sql
-- [TARGET] Select clients with at least two invoices
-- 不用ANY的做法
SELECT *
FROM clients
WHERE client_id IN (
    SELECT client_id
    FROM invoices
    GROUP BY client_id
    HAVING COUNT(*) >= 2
)

-- 用ANY的做法
-- '=ANY' 和 'IN'等价
SELECT *
FROM clients
WHERE client_id = ANY (
    SELECT client_id
    FROM invoices
    GROUP BY client_id
    HAVING COUNT(*) >= 2
)

```

#### 相关子查询

子查询和外查询存在相关性，例如子查询引用外查询里出现的别名, 这里子查询会在主查询每一行的层面执行(相当于一个for循环)

```sql
--[TARGET] select employees whose salary is above the average in their office 
SELECT *
FROM employees e
WHERE salary > (
    SELECT AVG(salary)
    FROM employees
    WHERE office_id = e.office_id
)

```

#### EXISTS 关键字

在下面这个例子中，如果数据量很大，IN会返回一张非常大的结果列表，会妨碍性能。EXISTS只会返回一个指示(indication)

```sql
-- [TARGET] Select clients that have an invoice

-- 没用EXIST的做法
SELECT *
FROM clients
WHERE client_id IN (
    SELECT DISTINCT client_id 
    FROM invoices
)
-- 用EXIST的做法
-- 内查询与外查询关联了 这也是一个字查询
-- 对每一个用户，都会检查是否存在一条符合这个条件的记录
SELECT *
FROM clients c
WHERE EXISTS （
    SELECT client_id
    FROM invoices
    WHERE client_id = c.client_id 
)
```

## 基本函数

#### 数值函数

&#x20;[https://dev.mysql.com/doc/refman/8.0/en/numeric-functions.html](https://dev.mysql.com/doc/refman/8.0/en/numeric-functions.html "https://dev.mysql.com/doc/refman/8.0/en/numeric-functions.html")

```sql
-- ROUND()：四舍五入
SELECT ROUND(5.73);   --6
SELECT ROUND(5.73,1); --5.7

-- CEILING(): 向上取整
SELECT CEILING(5.7); --6
SELECT CEILING(5.2); --6

-- FLOOR(): 向下取整
SELECT FLOOR(5.2); --5

-- ABS(): 绝对值
SELECT ABS(-5.2); --5.2

--RAND(): 生成0-1区间的随机浮点数
SELECT RAND(); --0.905960760923066
SELECT RAND(); --0.1754839012170992
```

#### 字符串函数

&#x20;[https://dev.mysql.com/doc/refman/8.0/en/string-functions.html](https://dev.mysql.com/doc/refman/8.0/en/string-functions.html "https://dev.mysql.com/doc/refman/8.0/en/string-functions.html")

```sql
-- LENGTH(): 字符串长度
SELECT LENGTH('sky');  --3

-- UPPER(): 大写
SELECT UPPER('sky');  --SKY

-- LOWER(): 小写
SELECT UPPER('SKY');  --sky

-- LTRIM(): 移除左侧空白字符
-- RTRIM(): 移除右侧空白字符
-- TRIM(): 删除前后的空格
SELECT LTRIM('  sky');  --sky
SELECT RTRIM('sky  ');  --sky
SELECT TRIM('  sky  ');  --sky

-- LEFT(): 最左边的几个字符
-- RIGHT(): 最右边的几个字符
-- SUBSTRING(): 任意位置的子字符串
SELECT LEFT('Kindergarten',4);     --Kind
SELECT RIGHT('Kindergarten',6);    --garten
SELECT SUBSTRING('Kindergarten',3,5);  --nderg
SELECT SUBSTRING('Kindergarten',3);    --ndergarten

-- LOCATE(): 会返回第一个字符或者一串字符匹配位置
SELECT LOCATE('n','Kindergarten');      --3
SELECT LOCATE('q','Kindergarten');      --0
SELECT LOCATE('garden','Kindergarten'); --7

-- REPLACE(): 替换
-- 具体用法 REPLACE(S，S1，S2) 使用字符串S2替代字符串S中所有的字符串S1
SELECT REPLACE('Kindergarten'，'garten','garten'); --Kindergarden

-- CONCAT(): 串联两个字符串
SELECT CONCAT(first_name,' ',last_name) AS full_name
FROM customers;
```

#### 日期函数

```sql
SELECT NOW();         -- 2022-10-18 17:27:33
SELECT CURDATE();     -- 2022-10-18
SELECT CURTIME();     -- 17:27:33

SELECT YEAR(NOW());      -- 2022
SELECT MONTH(NOW());     -- 10
SELECT DAY(NOW());       -- 18
SELECT HOUR(NOW());      -- 17
SELECT MINUTE(NOW());    -- 27
SELECT SECOND(NOW());    -- 33
SELECT DAYNAME(NOW());   -- Tuesday
SELECT MONTHNAME(NOW()); -- October

SELECT EXTRACT(DAY FROM NOW()); --18
SELECT EXTRACT(YEAR FROM NOW()); --2022

```

#### 格式化日期

```sql
SELECT DATE_FORMAT(NOW(),'%y');   --22
SELECT DATE_FORMAT(NOW(),'%Y');   --2022
SELECT DATE_FORMAT(NOW(),'%m %Y');   --11 2022
SELECT DATE_FORMAT(NOW(),'%M %Y');   --November 2022
SELECT DATE_FORMAT(NOW(),'%M %d %Y');   --November 30 2022

SELECT TIME_FORMAT(NOW(),'%H:%i %p'); --12:48 PM 
```

#### 计算时间和日期

```sql
SELECT DATE_ADD(NOW(), INTERVAL 1 YEAR);  --2023-11-30 20:52:30
SELECT DATE_SUB(NOW(), INTERVAL 1 YEAR);  --2021-11-30 20:52:30

-- DATEDIFF只返回天数差
SELECT DATEDIFF('2019-01-05','2019-01-01');               -- 4
SELECT DATEDIFF('2019-01-05 09:00','2019-01-01 17:00')    -- 4 

SELECT TIME_TO_SEC('09:00')-TIME_TO_SEC('09:02')          -- -120
```

#### IFNULL 和 COALESCE

```sql
-- 如果shipper_id为空，shipper_id返回‘Not assigned’
SELECT 
    order_id,
    IFNULL(shipper_id, 'Not assigned') AS shipper
FROM orders

--如果shipper_id为空，返回comments的值
--如果comments的值也为空，返回‘Not assigned’
SELECT 
    order_id,
    COALESCE(shipper_id, comments, 'Not assigned') AS shipper
FROM orders

```

#### IF

IF(expression, first, second) ， IF为真返回first，为假返回second

```sql
SELECT
    order_id,
    order_date,
    IF(YEAR(order_data) = YEAR(NOW()), 'Active','Archived') AS category
FROM orders
```

#### CASE

CASE { WHEN ... THEN...} END 从句判断多个情况

```sql
-- 将订单时间分类
SELECT
    order_id,
    order_date,
    CASE
        WHEN YEAR(order_date) = YEAR(NOW()) THEN 'Active'
        WHEN YEAR(order_date) = YEAR(NOW())-1 THEN 'Last Year'
        WHEN YEAR(order_date) < YEAR(NOW())-1 THEN 'Archived'
        ELSE 'Future'
    END AS category
FROM orders

-- `Person`表男女性别对调
UPDATE Person
SET sex = (
    CASE 
    WHEN sex = 'm' then 'f'
    WHEN sex = 'f' then 'm'
    END 
);


```
