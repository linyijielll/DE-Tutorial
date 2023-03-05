# SQL基础入门 — 匹配、连接、增删改查&#x20;

## SQL单表查询运算符

#### LIKE运算符

```sql
SELECT *
FROM student
WHERE name LIKE 'chen%'
```

**'%' 表示任意字符**

```text
'chen%' : 以chen开头的词
'%yi' : 以yi结尾的词
'%jun%' : 单词中间有jun的词

```

**' \_' 表示一个字符**

```纯文本
'b____y' : 6个字符的词语，第一个字符是b，最后一个是y，中间随意
```

#### REGEXP 运算符

```sql
SELECT *
FROM student
WHERE name REGEXP 'chen'

```

用REGEXP运算符使用正则表达式来匹配文本

```纯文本
  'field' : 包含field的
  '^field' : 以field开头
  'field$' : 以field结尾
  'field|mac|rose' : 名字包含field或mac或rose
  '^field|mac|rose' : 名字以field开头或包含mac或包含rose
  '[gim]e' : 包含ge或ie或me
  'e[a-h]' ：包含e_,其中_可以是a-h中任意一个字符
```

#### ORDER BY运算符

mysql中允许 ORDER BY排序内容不是SELECT中的列

```sql
SELECT first_name,last_name 
FROM customers
ORDER BY birth_date DESC
```

#### LIMIT运算符

LIMIT子句永远要放在最后

LIMIT n 保留几条

LIMIT m,n 跳过m条，保留n条

```sql
-- 只显示前3条记录，如果记录数量比3小，则显示全部记录
SELECT *
FROM customers
LIMIT 3

-- 跳过前6个，选择3个
SELECT *
FROM customers
LIMIT 6，3

-- 取第二高的薪水
SELECT DISTINCT
    Salary AS SecondHighestSalary
FROM
    Employee
ORDER BY Salary DESC
LIMIT 1 OFFSET 1


```

## SQL连接

#### 🔥内连接Inner joins&#x20;

```sql
-- 显示 order表中的order_id 和customers表中对应购买者的 first_name, last_name
-- customer_id由于两个表都有，需要指定从哪个表中来
-- 取别名之后必须全部都用别名，不然会报错
SELECT order_id, o.customer_id, first_name, last_name
FROM orders o
JOIN customers c
    ON o.customers_id = c.customers_id 

```

#### 跨数据库查询

跨数据库存连接时前面加上数据库名称

```sql
-- 连接sql_store中的order_items表和sql_inventory中的products表
USE sql_store ;

SELECT *
FROM order_items oi
JOIN sql_inventory.products p 
    ON oi.product_id=p.product_id
```

#### 自连接

```sql
-- 同一张饿employees表,通过管理人员编号（reports_to）获取到管理人员姓名
-- 这里employees表中所有人的管理者Yovonnda自身没有管理者，所以不会出现在结果中，用LEFT JOIN可以修正这一点
USE sql_hr;

SELECT e.employees_id, e.first_name, m.first_name AS manager
FROM employees e
JOIN employees m
    ON e.reports_to = m.employee_id
```

#### 多表连接

```sql
SELECT p.date, p.invoice_id, p.amount, c.name, pm.name
FROM payments p
JOIN clients c
    ON p.client_id = c.client_id
JOIN payment_methods pm
    ON p.payment_method = pm.payment_method_id
```

#### 复合连接条件

```sql
--表order_items(order_id, product_id, quantity, unit_price)
--表order_items_notes(notes_id,order_id, product_id,notes)
SELECT *
FROM order_items oi
JOIN order_item_notes oin 
    ON oi.order_id = oin.order_id AND oi.product_id = oin.product_id

```

#### 隐式连接语法

```sql
-- 上下两个代码块作用相同
-- 外链接
SELECT *
FROM orders o
JOIN customers c
    ON o.customer_id = c.customer_id
-- 隐式连接语法 Implicit Join Syntax
SELECT *
FROM orders o, customers c
WHERE o.customer_id = c.customer_id


```

#### 🔥外连接 Outer Joins

&#x20;LEFT JOIN以FROM后面的表为主&#x20;

RIGHT JOIN以JOIN后面的表为主

```sql
-- 查询结果展示了所有客户的订单详情 不管改客户是否有订单（没有的显示null）
SELECT *
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id
    
-- 展示了正常情况下订单详情，没订单的客户没记录
--（每个订单都会对应一个客户，但不是所有客户都有订单）
SELECT *
FROM customers c
RIGHT JOIN orders o
    ON c.customer_id = o.customer_id

```

最好尽量避免使用RIGHT JOIN，在多表外连接情况下，LEFT JOIN， RIGHT JOIN混用会使代码很难读懂

#### Using子句

两个表中存在名字完全相同的列连接时可以用USING 代替 ON

```sql
SELECT o.order_id, c.first_name
FROM orders o
JOIN customers c
    --ON o.customer_id = c.customer_id
    USING (customer_id)
    

SELECT *
FROM order_items oi
JOIN order_item_notes oin 
    --ON oi.order_id = oin.order_id AND oi.product_id = oin.product_id
    USING (order_id,product_id)

```

#### 自然连接 Natural join

使用\*\*NATURAL JOIN \*\*数据库会引擎会自动选择连接方式，无法人为控制 \[不推荐使用]

```sql
SELECT o.order_id, c.first_name
FROM orders o
NATURAL JOIN customers c
```

#### 交叉连接 Cross join

使用 **CROSS JOIN** 返回笛卡尔积（两个表中各行数据两两配对）

```sql
-- 显式交叉连接
SELECT *
FROM customers c
CROSS JOIN product p
-- 隐式交叉连接
SELECT *
FROM customers c, product p

```

#### 🔥联合查询 Unions

UNION可以合并多段查询记录（包括多段来自不同表中的查询记录）

多段查询结果的列数必须相同

联合后的查询结果列名时基于第一段查询的

```sql
SELECT customer_id, first_name, points,'Bronze' AS type
FROM customers
WHERE points < 2000
UNION
SELECT customer_id, first_name, points,'Silver' AS type
FROM customers
WHERE points BETWEEN 2000 AND 3000
UNION
SELECT customer_id, first_name, points,'Gold' AS type
FROM customers
WHERE points > 3000
ORDER BY first_name
```

UNION 合并数据库内表格，会自动去重。

UNION合并数据库内表格，不去重。

## SQL增删改

#### 增 Insert

插入单行

![](<image/截屏2022-10-09 11.25.13_-SRB0tOZqr.png>)

![](<image/截屏2022-10-09 15.04.42_UixpqXWcMA.png>)

```sql
-- customer_id 被设置为AI(自动递增,Auto Increment)，如果我们不提供任何值，MYSQL会帮我们生成一个唯一值
INSERT INTO customer 
VALUES(DEFAULT,
      'John',
      'SMith',
      '1990-01-01',
      NULL,
      'address',
      'city',
      'CA',
      DEFAULT)

--另一种方式
INSERT INTO customer (
      first_name,
      last_name,
      birth_date,
      address,
      city,
      state)
VALUES(
      'John',
      'SMith',
      '1990-01-01',
      'address',
      'city',
      'CA')

```

插入多行

![](<image/截屏2022-10-09 15.12.10_YkfyggbMJb.png>)

```sql
INSERT INTO shippers (name)
VALUES ('Shipper1'),
      ('Shipper2'),
      ('Shipper3')

```

插入分层行

![](<image/截屏2022-10-09 15.26.45_5LTMOFXkxt.png>)

![](<image/截屏2022-10-09 16.20.44_z88uMzJH62.png>)

```sql
-- 先生成order
INSERT INTO orders (customer_id,order_date,status)
VALUES (1,'2019-01-02',1);
-- 用内置函数 LAST_INSERT_ID() 获取刚刚插入的order_id号
-- 利用得到的order_id生成orders_items详情
INSERT INTO orders_items
VALUES
    (LAST_INSERT_ID(), 1, 1, 2.95)
    (LAST_INSERT_ID(), 2, 1, 3.95)

```

#### 创建表复制&#x20;

```sql
-- 快速复制orders表
-- 但是复制的这张表 orders_archived 中列名的一些属性没有被设置(PK,AI)
CREATE TABLE orders_archived AS 
SELECT * FROM orders

-- 插入2019之前的订单
INSERT INTO orders_archived
SELECT *
FROM orders
WHERE order_date < '2019-01-01'
```

#### 改 Update

更新单行

```sql
UPDATE invoices
SET payment_total = 10, payment_date = '2019-03-01'
WHERE invoice_id = 1 

UPDATE invoices
SET payment_total = invoice_total*0.5, payment_date = due_date
WHERE invoice_id = 1 


```

更新多行

```sql
-- 更新所有client_id=1或2的行
-- MySQL Workbench 中需要在设置里关闭 safe update选项
UPDATE invoices
SET payment_total = invoice_total*0.5, payment_date = due_date
WHERE client_id IN (1,2) 
```

在UPDATE中运用子查询

```sql
UPDATE invoices
SET payment_total = invoice_total*0.5, payment_date = due_date
-- 这里子查询有多个结果，所以WHERE后用IN
WHERE client_id IN 
                (SELECT client_id
                FROM clients
                WHERE state IN('CA','NY'))

```

#### 删 DELETE

```sql
-- 删除所有的记录! 
DELETE FROM invoices

-- 常规删除操作
DELETE FROM invoices
WHERE client_id = 1

-- 在删除中运用子查询 （子查询只有一个结果，WHERE后用 = ）
DELETE FROM invoices
WHERE client_id = （
    SELECT client_id
    FROM clients
    WHERE name = 'Myworks'
)

```
