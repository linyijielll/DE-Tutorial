# SQL提高效率 — 视图、存储过程、函数

## 视图

可以把查询保存为视图，以供在其他地方使用。实图不存储数据，数据存储在表中。

优点

1.  简化查询
2.  减小数据库设计改动的影响（抽象化）
3.  限制对基础数据访问（数据安全性）

#### 创建视图

```sql
-- 不会返回查询结果，而是创健了一个视图对象
CREATE VIEW sales_by_client AS
SELECT c.client_id,c.name,SUM(invoice_total) As total_sales 
FROM clients c
JOIN invoices i USING (client_id) 
GROUP BY client id, name;

```

#### 删除视图

```sql
DROP VIEW sales_by_client；
```

#### 更改视图

```sql
CREATE OR REPLACE VIEW sales_by_client AS
SELECT c.client_id,c.name,SUM(invoice_total) As total_sales 
FROM clients c
JOIN invoices i USING (client_id) 
GROUP BY client id, name
ORDER BY total_sales DESC;

```

#### 可更新视图Updatable Views

&#x20;如果一个视图没有DISTINCT，Aggregate Functions，GROUP BY / HAVING，UNION 则这个视图是一个可更新视图。

可以在可更新视图上进行数据更新(INSERT、UPDATE、DELETE), 只有当视图有所有基础表中要用到的列, 插入操作才会生效。

在MySQL8上实践: 在视图中更新数据，原表中的数据也会修改

```sql
-- 下面这个是一个可更新视图
CREATE OR REPLACE VIEW invoices_with_balance AS 
SELECT 
    invlice_id,
    number,
    client_id,
    invoice_total,
    payment_total,
    invoice_total - payment_total As balance,
    invoice_date,
    due_date,
    payment_date
FROM invoices
WHERE invoice_total - payment_total >0;

-- 可以对invoices_with_balance进行数据更新
DELETE FROM invoices_with_balance
WHERE invoice_id = 1;

UPDATE invoices_with_balance
SET due_date = DATE_ADD(due_date,INTERVAL 2 DAY)
WHERE invoice_id = 2;

```

#### WITH OPTION CHECK子句

在创建视图的末尾加上WITH OPTION CHECK子句，防止UPDATE或DELETE语句将行从视图中删除。

```sql
-- 加上 WITH OPTION CHECK
-- 如果一些修改操作导致行从视图中被删除，会有报错提示
CREATE OR REPLACE VIEW invoices_with_balance AS 
SELECT 
    invlice_id,
    number,
    client_id,
    invoice_total,
    payment_total,
    invoice_total - payment_total As balance,
    invoice_date,
    due_date,
    payment_date
FROM invoices
WHERE invoice_total - payment_total >0
WITH CHECK OPTION;

```

## 存储过程 Stored Procedures

1.  存储管理SQL脚本
2.  更快的执行速度
3.  加强数据安全性

#### 创建存储过程

```sql
-- 要把整个创建存储过程语句打包成一个整体，但是中间有 ; 符号
-- 用DELIMITER把默认分隔符号改变成别的
-- MySQL需要这样操作，SQL Server这些不需要
DELIMITER $$  #把分隔符变为$$
CREATE PROCEDURE get_clients()
BEGIN
    SELECT * FROM clients;
END $$
DELIMITER ;   #把分隔符变回;
```

#### 调用存储过程

```sql
CALL get_clients()
```

#### 删除存储过程

```sql
DROP PROCEDURE IF EXISTS get_clients;
```

#### 存储过程参数

正常参数

```sql
--创建带参数的存储过程
DROP PROCEDURE IF EXISTS get_clients_by_state;

DELIMITER $$  
CREATE PROCEDURE get_clients_by_state
(
    state CHAR(2)
)
BEGIN
    SELECT * FROM clients c
    WHERE c.state = state;
END $$
DELIMITER ;

-- 调用
CALL get_clients_by_state('CA');  
```

带默认值的参数

```sql
--有参数返回对应州的clients，没有参数则返回所有的clients
DROP PROCEDURE IF EXISTS get_clients_by_state;

DELIMITER $$  
CREATE PROCEDURE get_clients_by_state
(
    state CHAR(2)
)
BEGIN
    SELECT * FROM clients c
    WHERE c.state = IFNULL(state,c.state);
END $$
DELIMITER ;

-- 调用
CALL get_clients_by_state(NULL);  
```

#### 参数验证

SQLSTATE列表参考：[https://www.ibm.com/docs/en/i/7.5?topic=codes-listing-sqlstate-values](https://www.ibm.com/docs/en/i/7.5?topic=codes-listing-sqlstate-values "https://www.ibm.com/docs/en/i/7.5?topic=codes-listing-sqlstate-values")

```sql
-- 验证参数是否合理
-- sql errorcode上IBM网站查询
DROP PROCEDURE IF EXISTS make_payment;

DELIMITER $$ 
CREATE PROCEDURE make_payment 
(
    invoice_id INT,
    payment_amount DECIMAL(9,2), 
    payment_date DATE
)
BEGIN
    IF payment_amount <= 0 THEN
        SIGNAL SQLSTATE '22003' SET MESSAGE_TEXT 'Invalid payment amount';
    END IF;
    UPDATE invoices i
    SET
        i.payment_total = payment_amount
        i.payment_date = payment_date
    WHERE i.invoice_id = invoice_id
END $$
DELIMITER ;

CALL make_payment(2,100,2019-01-01')

```

#### 输出参数

```sql
-- tips: select 后面加 into，select的结果就不会输出
DROP PROCEDURE IF EXISTS get_unpaid_invoices_for_clients;

DELIMITER $$
CREATE PROCEDURE get_unpaid_invoices_for_clients(
    client_id INT，
    OUT invoices_count INT, --OUT关键字
    OUT invoices_total decimal(9,2),
)
BEGIN
    SELECT count(*),sum(invoice_total) 
    INTO invoices_count, invoices_total --into 读取数据复制到参数输出
    FROM invoices i
    WHERE i.client_id = client_id AND payment_total = 0;
END $$
DELIMITER ;

 -- 调用用户/会话变量
 set @invoices_count = 0;  --用户定义变量需要加前缀@
 set @invoices_total = 0;
 call sql_invocing.get_unpaid_invoices_for_clients(3,@invoices_count,@invoices_total);
 select @invoices_count，@invoices_total;
```

#### 变量

```sql
-- User or Session variables 用户变量
-- 保存直到断开MySQL连接
SET @invoices_count = 0;

-- Local variables 本地变量
-- 在存储过程或函数中定义，一旦存储过程执行完就清空
DECLARE invoices_count int

```

```sql
CREATE PROCEDURE get_risk_factor() 
BEGIN
    -- 假定 risk_factor = invoices_total/invoices_count *5
    DECLARE risk_factor decimal(9,2) default 0; --声明本地变量
    DECLARE invoices_total decimal(9,2);
    DECLARE invoices_count int;

    SELECT count(*), sum(invoice_total)
    INTO invoice_count, invoice_total
    FROM invoices;

    SET risk_factor = invoice_total / invoice_count *5;

    SELECT risk_factor
END $$
DELIMITER ;
```

## 函数

与存储过程的主要区别是函数只能返回单一值，无法返回拥有多行和多列的结果集。

#### 创建函数

每个MySQL Function至少要有一个属性

**deterministic**  确定性：传递相同的参数，返回相同的值

**reads sql data**  函数中会配置选择语句，用以读取一些数据

**modifies sql data**  函数中有插入/更新/删除语句

```sql
DROP function IF EXISTS get_risk_factor_for_client;

DELIMITER $$ 
CREATE FUNCTION get_risk_factor_for_client
(
    client_id INT
)
RETURNS INTEGER -- 函数返回值的类型(函数和存储过程的主要区别)
READS SQL DATA
BEGIN
    -- 假定 risk_factor = invoices_total/invoices_count *5
    DECLARE risk_factor decimal(9,2) default 0; --声明本地变量
    DECLARE invoices_total decimal(9,2);
    DECLARE invoices_count int;

    SELECT count(*), sum(invoice_total)
    INTO invoice_count, invoice_total
    FROM invoices i
    WHERE i.client_id = client_id

    SET risk_factor = invoice_total / invoice_count *5;
    
    RETURN IFNULL(risk_factor,0);
END$$
DELIMITER ;

```

#### 调用函数

```sql
SELECT
    client_id,
    name,
    get_risk_factor_for_client(client_id) as risk_factor
FROM clients
```

#### 删除函数

```sql
DROP function IF EXISTS get_risk_factor_for_client;
```
