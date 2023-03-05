# SQL高阶主题 — 触发器、事件、事务、并发

## 触发器Triggers

触发器是在插入、更新、删除语句前后自动执行的一堆SQL代码，用来增强数据一致性。另一个用处是记录对数据库的修改（审计）。

#### 创建触发器

```sql
-- 这里触发器2个任务
--  1）跟新payments这张表的同时保持invoices这张表也更新
--  2）审计
DELIMITER $$

CREATE TRIGGER payments_after_insert --命名规范： 触发表_before/after_触发的SQL语句类型
    AFTER INSERT ON payments  --触发条件语句：BEFORE/AFTER INSERT/UPDATE/DELETE ON 触发表
    FOR EACH ROW --其它有的DBMS还支持表级别的触发器
BEGIN
    UPDATE invoices  --唯一不能修改的表是触发表，否则会引发无限循环（“触发器自燃”）
    SET payment_total = payment_total + NEW.amount --使用 NEW/OLD 关键字来指代受影响的新/旧行
    WHERE invoice_id = NEW.invoice_id;             --(INSERT用NEW，若DELETE用OLD,UPDATE两者均可)
    
    INSERT INTO payments_audit
    VALUES (NEW.client_id, NEW.date, NEW.amount, 'insert', NOW());
    -- 在payments_after_delete触发器里可以写如下语句
    --INSERT INTO payments_audit
    --VALUES (OLD.client_id, OLD.date, OLD.amount, 'delete', NOW());   
END $$

DELIMITER ;
```



#### 查看触发器

```sql
SHOW TRIGGERS; --显示所有的触发器
SHOW TRIGGERS LIKE 'payments%'; --筛选特定表的触发器

```

#### 删除触发器

```sql
DROP TRIGGER [IF EXISTS] payments_after_insert
```



## 事件Events

事件Events是一段根据计划执行的代码，可以执行一次，或者按某种规律执行

```sql
SHOW VARIABLES LIKE 'event%'; --查看是否开启
SET GLOBAL event_scheduler = ON/OFF --开启/关闭

```

#### 创建事件

```sql
DELIMITER $$

CREATE EVENT yearly_delete_stale_audit_row
-- 设定事件的执行计划：
ON SCHEDULE
    EVERY 1 YEAR [STARTS '2019-01-01' ENDS '2029-01-01']    
-- 注意这里DO BEGIN开始
DO BEGIN
    DELETE FROM payments_audit
    WHERE action_date < NOW() - INTERVAL 1 YEAR;
END$$

DELIMITER ;
```

#### 查看事件

```sql
SHOW EVENTS;
SHOW EVENTS LIKE 'yearly%'
```

#### 删除事件

```sql
DROP EVENT IF EXISTS yearly_delete_stale_audit_row
```

#### 修改事件

除了可以修改事件内容

```sql
DELIMITER $$

ALTER EVENT yearly_delete_stale_audit_row
ON SCHEDULE
    AT '2019-05-01'     
DO BEGIN
    DELETE FROM payments_audit
    WHERE action_date < NOW() - INTERVAL 1 YEAR;
END$$

DELIMITER ;
```

还可以用ALTER暂时启用或禁止一个事件

```sql
ALTER EVENT yearly_delete_stale_audit_rows ENABLE/DISABLE; 
```



## 事务Transactions

事务是完成一个完整事件的一系列SQL语句，一起成功或失败。

事务四大特性：ACID

1.  **原子性（Atomicity）**：逻辑上是不可分割的操作单元，事务的所有操作要么全部提交成功，要么全部失败回滚（用回滚日志实现，反向执行日志中的操作）；
2.  **一致性（Consis tency）**：事务的执行必须使数据库保持一致性状态。在一致性状态下，所有事务对一个数据的读取结果都是相同的；
3.  **隔离性（Isolation）**：一个事务所做的修改在最终提交以前，对其它事务是不可见的（并发执行的事务之间不能相互影响）；
4.  **持久性（Durability）**：一旦事务提交成功，对数据的修改是永久性的

#### 创建事务

```sql
START TRANSACTION;

INSERT INTO orders (customer_id, order_date, status)-- 只需明确声明并插入这三个非自增必须（不可为空）字段
VALUES (1, '2019-01-01', 1);
INSERT INTO order_items  -- 所有字段都是必须的，就不必申明了
VALUES (last_insert_id(), 1, 1, 1);

COMMIT;
```

当 MySQL 看到上面这样的事务语句组，会把所有这些更改写入数据库，如果有任何一个更改失败，会自动撤销之前的修改，这种情况被称为事务被回滚

如果想对事务进行错误检查并且手动退回事务，可以将最后的 `COMMIT;` 换成 `ROLLBACK;`



## 并发Concurrency

多个用户同时访问更新一组数据时的情况被称为 并发(concurrency)。

#### 并发问题

1.  丢失更新(Lost Updates)：后更新覆盖先更新
2.  脏读(Dirty Reads)：一个事务读取了尚未被提交的数据
3.  不可重复读取(Non-repeating Reads) ：一个事务中两次读取结果不一致 (中途被修改了)
4.  幻读(Phantom Reads) ：一些数据在查询后才被添加，更新或删除

#### 事务隔离级别

|                   | Lost Updates | Dirty Reads | Non-repeating Reads | Phantom Reads |
| ----------------- | ------------ | ----------- | ------------------- | ------------- |
| READ UNCOMMITTED  |              |             |                     |               |
| READ COMMITTED    |              | ✅           |                     |               |
| REPEATABLE READ   | ✅            | ✅           | ✅                   |               |
| SERIALIZABLE      | ✅            | ✅           | ✅                   | ✅             |



1.  读取未提交(Read Uncommitted)：事务间并没有任何隔离 &#x20;
2.  读取已提交(Read Committed )：只能读取已提交的数据，未提交的即使修改了读取的也是修改前的
3.  可重复读取(Repeatable Read )：**MySQL默认隔离级别**，确保不同的读取会返回相同的结果，即便数据在这期间被更改和提交 &#x20;
4.  序列化(Serializable) ：可以防止以上所有问题，但这很明显会给服务器增加负担，因为管理等待的事务需要消耗额外的储存和CPU资源 &#x20;

更低的隔离级别更容易并发，并且有性能会更高，但是可能会有更多的并发问题。更高的隔离等级限制了并发并减少了并发问题，但代价是性能和可扩展性的降低。

```sql
--查看事务隔离级别，默认是REPEATABLE-READ
SHOW VARIABLES LIKE 'transaction_isolation';  

--设置事务隔离级别
SET [SESSION]/[GLOBAL] TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

#### 死锁

默认情况下MySQL对事物的增删改会锁定，如果两个同时在进行的事务分别锁定了对方下一步要使用的行，就会发生死锁。死锁不能完全避免。

案例

```sql
-- instance1：先将1号顾客的州改为'VA'，再将1号订单的状态改为1
START TRANSACTION;
UPDATE customers SET state = 'VA' WHERE customer_id = 1;
UPDATE orders SET status = 1 WHERE order_id = 1;
COMMIT;

```

```sql
-- instance2：先将1号订单的状态改为1，再将1号顾客的州改为'VA'
START TRANSACTION;
UPDATE orders SET status = 1 WHERE order_id = 1;
UPDATE customers SET state = 'VA' WHERE customer_id = 1;
COMMIT;
```

减少死锁出现次数的方法

1.  更新多条记录的时候遵照相同的顺序
2.  尽量简化事务，缩短运行时间
3.  事务操作较大的表尽量避开高峰期运行，以避开大量活跃用户
