# MySQL8 窗口函数

窗口函数也叫OLAP函数（Online Anallytical Processing,联机分析处理），可以对数据进行实时分析处理。特点是：**既能分组又可以排序，且不改变行数**。

## 语法格式

```sql
SELECT 函数名(字段名) over(partition by <要分列的组> order by <要排序的列> rows between <数据范围>)

```

1.  **OVER** 为关键字，后面指定函数执行窗口的范围，包含三部分：分组PARTITION BY、排序ORDER BY 、窗口ROWS BETWEEN ...AND ...
2.  **PARTITION BY** ：根据分组字段名将数据划分为不同的组，不同的组可以理解为不同的窗口
3.  **ORDER BY：** 根据排序字段对数据进行排序，如果前面已经有PARTITION BY 字段，那ORDER BY 将在PARTITION BY分好的组内进行排序，组与组之间相互独立，互不干扰。
4.  ROWS BETWEEN ...AND ...：指定行数统计的的范围；如果其前面有PARTITION BY 字段，将在分好的组的组内指定输出行数，即在分组后的窗口再划分为一个个小窗口。

    数据范围的一些例子：
    ```sql
    rows between 2 preceding and current row # 取本行和前面两行
    rows between unbounded preceding and current row # 取本行和之前所有的行 
    rows between current row and unbounded following # 取本行和之后所有的行 
    rows between 3 preceding and 1 following # 从前面三行和下面一行，总共五行 
    # 当order by后面没有rows between时，窗口规范默认是取本行和之前所有的行
    # 当order by和rows between都没有时，窗口规范默认是分组下所有行

    ```

## 专用窗口函数

#### **rank( ) over**

条件相同排名相同，排名间断不连续 (例 1134557)

#### **dense\_rank( ) over**

&#x20;条件相同排名相同，排名间断不连续 (例 11234556)

#### **row\_number( ) over**&#x20;

条件相同排名也不相同，排名间断不连续 (例 123456)

#### 一些模版

&#x20;TOPN问题：查询每个学生成绩最高的两个科目

```sql
SELECT *
FROM (
    SELECT *, rank() over (PARTITION BY 姓名 ORDER BY 成绩 DESC) AS ranking 
    FROM test1) AS tmp
WHERE ranking<=2;
```

## 聚合窗口函数

**普通聚合函数**是将多条记录聚合为一条(**多到一**)；**聚合窗口函数**是每条记录都会执行，有几条记录执行完还是几条 **(多对多)**

聚合函数作为窗口函数是起到“累加/累计”的效果，例如“截止本行最大/最小/平均/总和是多少？”

```sql
假设有这么一张表`paytable`
+-------+--------+------------+
| Years | Months | pay_amount |
+-------+--------+------------+
|2022   |1       |8000        | 
|2022   |2       |7800        | 
|2023   |1       |6800        | 
|2023   |2       |8000        | 
+-------+--------+------------+
```

#### 累计求和：sum( ) over( )

```sql
# 当order by后面没有rows between时，窗口默认是取本行和之前所有的行
select years, 
       months,
       pay_amount,
       sum(pay_amount) over(partition by years order by months) as sum_pay
from paytable;
"""
+-------+--------+------------+------------+
| Years | Months | pay_amount |  sum_pay   |
+-------+--------+------------+------------+
|2022   |1       |8000        | 8000       |
|2022   |2       |7800        | 15800      |
|2023   |1       |6800        | 6800       | 
|2023   |2       |8000        | 14800      |
+-------+--------+------------+------------+
"""

# 当order by和rows between都没有时，窗口规范默认是分组下所有行
select years, 
       months, 
       pay_amount,
       sum(pay_amount) over(partition by years) as sum_pay
from paytable;
"""
+-------+--------+------------+------------+
| Years | Months | pay_amount |  sum_pay   |
+-------+--------+------------+------------+
|2022   |1       |8000        | 15800      |
|2022   |2       |7800        | 15800      |
|2023   |1       |6800        | 14800      | 
|2023   |2       |8000        | 14800      |
+-------+--------+------------+------------+
"""



```

#### 平均：avg( ) over( )

```sql
# 当order by后面没有rows between时，窗口默认是取本行和之前所有的行
select years,
       months,
       pay_amount,
       avg(pay_amount) over(partition by years order by months) as avg_pay
from paytable;
"""
+-------+--------+------------+------------+
| Years | Months | pay_amount |  avg_pay   |
+-------+--------+------------+------------+
|2022   |1       |8000        | 8000.0000  |
|2022   |2       |7800        | 7900.0000  |
|2023   |1       |6800        | 6800.0000  | 
|2023   |2       |8000        | 7400.0000  |
+-------+--------+------------+------------+
"""

# 当order by和rows between都没有时，窗口规范默认是分组下所有行
select years,
       months,
       pay_amount,
       avg(pay_amount) over(partition by years) as avg_pay
from paytable;
"""
+-------+--------+------------+------------+
| Years | Months | pay_amount |  avg_pay   |
+-------+--------+------------+------------+
|2022   |1       |8000        | 7900.0000  |
|2022   |2       |7800        | 7900.0000  |
|2023   |1       |6800        | 7400.0000  | 
|2023   |2       |8000        | 7400.0000  |
+-------+--------+------------+------------+
"""
```

#### **极值：max( )/min( ) over( )**

```sql
# 当order by后面没有rows between时，窗口默认是取本行和之前所有的行
select years,
       months,
       pay_amount,
       min(pay_amount) over(partition by years order by months) as min_pay
from paytable;
"""
+-------+--------+------------+------------+
| Years | Months | pay_amount |  min_pay   |
+-------+--------+------------+------------+
|2022   |1       |8000        | 8000       |
|2022   |2       |7800        | 7800       |
|2023   |1       |6800        | 6800       | 
|2023   |2       |8000        | 6800       |
+-------+--------+------------+------------+
"""


# 当order by和rows between都没有时，窗口规范默认是分组下所有行
select years,
       months,
       pay_amount,
       min(pay_amount) over(partition by years) as min_pay
from paytable;
"""
+-------+--------+------------+------------+
| Years | Months | pay_amount |  min_pay   |
+-------+--------+------------+------------+
|2022   |1       |8000        | 7800       |
|2022   |2       |7800        | 7800       |
|2023   |1       |6800        | 6800       | 
|2023   |2       |8000        | 6800       |
+-------+--------+------------+------------+
"""

```
