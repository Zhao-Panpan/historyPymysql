[TOC]

# HistoryPyMySQL

这是一个基于`PyMySQL`实现自动生成相应历史拉链表记录的`API`

使用方式和`PyMySQL`或者`aiomysql`一致

## 依赖

- `PyMySQL` 、 `aiomysql`
- `sqlparse`

## 用途

用户无感知的情况下记录表的增删改操作
可以方便快速回滚数据表到历史任意时刻，仅通过一条查询语句

## 使用方式

- 操作数据表和`PyMySQL`、`aiomysql`一致
- 每个表需要对应一个历史拉链表，默认名字为主表名+`_history`，后缀名可以在建立连接时是指定
- 建立连接时可通过参数`operate_history`指定是否写拉链表; 通过`postfix`指定拉链表的后缀名
- 连接光标cursor时可通过参数`operate_user`指定拉链表操作人
- 执行`execute`或者`executemany`时同样可以通过`operate_history`来指定是否写拉链表; 通过`operate_user`来指定是谁操作历史拉链表
- 传参权重说明: 方法`execute`或者`executemany` > 建立连接和创建cursor
- 历史拉链表比主表多三个字段（`record_begin_time`, `record_end_time`, `record_operate_user`）其他字段一致
- 每次主表增删改时历史拉链表会同步操作
- 想要恢复到历史某一时刻可查看下面例子

## 例子


```mysql
CREATE TABLE `tag_1` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键自增id',
  `name` VARCHAR(200) NOT NULL DEFAULT '' COMMENT '设备名字',
  `created_time` DATETIME NOT NULL DEFAULT current_timestamp COMMENT '创建时间',
  `modified_time` DATETIME NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='设备标签映射';

CREATE TABLE `tag_1_history` (
  -- 历史拉链表固定字段
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键自增id',
  `record_begin_time` DATETIME(3) NOT NULL COMMENT '历史记录开始时间',
  `record_end_time` DATETIME(3) NOT NULL COMMENT '历史记录终止时间',
  `record_operate_user` VARCHAR(255) NOT NULL DEFAULT '' COMMENT '历史记录操作人',
  `created_time` DATETIME NOT NULL DEFAULT current_timestamp COMMENT '创建时间',
  `modified_time` DATETIME NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新时间',
  -- 关联表的原表字段
  `base_id` INT UNSIGNED NOT NULL COMMENT '主表id',
  `name` VARCHAR(200) NOT NULL DEFAULT '' COMMENT '设备名字',
  `base_created_time` DATETIME NOT NULL DEFAULT current_timestamp COMMENT '创建时间',
  `base_modified_time` DATETIME NOT NULL DEFAULT current_timestamp COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='设备标签映射历史拉链表';


```


## pymysql版
```python
from pymysql_connection import Connection

BASE_COLUMN = ['id', 'created_time', 'modified_time']

conn = Connection(host='host1', port=3306, user='user_name', password='pwd', db='database_name', base_column=BASE_COLUMN)
sql1 = "insert into tag_1(name) value ('aaa')"
with conn.cursor() as cursor:
    cursor.execute(sql1, history_operate=True, operate_user="xxx")
    cursor.commit()
```


## aiomysql版
```python
import asyncio
from aiomysql_connection import connect, DictCursor

loop = asyncio.get_event_loop()

BASE_COLUMN = ['id', 'created_time', 'modified_time']

@asyncio.coroutine
def test_example():
    conn = yield from connect(host='xxx', port=3306, user='user_nmae', password='password', db='db',
                              loop=loop, base_column=BASE_COLUMN)
    cur = yield from conn.cursor(DictCursor)
    yield from cur.execute("insert into tag_1(name) value ('dddd')", history_operate=True)
    yield from conn.commit()
    yield from cur.close()
    conn.close()


if __name__ == '__main__':
    loop.run_until_complete(test_example())
```


执行后`history`表会同步插入一条记录

查询历史某一时刻(2019-09-20)数据：

```mysql
select * from tag_1_history 
where record_begin_time < '2019-09-20' and record_end_time > '2019-09-20'
```

用法示例：

```

sql = """select t.name from net_tag t where t.name =  %s"""
history_time = '2019-05-27'

cursor.execute_history(sql, args=["tome"], history_time=history_time)

```

## 注意

- 不支持分布式


#### 版本变更记录

  
- v1.0.5(20201208):
  1. 修复批量操作时（非简单批量插入操作），获取pairs值异常

- v1.0.4(20201021):
  1. 批量插入时lastrowid的处理(返回拆分后每条insert语句的lastrowid和rowcount)
 
- v1.0.3(20200925):
  1. 增加历史数据的回滚操作
  2. 增加某条数据的历史变更过程的查看

- v1.0.2(20200819):
  1. pymysql增加后续开启历史拉链表时补充历史数据方法 
  2. 修复aiomysql批量insert操作时的异常，并增加补充历史数据方法 

- v1.0.1(20200722):
  1. 修改批量添加大量数据时获取lastrowid不准确的异常
  2. 关闭warning提示 
