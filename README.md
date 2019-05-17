# 数据监控工具

[TOC]

## 1. 开发背景

数据监控工具 data-monitor 用于监控数据库内的数据，当数据不符合用户预期时，通过邮件、百度Hi等方式向用户发出警报。

先前已存在一版监控工具：[旧版监控工具](http://wiki.baidu.com/pages/viewpage.action?pageId=208121386)，但该工具存在难以配置、难以扩展的问题，因此予以重构。

新版监控工具可覆盖旧版工具的所有需求，并着重对可配置性、可扩展性、实时性做了提升。

目前，data-monitor 支持如下两种基本的监控需求：

**1. 单查询结果集监控**

对单个 SQL 的查询结果做监控，查询结果可以是单个值或一个二维表格。

该监控类型覆盖了旧版监控工具中的如下几类需求：

- 时效性监控
- 行数监控
- 数值监控

并且更进一步，用户可以对查询的二维表格自由处理、判定，提供了更高的扩展性。

**2. 多查询结果集监控**

对多个 SQL 的查询结果做监控，可用于监控两份数据的 diff。覆盖了旧版监控工具中的同等需求，并提供了更高的扩展性。

## 2. 原理

data-monitor 运行流程如下：

- 程序启动。
- 读取配置文件。
- 依次检查、渲染所有配置项。对于每个配置，如果检查通过则生成一个 job，否则打印错误、发出警报并跳过该配置项。
- 将所有 job 加入作业队列（Priority Queue），以 job 的到期时间作为优先级。
- 启动主循环：
	- 轮询任务队列，一旦有任务到期则分发给线程池，多个任务同时到期可并行分发。
	- 轮询线程池，收集已完成的 job，根据 job 执行结果选择是否报警。
		- 如果报警，则向报警人发送百度Hi消息和邮件。
		- 报警后，如果 job 设置了重试，则根据重试时间将 job 重新放回作业队列。
	- 当作业队列为空、且线程池中无正在运行的作业时，退出循环。
- 程序结束。

## 3. 配置

data-monitor 的所有配置文件均采用对用户友好的 `.cfg` 格式（相比之下，json 格式虽然对机器友好，但不方便人工编辑）。关于 `.cfg` 格式，有以下几个简单的规则：

1. 一个配置文件中可以包含多个配置组（section），每个 section 以 `[section_name]` 为开始标志。
2. 每个配置项写作 `name = value`。value 自动被识别为字符串，**不需要用引号括起来**。
3. 注释可以使用 `#` 或 `;`。推荐使用 `;`，因为 `#` 不支持行内注释。
4. `[DEFAULT]` section 是一个特殊的 section，其中的选项是其他所有 section 中对应选项的默认值。
5. 每个 section 的内部，可以使用 `%(ref_name)s` 的方式引用选项 `ref_name` 的值。

关于 `.cfg` 格式的详细介绍，请查看 [Python ConfigParser 文档](https://docs.python.org/2/library/configparser.html)。

data-monitor 包含两个配置文件：

1. `database.cfg`：数据库配置，配置需要查询的数据库，内容相对固定，一般只需要配置一次。
2. `job.cfg`：作业配置，用户向其中添加自己的监控任务。

### 3.1 数据库配置 —— `database.cfg`

每个 section 包含一个数据库的相关配置，section 名称即为该配置组合的名称。以下是一个配置示例：

```ini
[palo_gaia_db]              ; 配置组名称，可在 job.cfg 中通过 `db_conf` 字段进行引用。
host = palo-yqa.baidu.com   ; 数据库 host 地址
port = 9030                 ; 数据库端口号
port_mini_load = 8030       ; Palo mini load 专用端口号
user = gaia_user            ; 数据库用户名
passwd = ******             ; 数据库密码
db = gaia_db                ; 默认使用的数据库名称（USE db）
charset = utf8              ; 数据库编码
```

### 3.2 作业配置 —— `job.cfg`

每个作业对应一个 section，section 名称即为作业名称。作业名称可以任取，但最好有含义，且不能与已有作业冲突。

下面的配置模板列出了一个作业中可能包含的所有配置项：

```ini
[__DOC__]
due_time =  ; 必填。该条监控的到期时间，当时钟超过该时刻后，当前监控任务将被触发。
            ; 一个 ISO 格式的日期时间字符串，可通过 BASETIME 环境变量生成（见下文）。
            ; 对于天级以上（周级、月级、年级）的监控，仅当 due_time 设定的日期刚好是当天时，才触发监控；
            ; 对于小时级监控，只需设定第一个小时的 due_time，后续监监控任务会以一小时为间隔自动生成。

db_conf =   ; 必填。数据库配置，引用 database.cfg 中的 section name。如果有多个值，使用半角逗号分隔。
database =  ; 可选。数据库连接所使用的数据库名称。如果有多个值，使用半角逗号分隔。默认为 database.cfg 中给出的值。

sql =       ; 必填。查询数据所调用的 SQL 语句，如果语句过长，可以写在一个 .sql 文件中，并在此填写文件路径。
            ; 如果有多个值，使用两个半角冒号（`::`）分隔（由于半角逗号是 SQL 语句的合法字符，因此无法用作分隔符；
            ; 另外，分号应该是最合适的分隔符，因为 .sql 文件中也使用分号分隔多个查询语句，但很遗憾分号正好是
            ; .cfg 格式的注释符号）。
            ; 多个 SQL 语句会返回多个查询结果集给校验表达式（见下文）。
            ; db_conf，database 以及 sql 如果包含多个值，那么值的数量必须相一致。

validator = ; 必填。校验表达式，一个合法的 Python 表达式，用于判定查询结果是否会触发报警。
			; 返回值为一个布尔值，如果为 `False` 说明校验失败，将触发报警，程序会根据作业配置
            ; 自动生成报警原因。如果需要更细节的报警原因，可提供第二个返回值 `info` 作为定制信息。
            ; 校验表达式的核心基础在于它可以通过钩子变量 `result` 来引用 SQL 的返回结果：
            ; 如果 SQL 的查询结果是单个值（比如查询数据行数），那么 `result` 就是该值；
            ; 否则，`result` 是一个二维表格（嵌套列表），列表中的每一行代表查询结果的一行数据，
            ; 该规范详见 PEP249: https://www.python.org/dev/peps/pep-0249/#fetchmany）。
            ; 如果有多个 SQL，那么 result 会是一个数组，其中的每个元素分别代表一个查询结果，与 SQL 一一对应。
            ;
            ; 以下高阶内容，也是高扩展性的核心所在，普通用户可不必了解：
            ; 考虑到安全性问题，校验表达式中并不能无限制地调用任意 Python 表达式，比如不应该允许
            ; 用户调用 `os.system('rm -rf /')`。因此我们对校验表达式的上下文环境进行了一定的限制，
            ; 使得用户只能调用 float, min, max, sum, map 等安全的方法。
            ; 同时该上下文环境支持自由扩展，用户可以在其中使用任意自定义函数，只需要把想调用的函数
            ; 使用 `context.register_validator` 装饰器装饰即可。`data_monitor/user/validators.py` 
            ; 文件中已经定义了一些常用的 validator 函数，可供参考。
            ; 如果你的校验逻辑比较复杂，那么推荐你定义自己的 validator 函数。

alarm_hi =  ; 必填。报警接收人的百度Hi账号，多个值以半角逗号分隔。
alarm_email=; 必填。报警接收人的百度邮箱或百度ID，多个值以半角逗号分隔。

period =    ; 可选。监控周期，可取的值有：day_and_above, hour，分别代表天级及以上监控、小时级监控。
			; 默认为 day_and_above，一般监控作业无需指定该参数。

is_active = ; 可选。是否激活该监控，可取的值为：true, false。未激活的配置会跳过。可用于禁用某些监控作业。

retry_times =    ; 如果数据校验结果失败，继续重试的次数。如果校验成功，不会触发重试。默认为 0，即不重试。
retry_interval = ; 每次重试的间隔，默认为 01:00:00，即一小时后重试。
```

一些配置项在 `[DEFAULT]` section 中给出了默认值：

```ini
[DEFAULT]
period = day_and_above
is_active = true
retry_times = 0
retry_interval = 01:00:00
```

另外 `[DEFAULT]` section 中还提供了一些全局变量，方便用户在自己的配置中进行引用：

```ini
[DEFAULT]
; 注意，由于 % 是 .cfg 文件的特殊符号，所以需要转义，使用 %% 来代表一个百分号
TODAY = {BASETIME | dt_format('%%Y%%m%%d')}
YESTERDAY = {BASETIME | dt_add(days=-1) | dt_format('%%Y%%m%%d')}
TODAY_ISO = {BASETIME | dt_format('%%Y-%%m-%%d')}
YESTERDAY_ISO = {BASETIME | dt_add(days=-1) | dt_format('%%Y-%%m-%%d')}
```

下面是一个简单的配置示例：

```ini
[demo_simple_value]
; 简单的单值监控
due_time = {BASETIME | dt_set(hour=9, mimute=30)}	; 触发时间为 BASETIME 当天 09:30
db_conf = palo_muse
sql =
    SELECT count(1)
    FROM pmc_all_channel_advertising
    WHERE event_day = '%(YESTERDAY)s'
validator = result > 40								; 要求查询结果大于 40，否则发出警报
alarm_hi = zhuhe02_02
alarm_email = zhuhe02
```

其中，`db_conf`、`alarm_hi`、`alarm_email` 的含义显而易见，其余几条配置需要额外说明一下：

- `due_time`：
	+ 花括号代表该块内容需要动态渲染（这是一种常见的模板语法，一般使用 Python 的 `str.format` 函数就可以做渲染，但此处需要支持管道操作，因此采用了更高级的 jinja2 包做渲染）。
	+ `BASETIME` 是程序传递给配置文件的环境变量，是一个日期时间类型（类似 2019-05-14 00:00:00），目前采用的值为“监控程序启动当天的零点整”。
	+ `| dt_set(hour=9, mimute=30)` 是一个管道操作（在 jinja2 中称为过滤），其作用是把管道符之前的值（`BASETIME`）通过函数处理一下，得到一个新的值。其中，`dt_set`（set datetime）是一个过滤器，用于计算绝对日期时间。整个表达式 `BASETIME | dt_set(hour=9, mimute=30)` 的含义就是把 `BASETIME` 的小时数设为 `9`，分钟数设为 `30`，得到一个新的时间，即 `BASETIME` 当天的 09:30。
	+ 过滤器函数可由用户自由定制，目前已实现的过滤器包括 `dt_set`、`dt_add`、`dt_format` 都是自定义的（分别用于生成绝对时间、相对时间、格式化时间字符串），你可以在 `data_monitor/user/filters.py` 中查看它们的定义。如果这些过滤器不能满足你的需求，欢迎定义自己的过滤器。

- `sql`：
	+ 前半部分容易理解，就是一个普通的 SQL 查询语句。
	+ `%(YESTERDAY)s` 是一个配置引用，这是 `.cfg` 格式支持的一种语法，用于引用已存在的别的配置项的值。被引用的 `YESTERDAY` 已经定义在 `[DEFAULT]` section 中。

- `validator`：
	+ `result > 40` 是一个合法的 Python 表达式，如果 `result` 大于 `40`，将返回 `True`，否则返回 `False`。

当校验失败时，将发出类似下面的警报：

```
job: demo_simple_value
due time: 2019-05-14 09:00:00
====================
reason: validator not pass
--------------------
validator is: `result > 40`
with `result` as: `38L`
```

## 4. 使用

首先进入 `data-monitor` 主目录，程序的入口为 `data-monitor/main.py`。

对于上一节的配置示例，我们可以通过如下命令发起该监控作业：

```sh
python main.py --job demo_simple_value
# 或者
# python main.py -j demo_simple_value
```

如果你有多个监控作业需要发起，可多次使用 `--job` 选项，例如：

```sh
python main.py -j demo_simple_value -j another_job
```

也可以不指定 `--job` 选项，这样将会发起配置文件中所有激活的作业：

```sh
python main.py
```

还可以指定别的的配置文件：

```sh
python main.py --config-file /path/to/job_config_file.cfg
# 或者
# python main.py -c /path/to/job_config_file.cfg
```

与 `--job` 选项类似，`--config-file` 选项也至此多次叠加使用，程序会自动合并多个配置文件。

更详细的用法见命令帮助：

```sh
python main.py --help
```

```
usage: main.py [-h] [-c JOB_CONFIG_FILES] [--db-config-file DB_CONFIG_FILE]
               [-j JOB_NAMES] [--force]

data-monitor: monitor databases and alarm when data is not as expected

optional arguments:
  -h, --help            show this help message and exit
  -c JOB_CONFIG_FILES, --config-file JOB_CONFIG_FILES
                        path of job config file, if not provided, use
                        `job.cfg` under current path. you can provide multiple
                        config files by repeating `-c` option, conflicted job
                        names will be auto-detected.
  --db-config-file DB_CONFIG_FILE
                        path of database config file, if not provided, use
                        `database.cfg` under current path.
  -j JOB_NAMES, --job JOB_NAMES
                        job name (section name in your job config file). you
                        can launch multiple jobs by repeating `-j` option.
  --force               force to run job(s) immediately, do not wait until due
                        time of job.
```

程序开始执行后，会在控制台中打印详细的执行日志，覆盖作业调度、是否报警、异常等各种信息。以下为某次启动 data-monitor 之后的执行日志：

```
[2019-05-14 18:28:36,295] data_monitor INFO: checking job configs ...
[2019-05-14 18:28:36,328] data_monitor INFO: job [demo_two_table_diff] config OK.
[2019-05-14 18:28:36,338] data_monitor INFO: job [demo_simple_value_with_sql_in_file] config OK.
[2019-05-14 18:28:36,355] data_monitor INFO: job [demo_simple_diff] config OK.
[2019-05-14 18:28:36,367] data_monitor INFO: job [demo_simple_value] config OK.
[2019-05-14 18:28:36,367] data_monitor INFO: all job configs OK.
[2019-05-14 18:28:36,367] data_monitor INFO: monitor start ...
[2019-05-14 18:28:36,367] data_monitor INFO: ============================================================
[2019-05-14 18:28:36,367] data_monitor INFO: ****** total jobs: 4 ...
[2019-05-14 18:28:36,368] data_monitor INFO: ****** pending: 4, running: 0, completed: 0 ******
[2019-05-14 18:28:36,372] data_monitor INFO: job [demo_simple_value] is due. launched.
[2019-05-14 18:28:36,373] data_monitor INFO: ****** pending: 3, running: 1, completed: 0 ******
[2019-05-14 18:28:36,374] data_monitor INFO: job [demo_simple_value_with_sql_in_file] is due. launched.
[2019-05-14 18:28:36,375] data_monitor INFO: ****** pending: 2, running: 2, completed: 0 ******
[2019-05-14 18:28:36,376] data_monitor INFO: job [demo_two_table_diff] is due. launched.
[2019-05-14 18:28:36,377] data_monitor INFO: ****** pending: 1, running: 3, completed: 0 ******
[2019-05-14 18:28:36,379] data_monitor INFO: job [demo_simple_diff] is due. launched.
[2019-05-14 18:28:36,493] data_monitor INFO: job [demo_simple_value] returned. status: =====> ALARM <=====
	job: demo_simple_value
	due time: 2019-05-14 09:00:00
	====================
	reason: validator not pass
	--------------------
	validator is: `result > 50`
	with `result` as: `48L`
[2019-05-14 18:28:36,638] data_monitor.alarm INFO: succeeded sending BaiduHi message to user "zhuhe02_02"
[2019-05-14 18:28:36,692] data_monitor INFO: job [demo_simple_value_with_sql_in_file] returned. status: OK.
[2019-05-14 18:28:36,692] data_monitor INFO: job [demo_simple_diff] returned. status: OK.
[2019-05-14 18:28:36,692] data_monitor INFO: job [demo_two_table_diff] returned. status: OK.
[2019-05-14 18:28:36,692] data_monitor INFO: ****** pending: 0, running: 0, completed: 4 ******
[2019-05-14 18:28:36,693] data_monitor INFO: all jobs (4) finished.
[2019-05-14 18:28:36,693] data_monitor INFO: ============================================================
[2019-05-14 18:28:36,693] data_monitor INFO: monitor exit.
```

## 5. 更多配置示例

### 单值监控

```ini
[demo_single_value]
due_time = {BASETIME | dt_set(hour=9)}
db_conf = palo_muse
sql =
    SELECT count(1)
    FROM pmc_all_channel_advertising
    WHERE event_day = '%(YESTERDAY)s'
validator = result > 50
alarm_hi = zhuhe02_02
alarm_email = zhuhe02
```

如果校验失败，将发出类似下面的警报：

```
job: demo_single_value
due time: 2019-05-17 09:00:00
====================
reason: validator not pass
--------------------
validator is: `result > 50`
with `result` as: `47L`
```

### 单表监控

```ini
[demo_single_table]
due_time = {BASETIME | dt_set(hour=9)}
db_conf = palo_muse
sql =
    SELECT event_day, count(1)
    FROM pmc_all_channel_advertising
    WHERE event_day >= '{BASETIME | dt_add(months=-1)}'
    GROUP BY event_day
    ORDER BY event_day
validator = claim(result, gt(50))
alarm_hi = zhuhe02_02
alarm_email = zhuhe02
```

该示例的 `validator` 中使用了自定义校验函数 `claim` 和 `gt`，这些函数定义在 `data_monitor/user/validators.py` 中。其中：

- `claim` 函数用于断言一个 SQL 查询结果集。接收两个参数，参数一为查询结果集，参数二是一个“谓词函数”，接收一个单值并返回一个布尔值。
- `gt(50)` 是一个谓词函数，用于判定一个值是否“大于50”。类似的谓词还有 `ge`、`lt`、`le`、`eq`、`ne`，分别用于判定大于等于、小于、小于等于、等于、不等于。
- 整个 validator 表达式的含义就是：判断查询结果集的 value 列（最后一列）的数据是否都大于50，如果有任意一个不大于50，则触发警报。警报中会给出所有不大于50的行。

如果校验失败，将发出类似下面的警报：

```
job: demo_single_table
due time: 2019-05-17 09:00:00
====================
reason: claim failed for some records
validator is: `claim(result, gt(50))`
--------------------
     event_day  col1
0   2019-04-23    49
1   2019-04-24    49
2   2019-04-25    48
3   2019-04-26    49
4   2019-04-27    45
..         ...   ...
19  2019-05-12    49
20  2019-05-13    48
21  2019-05-14    47
22  2019-05-15    39
23  2019-05-16    47
```

#### 谓词函数的组合

谓词函数可以通过 `ands`（且）、`ors`（或）两个高阶谓词函数进行自由组合（包括嵌套组合），这两个函数同样定义在 `data_monitor/user/validators.py` 中。使用示例如下：

```ini
; 大于 50 且小于 60 且不等于 55
validator = claim(result, ands(gt(50), lt(60), ne(55)))

; 大于 50 且小于 60 且不等于 55，或等于 0
validator = claim(result, ors(ands(gt(50), lt(60), ne(55)), eq(0)))
```

### 单值 diff

```ini
[demo_diff_value]
due_time = {BASETIME | dt_set(hour=9)}
db_conf = palo_muse, palo_muse_new
_sql =
    SELECT count(1)
    FROM pmc_all_channel_advertising
    WHERE event_day = '%(YESTERDAY)s'
sql = %(_sql)s :: %(_sql)s
validator = abs(result[0] - result[1]) < 1
alarm_hi = zhuhe02_02
alarm_email = zhuhe02
```

如果校验失败，将发出类似下面的警报：

```
job: demo_simple_diff
due time: 2019-05-14 09:00:00
====================
reason: validator not pass
--------------------
validator is: `abs(result[0] - result[1]) < 1`
with `result` as: `[47L, 48L]`
```

### 两表 diff

```ini
[demo_diff_table]
due_time = {BASETIME | dt_set(hour=9)}
db_conf = palo_muse, palo_muse_new
sql =
    SELECT event_day, product, partner, sum(click) AS num
    FROM pmc_all_channel_advertising
    WHERE event_day = '%(YESTERDAY)s'
    GROUP BY event_day, product, partner
    ::
    SELECT event_day, product, partner, sum(click) AS num
    FROM pmc_all_channel_advertising
    WHERE event_day = '%(YESTERDAY)s'
    GROUP BY event_day, product, partner
validator = diff(result[0], result[1], threshold=1)
alarm_hi = zhuhe02_02
alarm_email = zhuhe02
```

该示例的 `validator` 中使用了自定义校验函数 `diff`，该函数定义在 `data_monitor/user/validators.py` 中。

其中，`diff(result[0], result[1], threshold=1)` 的含义是对 `result[0]` 和 `result[1]` 做 diff，如果 diff 的绝对值超过 `threshold`，则发出警报。警报信息中会给出所有不满足条件的行，示例如下：

```
job: demo_two_table_diff
due time: 2019-05-14 09:00:00
====================
reason: find diff
validator is: `diff(result[0], result[1], threshold=1)`
--------------------
             product        partner    num_1   num_2    diff
0             haokan  guangdiantong  1134984  518837  616147
1   baiduboxapp_lite    yingyongbao        0   25357   25357
2            quanmin         xiaomi        0    3560    3560
3            quanmin    yingyongbao        0   11321   11321
4             haokan         xiaomi   847320  449064  398256
5             haokan           oppo   177334  189478   12144
6   baiduboxapp_lite           oppo        0   46637   46637
7        baiduboxapp         xiaomi        0   42740   42740
8            quanmin           oppo        0      66      66
9             haokan           vivo   502760  199748  303012
10  baiduboxapp_lite         xiaomi    42705   50605    7900
11       baiduboxapp           oppo        0   77691   77691
12            haokan          meizu    76183   38706   37477
13            haokan         liebao    15763       0   15763
14       baiduboxapp    yingyongbao        0   68564   68564
15            haokan    yingyongbao        0   67475   67475
16       baiduboxapp          meizu    14744   12701    2043
```

`diff` 函数还可以接受一个额外的参数 `direction` 用于指定 diff 的方向，其取值为 `-1`、`0`、`1`，分别代表左表减右表、两表相减取绝对值、右表减左表，默认值为 `0`。

### 小时级数据监控

小时级任务比起其他任务有些特殊，主要体现在以下几个方面：

- 需要在配置中明确指定 `period = hour`。
- 程序会在配置加载完成后，将每个小时级任务复制成 24 份，它们的 `due_time` 分别为初始 `due_time` 加上 0~23 小时，名称为原始名称加上小时后缀，以便报警时区分。
- 小时级任务除了 `BASETIME` 以外，还有一个特有的环境变量 `DUETIME`，表示作业被调起的时间。这样用户的 sql 就可以关联到作业的调起时间，比如“每个小时检查 3 小时之前的数据”，这一功能是无法通过 `BASETIME` 变量实现的。
