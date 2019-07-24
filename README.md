# 数据监控工具

## 1. 开发背景

数据监控工具 data-monitor 用于监控数据库内的数据，当数据不符合用户预期时，通过短信、邮件、即时通信工具等方式向用户发出警报（目前仅实现了邮件警报）。

data-monitor 支持如下两种基本的监控需求：

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
		- 如果报警，则向报警人发送邮件或其他消息。
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
[mysql_test_db]             ; 配置组名称，可在 job.cfg 中通过 `db_conf` 字段进行引用。
host = localhost            ; 数据库 host 地址
port = 3306                 ; 数据库端口号
user = username             ; 数据库用户名
passwd = ******             ; 数据库密码
database = test_db          ; 默认使用的数据库名称（USE db）
charset = utf8              ; 数据库编码
```

### 3.2 作业配置 —— `job.cfg`

每个作业对应一个 section，section 名称即为作业名称。作业名称可以任取，但最好有含义，且不能与已有作业冲突。

下面的配置模板列出了一个作业中可能包含的所有配置项：

```ini
[__DOC__]
desc =      ; 必填。一句简要的作业描述，可以使用中文，将会出现在报警信息中。
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

alarm_email=; 必填。报警接收人的邮箱，多个值以半角逗号分隔。

period =    ; 可选。所监控数据的产出周期，可取的值有：year, month, week, day, hour。
            ; 默认为 day，一般监控作业无需指定该参数。

is_active = ; 可选。是否激活该监控，可取的值为：true, false。未激活的配置会跳过。可用于禁用某些监控作业。

retry_times =    ; 如果数据校验结果失败，继续重试的次数。如果校验成功，不会触发重试。默认为 0，即不重试。
retry_interval = ; 每次重试的间隔，默认为 01:00:00，即一小时后重试。
```

一些配置项在 `[DEFAULT]` section 中给出了默认值：

```ini
[DEFAULT]
period = day
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

配置文件中可以引用下列**环境变量**：

- `BASETIME`：基准时间，datetime 类型，其值为监控程序启动当天的零点整。例如 2019-05-14 启动监控程序，则 `BASETIME = '2019-05-14 00:00:00'`。
- `DUETIME`：作业发起时间，datetime 类型，其值为作业配置项中的 `due_time`。

下面是一个简单的配置示例：

```ini
[demo_single_value]
; 单值监控
desc = 演示-单值监控
due_time = {BASETIME | dt_set(hour=9, mimute=30)}	; 触发时间为 BASETIME 当天 09:30
db_conf = mysql_test_db
sql =
    SELECT count(1)
    FROM table1
    WHERE event_day = '%(YESTERDAY)s'
validator = result > 60								; 要求查询结果大于 60，否则发出警报
alarm_email = zhuhe212
```

其中，`db_conf`、`alarm_email` 的含义显而易见，其余几条配置需要额外说明一下：

- `due_time`：
	+ 花括号代表该内容块需要动态渲染（针对简单的模板渲染，一般可采用 Python 的 `str.format` 函数，但此处需要支持管道过滤器操作，因此采用了更高级的 [jinja2](http://docs.jinkan.org/docs/jinja2/) 包做渲染）。
	+ `| dt_set(hour=9, mimute=30)` 是一个管道操作（在 jinja2 中称为过滤），其作用是把管道符之前的值（`BASETIME`）通过函数处理一下，得到一个新的值。其中，`dt_set`（set datetime）是一个过滤器，用于计算绝对日期时间。整个表达式 `BASETIME | dt_set(hour=9, mimute=30)` 的含义就是把 `BASETIME` 的小时数设为 `9`，分钟数设为 `30`，得到一个新的时间，即 `BASETIME` 当天的 09:30。
	+ 过滤器函数可由用户自由定制，目前已实现的过滤器包括 `dt_set`、`dt_add`、`dt_format`，分别用于生成绝对时间、相对时间、格式化时间字符串，详细用法参见【过滤器函数用法详解】。你可以在 `data_monitor/user/filters.py` 中查看它们的定义。

- `sql`：
	+ 前半部分容易理解，就是一个普通的 SQL 查询语句。
	+ `%(YESTERDAY)s` 是一个配置项引用，这是 `.cfg` 格式支持的一种语法，用于引用其他已存在的配置项的值。被引用的 `YESTERDAY` 已经定义在 `[DEFAULT]` section 中。

- `validator`：
	+ `result > 40` 是一个合法的 Python 表达式，如果 `result` 大于 `40`，将返回 `True`，否则返回 `False`。

当校验失败时，将发出类似下面的警报：

```
🙏
监控描述：演示-单值监控
作业名称：demo_single_value
发起时间：2019-05-24 09:00:00
====================
报警原因：数据校验未通过
--------------------
校验表达式：`result > 60`
查询结果`result`：`50L`
```

### 过滤器函数用法详解

#### `dt_set`

设置 datetime 的绝对量。

调用形式为： `dt_set(year, month, weekday, day, hour, minute, second, microsecond)`，所有参数均可缺省，每个参数均可单独设定。

**但需要注意，`weekday` 不能与 `year`、`month`、`day` 同时设定，因为可能会出现矛盾，可以使用两次过滤器分别设定，以避免矛盾**。

示例：

- `{BASETIME | dt_set(day=15)}`: BASETIME 所在月的15号
- `{BASETIME | dt_set(day=15, hour=9, minite=30, second=10)}`: BASETIME 所在月的15号的 09:30:10
- `{BASETIME | dt_set(month=10, day=1)}`: BASETIME 所在年份的10月1号
- `{BASETIME | dt_set(month=10, day=1) | dt_set(weekday=5)}`: BASETIME 所在年份的10月1号所在周的周五

#### `dt_add`

设置 datetime 偏移量。

调用形式为 `dt_add(years, months, weeks, days, hours, minutes, seconds, microseconds)`，所有参数均可缺省，每个参数均可单独设定。

示例：

- `{BASETIME | dt_add(days=-3)}`：BASETIME 三天前的同一时间
- `{BASETIME | dt_add(hours=9, minutes=30, seconds=10)}`：BASETIME 9小时30分钟10秒之后的时间
- `{BASETIME | dt_add(years=-1, months=-3)`：BASETIME 一年零三个月前的同一时间
- `{BASETIME | dt_add(weeks=-3)}`：BASETIME 三周前的同一时间

注意：`dt_add` 的 `months` 参数是指自然月。增减月份时，不管当月包含多少天，总是会保持增减后的日期与当前一致。例如 `2019-03-01` 前推1个月是 `2019-02-01`，前推2个月是 `2019-01-01`，这是符合直觉的。

当增减后的日期越界时，会截断到最大的非越界日期，例如：`2019-03-31` 前推1个月是 `2019-02-28`，而不是 `2019-02-31`，因为2019年2月不存在 29、30、31 号；基于这个原因，`2019-03-30`、`2019-03-29`、`2019-03-28` 前推一个月也都会得到 `2019-02-28`。

这一特性可能会导致粗心的错误，例如：`2019-03-31` 一次性前推2个月是 `2019-01-31`，但如果分作两次，每次分别前推一个月，却会得到 `2019-01-28`。这有点违背算术直觉，但也只能如此实现。避免这个错误的方法是，**尽量不要在月末的日期上做月粒度的增减操作，如果确实需要处理月末，可以先求出下个月的月初，然后减去一天**。

#### `dt_format`

将 datetime 格式化为字符串。调用形式为 `dt_format(fmt='%%Y-%%m-%%d %%H:%%M:%%S')`。该示例为 ISO 8601 格式，即 YYYY-MM-DD HH:MM:SS，如果 datetime 不采用任何格式化操作，默认输出的就是这种形式。可以根据自己的需要，将 datetime 格式化为需要的形式。

#### 组合使用

过滤器函数可以通过管道符串接叠加使用。

示例：

- `{BASETIME | dt_add(days=-3) | dt_set(hour=8, minute=30)}`: BASETIME 三天前的 08:30。
- `sql = SELECT ... WHERE stat_date = {BASETIME | dt_add(days=-1) | dt_format('%%Y%%m%%d')} AND stat_hour = '{DUETIME | dt_add(hour=-1) | dt_format('%%H')}'`

## 4. 使用

首先进入 `data-monitor` 主目录，程序的入口为 `data-monitor/main.py`。

对于上一节的配置示例，我们可以通过如下命令发起该监控作业：

```sh
python main.py --job demo_single_value
# 或者
# python main.py -j demo_single_value
```

如果你有多个监控作业需要发起，可多次使用 `--job` 选项，例如：

```sh
python main.py -j demo_single_value -j another_job
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
                        path(s) of job config file. support wildcards (path
                        contains wildcards must be quoted). if not provided,
                        use `job.cfg` under current path. you can provide
                        multiple config files by repeating `-c` option.
                        conflicted job names will be auto-detected.
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
[2019-05-24 18:03:08,475] data_monitor INFO: using job config file(s): ['/home/work/zhuhe212/workspace/data-monitor/job.cfg']
[2019-05-24 18:03:08,475] data_monitor INFO: checking job configs ...
[2019-05-24 18:03:08,509] data_monitor INFO: job [demo_single_value_with_sql_in_file] config OK.
[2019-05-24 18:03:08,523] data_monitor INFO: job [demo_diff_table] config OK.
[2019-05-24 18:03:08,534] data_monitor INFO: job [demo_single_value] config OK.
[2019-05-24 18:03:08,546] data_monitor.config INFO: skiped inactive job "demo_hourly_job"
[2019-05-24 18:03:08,561] data_monitor.config INFO: skiped inactive job "demo_single_table_history"
[2019-05-24 18:03:08,577] data_monitor INFO: job [demo_diff_value] config OK.
[2019-05-24 18:03:08,588] data_monitor INFO: job [demo_single_table] config OK.
[2019-05-24 18:03:08,589] data_monitor INFO: all job configs OK.
[2019-05-24 18:03:08,589] data_monitor INFO: monitor start ...
[2019-05-24 18:03:08,589] data_monitor INFO: ============================================================
[2019-05-24 18:03:08,589] data_monitor INFO: ****** total jobs: 5 ...
[2019-05-24 18:03:08,589] data_monitor INFO: ****** pending: 5, running: 0, completed: 0 ******
[2019-05-24 18:03:08,590] data_monitor INFO: job [demo_diff_table] is due. launched.
[2019-05-24 18:03:08,595] data_monitor INFO: ****** pending: 4, running: 1, completed: 0 ******
[2019-05-24 18:03:08,596] data_monitor INFO: job [demo_single_table] is due. launched.
[2019-05-24 18:03:08,597] data_monitor INFO: ****** pending: 3, running: 2, completed: 0 ******
[2019-05-24 18:03:08,598] data_monitor INFO: job [demo_single_value] is due. launched.
[2019-05-24 18:03:08,599] data_monitor INFO: ****** pending: 2, running: 3, completed: 0 ******
[2019-05-24 18:03:08,599] data_monitor INFO: job [demo_single_value_with_sql_in_file] is due. launched.
[2019-05-24 18:03:08,601] data_monitor INFO: ****** pending: 1, running: 4, completed: 0 ******
[2019-05-24 18:03:08,601] data_monitor INFO: job [demo_diff_value] is due. launched.
[2019-05-24 18:03:08,815] data_monitor INFO: job [demo_single_value] returned. status: =====> ALARM <=====
    🙏
    监控描述：演示-单值监控
    作业名称：demo_single_value
    发起时间：2019-05-24 09:00:00
    ====================
    报警原因：数据校验未通过
    --------------------
    校验表达式：`result > 60`
    查询结果`result`：`50L`
[2019-05-24 18:03:08,984] data_monitor INFO: job [demo_single_table] returned. status: OK.
[2019-05-24 18:03:08,985] data_monitor INFO: job [demo_single_value_with_sql_in_file] returned. status: OK.
[2019-05-24 18:03:08,985] data_monitor INFO: job [demo_diff_value] returned. status: OK.
[2019-05-24 18:03:08,985] data_monitor INFO: job [demo_diff_table] returned. status: OK.
[2019-05-24 18:03:08,985] data_monitor INFO: ****** pending: 0, running: 0, completed: 5 ******
[2019-05-24 18:03:08,985] data_monitor INFO: all jobs (5) finished.
[2019-05-24 18:03:08,985] data_monitor INFO: ============================================================
[2019-05-24 18:03:08,985] data_monitor INFO: monitor exit.
```

## 5. 更多配置示例

### 单值监控

```ini
[demo_single_value]
; 单值监控
desc = 演示-单值监控
due_time = {BASETIME | dt_set(hour=9)}
db_conf = mysql_test_db
sql =
    SELECT count(1)
    FROM table1
    WHERE event_day = '%(YESTERDAY)s'
validator = result > 60
alarm_email = zhuhe212
```

如果校验失败，将发出类似下面的警报：

```
🙏
监控描述：演示-单值监控
作业名称：demo_single_value
发起时间：2019-05-24 09:00:00
====================
报警原因：数据校验未通过
--------------------
校验表达式：`result > 60`
查询结果`result`：`50L`
```

### 单表监控

```ini
[demo_single_table]
; 单表监控
desc = 演示-单表监控
due_time = {BASETIME | dt_set(hour=9)}
db_conf = mysql_test_db
sql =
    SELECT event_day, count(*) AS num
    FROM table1
    WHERE event_day >= '{BASETIME | dt_add(months=-1)}'
    GROUP BY event_day
    ORDER BY event_day
validator = claim(result, gt(50))
alarm_email = zhuhe212
```

该示例的 `validator` 中使用了自定义校验函数 `claim` 和 `gt`，这些函数定义在 `data_monitor/user/validators.py` 中。其中：

- `claim` 函数用于断言一个 SQL 查询结果集。接收两个参数，参数一为查询结果集，参数二是一个“谓词函数”，接收一个单值并返回一个布尔值。
- `gt(50)` 是一个谓词函数，用于判定一个值是否“大于50”。类似的谓词还有 `ge`、`lt`、`le`、`eq`、`ne`，分别用于判定：大于等于、小于、小于等于、等于、不等于。
- 整个 validator 表达式的含义就是：判断查询结果集的 value 列（最后一列）的数据是否都大于50，如果有任意一个不大于50，则触发警报。警报中会列出所有不大于50的行。

如果校验失败，将发出类似下面的警报：

```
🙏
监控描述：演示-单表监控
作业名称：demo_single_table
发起时间：2019-05-24 09:00:00
====================
报警原因：数据缺失或不符合要求
校验表达式：`claim(result, gt(50), period="day")`
--------------------
不合格的数据：
     event_day  num has_data
0   2019-04-24   49      Yes
1   2019-04-25   48      Yes
2   2019-04-26   49      Yes
3   2019-04-27   45      Yes
4   2019-04-28   49      Yes
..         ...  ...      ...
25  2019-05-19   47      Yes
26  2019-05-20   39      Yes
27  2019-05-21   48      Yes
28  2019-05-22   50      Yes
29  2019-05-23   50      Yes
```

#### 谓词函数的组合

谓词函数可以通过 `ands`（且）、`ors`（或）两个高阶谓词函数进行自由组合（包括嵌套组合），这两个函数同样定义在 `data_monitor/user/validators.py` 中。使用示例如下：

```ini
; 大于 50 且小于 60 且不等于 55
validator = claim(result, ands(gt(50), lt(60), ne(55)))

; 大于 50 且小于 60 且不等于 55，或等于 0
validator = claim(result, ors(ands(gt(50), lt(60), ne(55)), eq(0)))
```

### 单表历史监控

在前一个示例中，通过 `claim` 校验函数，我们可以监控查询结果中所有不满足条件的行，但却无法监控缺失的行。例如某一天的数据缺失，由于查询结果终不存在该日期的数据，因此根本不会触发谓词函数，也就无法被监控到。

为了应对这种需求，`claim` 函数提供了以下几个额外的参数，用于监控连续序列终是否有缺失：

- serial: 是否要开启连续序列监控，默认开启，一般不必设置。
- period: 连续序列的周期，可以枚举以下几个值：year, month, week, day, hour，默认为 day。
- start: 连续序列的起始时间（包含），datetime 类型。如果不提供，则取 data 中检测到的最小 datetime。
- end: 连续序列的结束时间（包含），datetime 类型。如果不提供，则取 data 中检测到的最大 datetime。

当连续序列监控开启时，`claim` 会假定查询结果的第一列为要监控的连续序列，比如 `event_day`、`event_month` 等，一旦连续序列中间有空缺，将会触发“缺数”报警。示例如下：

```ini
[demo_single_table_history]
; 单表历史数据监控
desc = 演示-单表历史数据监控
due_time = {BASETIME | dt_set(hour=9)}
db_conf = mysql_test_db
sql =
    SELECT event_day
    FROM table1
    WHERE 
        event_day >= '{BASETIME | dt_add(months=-1)}'
        ; 为了发出报警故意漏选了某些时间……
        AND NOT substr(event_day, 9, 2) IN ('16', '25')
    GROUP BY event_day
    ORDER BY event_day
validator = claim(result, period='day', start='{BASETIME | dt_add(months=-1)}', end='%(YESTERDAY)s')
alarm_email = zhuhe212
```

该作业实现的功能是：监控一个月内的历史数据，如果有任何一天缺数，则报警。

报警信息如下：

```
🙏
监控描述：演示-单表历史数据监控
作业名称：demo_single_table_history
发起时间：2019-05-24 09:00:00
====================
报警原因：数据缺失或不符合要求
校验表达式：`claim(result, period='day', start='2019-04-24 00:00:00', end='20190523')`
--------------------
不合格的数据：
    event_day  num has_data
0  2019-04-25  NaN       缺数
1  2019-05-16  NaN       缺数
```

历史监控不仅可以监控缺数，也可以同时添加谓词条件，报警信息中会同时列出缺数和不满足条件的记录。例如，在上例中的校验表达式中添加谓词函数 `gt(40)`：

```ini
[demo_single_table_history]
; 单表历史数据监控
desc = 演示-单表历史数据监控
due_time = {BASETIME | dt_set(hour=9)}
db_conf = mysql_test_db
sql =
    ; 这里 SELECT 选择了两列，因为要对第二列做谓词判断
    SELECT event_day, count(*) AS num
    FROM table1
    WHERE event_day >= '{BASETIME | dt_add(months=-1)}'
        ; 为了发出报警故意漏选了某些时间……
        AND NOT substr(event_day, 9, 2) IN ('16', '25')
    GROUP BY event_day
    ORDER BY event_day
validator = claim(result, gt(40), period='day', start='{BASETIME | dt_add(months=-1)}', end='%(YESTERDAY)s')
alarm_email = zhuhe212
```

该作业实现的功能是：监控一个月内的历史数据，如果有任何一天缺数、或数据条数不大于40，则报警。

报警信息如下：

```
🙏
监控描述：演示-单表历史数据监控
作业名称：demo_single_table_history
发起时间：2019-05-24 09:00:00
====================
报警原因：数据缺失或不符合要求
校验表达式：`claim(result, gt(40), period='day', start='2019-04-24 00:00:00', end='20190523')`
--------------------
不合格的数据：
    event_day   num has_data
0  2019-04-25   NaN       缺数
1  2019-05-06  35.0      Yes
2  2019-05-15  39.0      Yes
3  2019-05-16   NaN       缺数
4  2019-05-20  39.0      Yes
```

#### 小时粒度的历史数据监控

如果要监控小时粒度的历史数据是否缺失，需要费一些周折，因为“小时粒度”隐含着“天粒度”，任意一天的任意一小数缺数都不能通过。然而，`claim` 函数只会假定查询结果的第一列为要监控的连续序列，因此我们需要把“日期”和“小时”组合到一起作为一列（可以借助 SQL 中的 `concat` 函数），详见如下示例：

```ini
[demo_single_table_history_hourly]
; 小时粒度数据的历史监控（注意：并不是监控周期为小时级，而是所监控数据的粒度为小时级！）
desc = 演示-单表历史数据监控-小时粒度
due_time = {BASETIME | dt_set(hour=9)}
db_conf = mysql_test_db
sql =
    ; 注意 SELECT 的第一个字段，必须是“日期”和“小时”的组合
    SELECT concat(stat_date, ' ', stat_hour) AS stat_hour, count(*) AS num
    FROM table2
    WHERE
        stat_date >= {BASETIME | dt_add(days=-3) | dt_format('%%Y%%m%%d')}
        ; 为了发出报警故意漏选了某些时间……
        AND stat_hour != '16'
    GROUP BY stat_date, stat_hour
    ORDER BY stat_hour
; 注意：claim 的参数 period 设为 'hour'
validator = claim(result, period='hour', start='{BASETIME | dt_add(days=-3)}', end='{DUETIME | dt_add(hours=-6)}')
alarm_email = zhuhe212
```

该作业实现的功能是：监控 3 天以内的历史数据，如果有任意一个小时缺数，则报警。

报警信息如下：

```
🙏
监控描述：演示-单表历史数据监控-小时粒度
作业名称：demo_single_table_history_hourly
发起时间：2019-05-24 09:00:00
====================
报警原因：数据缺失或不符合要求
校验表达式：`claim(result, period='hour', start='2019-05-21 00:00:00', end='2019-05-24 03:00:00')`
--------------------
不合格的数据：
       stat_hour  num has_data
0  2019-05-21 16  NaN       缺数
1  2019-05-22 16  NaN       缺数
2  2019-05-23 16  NaN       缺数
```

### 两表 diff

```ini
[demo_diff_table]
; 两表 diff
desc = 演示-两表 diff
due_time = {BASETIME | dt_set(hour=9)}
db_conf = mysql_test_db, mysql_test_db2
sql =
    SELECT event_day, product, partner, sum(click) AS num
    FROM table1
    WHERE event_day = '%(YESTERDAY)s'
    GROUP BY event_day, product, partner
    ::
    SELECT event_day, product, partner, sum(click) AS num
    FROM table1
    WHERE event_day = '%(YESTERDAY)s'
    GROUP BY event_day, product, partner
validator = diff(result[0], result[1], threshold=-1)
alarm_email = zhuhe212
```

该示例的 `validator` 中使用了自定义校验函数 `diff`，该函数定义在 `data_monitor/user/validators.py` 中。

其中，`diff(result[0], result[1], threshold=1)` 的含义是对 `result[0]` 和 `result[1]` 做 diff，如果 diff 的绝对值超过 `threshold`，则发出警报。警报信息中会给出所有不满足条件的行，示例如下：

```
🙏
监控描述：演示-两表 diff
作业名称：demo_diff_table
发起时间：2019-05-24 09:00:00
====================
报警原因：数据diff超出阈值
校验表达式：`diff(result[0], result[1], threshold=-1)`
--------------------
不合格的数据：
     event_day  key1  key2   num_1   num_2  diff
0   2019-05-23  k1v1  k2v1  672798  672798     0
1   2019-05-23  k1v2  k2v2       0       0     0
2   2019-05-23  k1v2  k2v3       0       0     0
3   2019-05-23  k1v1  k2v4  966881  966881     0
4   2019-05-23  k1v3  k2v5   10760   10760     0
..         ...   ...   ...     ...     ...   ...
12  2019-05-23  k1v1  k2v5   74393   74393     0
13  2019-05-23  k1v4  k2v6       0       0     0
14  2019-05-23  k1v3  k2v2       0       0     0
15  2019-05-23  k1v2  k2v6   35858   35858     0
16  2019-05-23  k1v4  k2v2       0       0     0
```

`diff` 函数还可以接受一个额外的参数 `direction` 用于指定 diff 的方向，其取值为 `-1`、`0`、`1`，分别代表左表减右表、两表相减取绝对值、右表减左表，默认值为 `0`。

### 小时级数据监控

```ini
[demo_hourly_job]
; 小时级监控
desc = 演示-小时级监控
period = hour
due_time = {BASETIME | dt_set(hour=6)}
db_conf = mysql_test_db
sql =
    SELECT count(*)
    FROM table2
    WHERE
        stat_date = %(TODAY)s
        ; 注意，这里使用 DUETIME 而不是 BASETIME！
        AND stat_hour = '{DUETIME | dt_add(hours=-6) | dt_format('%%H')}'
validator = result > 0
alarm_email = zhuhe212
```

该作业实现的功能是：每个小时检查六小时之前的那个小时的数据是否已就绪，如果未就绪则发出警报。

小时级任务比起其他任务有些特殊，主要体现在以下几个方面：

- 需要在配置中明确指定 `period = hour`。
- 程序会在配置加载完成后，将每个小时级任务复制成 24 份，它们的 `due_time` 分别为初始 `due_time` 加上 0~23 小时，名称为原始名称加上小时后缀，以便报警时区分。
- 小时级任务的 `sql` 中，一般要使用 `DUETIME` 环境变量，而不是 `BASETIME`。这样用户的 sql 就可以关联到作业的调起时间，比如“每个小时检查 6 小时之前的数据是否就绪”。

执行小时级的任务，打印的日志也会体现出 24 个作业：

```sh
python main.py -j demo_hourly_job
```

```
[2019-05-23 16:12:12,332] data_monitor INFO: using job config file(s): ['/home/work/zhuhe212/workspace/data-monitor/job.cfg']
[2019-05-23 16:12:12,332] data_monitor INFO: checking job configs ...
[2019-05-23 16:12:12,369] data_monitor INFO: job [demo_hourly_job_hour06] config OK.
[2019-05-23 16:12:12,372] data_monitor INFO: job [demo_hourly_job_hour07] config OK.
[2019-05-23 16:12:12,375] data_monitor INFO: job [demo_hourly_job_hour08] config OK.
[2019-05-23 16:12:12,377] data_monitor INFO: job [demo_hourly_job_hour09] config OK.
[2019-05-23 16:12:12,380] data_monitor INFO: job [demo_hourly_job_hour10] config OK.
[2019-05-23 16:12:12,382] data_monitor INFO: job [demo_hourly_job_hour11] config OK.
[2019-05-23 16:12:12,385] data_monitor INFO: job [demo_hourly_job_hour12] config OK.
[2019-05-23 16:12:12,387] data_monitor INFO: job [demo_hourly_job_hour13] config OK.
[2019-05-23 16:12:12,391] data_monitor INFO: job [demo_hourly_job_hour14] config OK.
[2019-05-23 16:12:12,394] data_monitor INFO: job [demo_hourly_job_hour15] config OK.
[2019-05-23 16:12:12,397] data_monitor INFO: job [demo_hourly_job_hour16] config OK.
[2019-05-23 16:12:12,400] data_monitor INFO: job [demo_hourly_job_hour17] config OK.
[2019-05-23 16:12:12,403] data_monitor INFO: job [demo_hourly_job_hour18] config OK.
[2019-05-23 16:12:12,406] data_monitor INFO: job [demo_hourly_job_hour19] config OK.
[2019-05-23 16:12:12,409] data_monitor INFO: job [demo_hourly_job_hour20] config OK.
[2019-05-23 16:12:12,412] data_monitor INFO: job [demo_hourly_job_hour21] config OK.
[2019-05-23 16:12:12,415] data_monitor INFO: job [demo_hourly_job_hour22] config OK.
[2019-05-23 16:12:12,418] data_monitor INFO: job [demo_hourly_job_hour23] config OK.
[2019-05-23 16:12:12,420] data_monitor INFO: job [demo_hourly_job_hour00] config OK.
[2019-05-23 16:12:12,423] data_monitor INFO: job [demo_hourly_job_hour01] config OK.
[2019-05-23 16:12:12,425] data_monitor INFO: job [demo_hourly_job_hour02] config OK.
[2019-05-23 16:12:12,428] data_monitor INFO: job [demo_hourly_job_hour03] config OK.
[2019-05-23 16:12:12,430] data_monitor INFO: job [demo_hourly_job_hour04] config OK.
[2019-05-23 16:12:12,433] data_monitor INFO: job [demo_hourly_job_hour05] config OK.
[2019-05-23 16:12:12,433] data_monitor INFO: all job configs OK.
[2019-05-23 16:12:12,433] data_monitor INFO: monitor start ...
[2019-05-23 16:12:12,433] data_monitor INFO: ============================================================
[2019-05-23 16:12:12,433] data_monitor INFO: ****** total jobs: 24 ...
[2019-05-23 16:12:12,434] data_monitor INFO: ****** pending: 24, running: 0, completed: 0 ******
[2019-05-23 16:12:12,442] data_monitor INFO: job [demo_hourly_job_hour06] is due. launched.
[2019-05-23 16:12:12,443] data_monitor INFO: ****** pending: 23, running: 1, completed: 0 ******
[2019-05-23 16:12:12,443] data_monitor INFO: job [demo_hourly_job_hour07] is due. launched.
[2019-05-23 16:12:12,459] data_monitor INFO: ****** pending: 22, running: 2, completed: 0 ******
[2019-05-23 16:12:12,459] data_monitor INFO: job [demo_hourly_job_hour08] is due. launched.
[2019-05-23 16:12:12,461] data_monitor INFO: ****** pending: 21, running: 3, completed: 0 ******
[2019-05-23 16:12:12,463] data_monitor INFO: job [demo_hourly_job_hour09] is due. launched.
[2019-05-23 16:12:12,464] data_monitor INFO: ****** pending: 20, running: 4, completed: 0 ******
[2019-05-23 16:12:12,465] data_monitor INFO: job [demo_hourly_job_hour10] is due. launched.
[2019-05-23 16:12:12,466] data_monitor INFO: ****** pending: 19, running: 5, completed: 0 ******
[2019-05-23 16:12:12,467] data_monitor INFO: job [demo_hourly_job_hour11] is due. launched.
[2019-05-23 16:12:12,468] data_monitor INFO: ****** pending: 18, running: 6, completed: 0 ******
[2019-05-23 16:12:12,469] data_monitor INFO: job [demo_hourly_job_hour12] is due. launched.
[2019-05-23 16:12:12,470] data_monitor INFO: ****** pending: 17, running: 7, completed: 0 ******
[2019-05-23 16:12:12,471] data_monitor INFO: job [demo_hourly_job_hour13] is due. launched.
[2019-05-23 16:12:12,472] data_monitor INFO: ****** pending: 16, running: 8, completed: 0 ******
[2019-05-23 16:12:12,472] data_monitor INFO: job [demo_hourly_job_hour14] is due. launched.
[2019-05-23 16:12:12,474] data_monitor INFO: ****** pending: 15, running: 9, completed: 0 ******
[2019-05-23 16:12:12,477] data_monitor INFO: job [demo_hourly_job_hour15] is due. launched.
[2019-05-23 16:12:12,479] data_monitor INFO: ****** pending: 14, running: 10, completed: 0 ******
[2019-05-23 16:12:12,479] data_monitor INFO: job [demo_hourly_job_hour16] is due. launched.
[2019-05-23 16:12:12,480] data_monitor INFO: ****** pending: 13, running: 11, completed: 0 ******
[2019-05-23 16:12:17,486] data_monitor INFO: job [demo_hourly_job_hour13] returned. status: OK.
[2019-05-23 16:12:17,486] data_monitor INFO: job [demo_hourly_job_hour11] returned. status: OK.
[2019-05-23 16:12:17,486] data_monitor INFO: job [demo_hourly_job_hour12] returned. status: OK.
[2019-05-23 16:12:17,486] data_monitor INFO: job [demo_hourly_job_hour08] returned. status: OK.
[2019-05-23 16:12:17,486] data_monitor INFO: job [demo_hourly_job_hour16] returned. status: OK.
[2019-05-23 16:12:17,486] data_monitor INFO: job [demo_hourly_job_hour06] returned. status: OK.
[2019-05-23 16:12:17,487] data_monitor INFO: job [demo_hourly_job_hour15] returned. status: OK.
[2019-05-23 16:12:17,487] data_monitor INFO: job [demo_hourly_job_hour07] returned. status: OK.
[2019-05-23 16:12:17,487] data_monitor INFO: job [demo_hourly_job_hour14] returned. status: OK.
[2019-05-23 16:12:17,487] data_monitor INFO: job [demo_hourly_job_hour09] returned. status: OK.
[2019-05-23 16:12:17,487] data_monitor INFO: job [demo_hourly_job_hour10] returned. status: OK.
[2019-05-23 16:12:17,487] data_monitor INFO: ****** pending: 13, running: 0, completed: 11 ******
[2019-05-23 16:12:17,487] data_monitor INFO: sleeping until the most recent job [demo_hourly_job_hour17] due (2019-05-23 17:00:00) ...
```
