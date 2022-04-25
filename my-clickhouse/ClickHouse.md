# ClickHouse入门

## 一、Mac安装ClickHouse

**1、下载**

```sh
mkdir clickhouse
cd clickhouse
curl -O 'https://builds.clickhouse.com/master/macos/clickhouse' && chmod a+x ./clickhouse
```

**2、启动clickhouse server**

```shell
./clickhouse server
```

![image-20220425233033560](https://xh-blog-1258155326.cos.ap-guangzhou.myqcloud.com/2022-04-25-153043.png)

看到```Application: Ready for connections.```说明ClickHouse server启动成功。

启动后，ClickHouse的目录如下：

![image-20220425233225333](https://xh-blog-1258155326.cos.ap-guangzhou.myqcloud.com/2022-04-25-153230.png)

ClickHouse的页面：http://localhost:8123/play

![image-20220425233358744](https://xh-blog-1258155326.cos.ap-guangzhou.myqcloud.com/2022-04-25-153404.png)

## 二、ClickHouse客户端访问

**1、CLI**

```sh
./clickhouse client
```

![image-20220425233720906](https://xh-blog-1258155326.cos.ap-guangzhou.myqcloud.com/2022-04-25-153725.png)

查看ClickHouse帮助

```shell
./clickhouse help
```

![image-20220425233919615](https://xh-blog-1258155326.cos.ap-guangzhou.myqcloud.com/2022-04-25-153927.png)

**2、非交互式访问**

**单个SQL：**

```shell
./clickhouse client --query "show tables in system"
```

![image-20220425233957683](https://xh-blog-1258155326.cos.ap-guangzhou.myqcloud.com/2022-04-25-153958.png)

**多个SQL：**

```shell
./clickhouse client --multiquery --query="select 1;select 2;select 3;"
```

![image-20220425234902022](https://xh-blog-1258155326.cos.ap-guangzhou.myqcloud.com/2022-04-25-154908.png)

**重要参数：**

--host / -h：服务端的地址，默认localhost，如果修改了config.xml内的listen_host则根据此修改地址。

--port：服务端的端口，默认9000。

--user / -u：登录的用户名，默认default。

--password：登录密码，默认为空。

--database / -d：登录的数据库，默认default。

--query / -q：只能在非交互式查询时使用，用于指定SQL语句。

--multiquery / -n：在非交互式执行时，允许执行多条SQL语句，多条语句之间用分号间隔。

--time / -t：在非交互式查询时，会打印每条SQL执行的时间。

**3、JDBC：**

添加依赖：

```xml
<dependency>
  <groupId>ru.yandex.clickhouse</groupId>
  <artifactId>clickhouse-jdbc</artifactId>
  <version>0.2.5</version>
</dependency>
```

代码：

```java
public class Test {
    public static void main(String[] args) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://localhost:8123/default";
        String user = "default";
        String password = "";
        Connection con = DriverManager.getConnection(url, user, password);
        System.out.println(con);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 1");
        rs.next();
        System.out.println(rs.getInt(1));
    }
}
```

高可用模式的URL如下：

```java
String url = "jdbc:clickhouse://localhost1:8123,localhost2:8123/default";
```

## 三、实用工具

**1、clickhouse-benchmar**

clickhouse-benchmar是基准测试的小工具，可以自动运行SQL查询，并生成相应的运行指标报告，如下：

```shell
echo "select * from system.numbers limit 100" | ./clickhouse benchmark -i 5 Loaded 1 queries
```

![image-20220426001150889](https://xh-blog-1258155326.cos.ap-guangzhou.myqcloud.com/2022-04-25-161158.png)

可以指定多条SQL进行测试，需要将SQL语句定义在文件中：

```sql
select * from system.numbers limit 100
select * from system.numbers limit 200
```

在multi-sqls文件内定义了两条SQL，按照顺序依次执行：

```sh
./clickhouse benchmark -i 5 < ./multi-sqls
```

![image-20220426001553359](https://xh-blog-1258155326.cos.ap-guangzhou.myqcloud.com/2022-04-25-161557.png)

clickhouse-benchmar的核心参数：

-i / --iterations：SQL查询执行的次数，默认值是0。

-c / --concurrency：同时执行查询的并发数，默认值是1。

-r / --randomize：在执行多条SQL语句的时候，按照随机顺序执行。

-h / --host：服务端地址，默认值是localhost。















