package zcSql;



import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import zcSql.cli.CliOptions;
import zcSql.cli.CliOptionsParser;
import zcSql.cli.SqlCommandParser;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

/**
 * @Author 赵晨
 * Date on 2021/10/14
 */
public class SqlSubmit {
    private String sqlFilePath;
    private String workSpace;
    private String debug;
    private String ck;
    private TableEnvironment tableEnv;
    private String ddl;
    private String chain;
    private String type;

    public static void main(String[] args) throws Exception {
        CliOptions options = CliOptionsParser.parseClient(args);
        String debug = args[4];
        String ck = args[5];
        String ddl = "";
        String chain = "";
        String type = "";
        if (args.length >= 9) {
            ddl = args[6];
            chain = args[7];
            type = args[8];
        }

        System.out.println("--------------" + ck + "-----------" + debug);
        SqlSubmit submit = new SqlSubmit(options, debug, ck, ddl, chain, type);
        submit.run();
    }

    private SqlSubmit(CliOptions options, String debug, String ck, String ddl, String chain, String type) {
        this.sqlFilePath = options.getSqlFilePath();
        this.workSpace = options.getWorkingSpace();
        this.debug = debug;
        this.ck = ck;
        this.ddl = ddl;
        this.chain = chain;
        this.type = type;
    }

    private void run() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String name;
        String defaultDatabase;
        String hiveConfDir;
        if (this.ck != null && !this.ck.equals("false")) {
            name = JSON.parseObject(this.ck).getString("task_id");
            defaultDatabase = JSON.parseObject(this.ck).getString("checkpoint_interval");
            hiveConfDir = JSON.parseObject(this.ck).getString("checkpoint_mode");
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.valueOf(hiveConfDir));
            env.getCheckpointConfig().setCheckpointInterval(Long.valueOf(defaultDatabase) * 1000L);
        }

        if (this.chain != null && this.chain.equals("true")) {
            env.disableOperatorChaining();
        }

        this.tableEnv = StreamTableEnvironment.create(env, settings);
        name = "myhive";
        defaultDatabase = "default";
        hiveConfDir = "/home/work/software/hive_online_client/hive_online/conf";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        List<String> sql = Files.readAllLines(Paths.get(this.workSpace + "/" + this.sqlFilePath));
        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sql);
        System.out.println("-------------" + this.ddl + "--------" + this.type);
        Iterator var9;
        SqlCommandParser.SqlCommandCall call;
        if (this.ddl != null && this.ddl.equals("true")) {
            // todo   单独的ddl 建表（不走execute）， 数仓/数据湖的逻辑sql, 普通的sql(有建表+有逻辑) ，以及debug测试sql()
            // todo ddl,没有执行execute，不提交，只在客户端执行
                   // 存在个问题，建立icberg 表+hadoop catalog,不提供hadoop的包+iceberg的包可以吗？
            // 因为这个类是提交到flink cli，哪个地方已经有hadoop 的了， 所以这里不用加hadoop 的lib (如果不是提交给hadoop cli，就要加上)
            // wstream 需要hadoop lib 是因为他不提交给flink,他就是普通的java 程序，需要链接hadoop,
            // 看下iceberg 的修改删除脚本，我写的为啥需要hadooplib + 而且提交给flink 了
            // 存在个问题，执行flink clie 的话一般有日志，而且就是在wstream 机器执行的，所以我们的脚本吧主类调用的放在flink cli。 对应的日志
            // 就是 wstream 的客户端日志中  /home/work/software/flink-1.13.1-warehouse-bh_new/log 下的flink-work-client-tjtx-126-221.58os.org.log

/*
          用户的建表ddl(数仓，数据湖)都是用flink client 执行的(单独脚本)，所以再脚本中需要客户端的环境，
          而删除表，修改的话，
          删除数仓表，用的是hive 脚本，删除数据湖用的是iceberg（也是单独的一个jar）
          修改数据湖表，自己写的脚本

*/

          // 更改icebegr schema的时候，用的是普通的方法，没有提交给flink cli ,但是因为其他的比如日志配置用的，所以在日志中可以看到iceBerga的main 方法的log

            if (this.type != null && this.type.equals("iceberg")) {
                this.tableEnv.executeSql("CREATE CATALOG dataplat_wstream_catalog WITH (\n  'type'='iceberg',\n  'catalog-type'='hadoop',\n  'warehouse'='viewfs://58-cluster/home/lakehouse/iceberg',\n  'property-version'='1')");
                this.tableEnv.useCatalog("dataplat_wstream_catalog");
                System.out.println("-------------" + this.tableEnv.getCurrentCatalog());
            } else {
                this.tableEnv.registerCatalog("flink_hive", hive);
                this.tableEnv.useCatalog("flink_hive");
            }

            var9 = calls.iterator();

            while(var9.hasNext()) {
                call = (SqlCommandParser.SqlCommandCall)var9.next();
                this.tableEnv.executeSql(call.operands[0]);
            }

        } else {  // todo 执行sql 的话，iceberg 的sql 用的是hive 的catalog啊,但是能执行成功，怎么找到iceberg 的表呢
            // 如果普通的sql(有ddl+insertinto)也走这个的话，走hive catalog的话，可以吗？普通sql 在hiv ecata 下执行ddl. 会不会在hive 下建表呢
            this.tableEnv.executeSql("CREATE CATALOG dataplat_wstream_catalog WITH (\n  'type'='iceberg',\n  'catalog-type'='hadoop',\n  'warehouse'='viewfs://58-cluster/home/lakehouse/iceberg',\n  'property-version'='1')");
            this.tableEnv.registerCatalog("flink_hive", hive);
            this.tableEnv.useCatalog("flink_hive");
            var9 = calls.iterator();

            while(var9.hasNext()) {
                call = (SqlCommandParser.SqlCommandCall)var9.next();
                this.callCommand(call);
            }

            if (Boolean.valueOf(this.debug)) {
                this.tableEnv.execute("Wstream_SQL_debug Job");
                System.out.println("Success");
            } else {
                this.tableEnv.execute("SQL Job");
            }

        }
    }

    private void callCommand(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        switch(cmdCall.command) {
            case SET:
                this.callSet(cmdCall);
                break;
            default:
                this.tableEnv.sqlUpdate(ddl);
        }

    }

    private void callSet(SqlCommandParser.SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        if (key.equals("table.exec.emit.late-fire.enabled")) {
            this.tableEnv.getConfig().setIdleStateRetentionTime(Time.minutes(30L), Time.minutes(60L));
        }

        this.tableEnv.getConfig().getConfiguration().setString(key, value);
    }

    private void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];

        try {
            this.tableEnv.sqlUpdate(ddl);
        } catch (SqlParserException var4) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", var4);
        }
    }

    private void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];

        try {
            this.tableEnv.sqlUpdate(dml);
        } catch (SqlParserException var4) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", var4);
        }
    }
}