/*
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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

public class Putongsql {

    // 这是线上普通sql 执行的逻辑，不是数仓sql ()

    private String sqlFilePath;
    private String workSpace;
    private String debug;
    private String ck;
    private TableEnvironment tableEnv;
    private String ddl;
    private String chain;

    public static void main(String[] args) throws Exception {
        CliOptions options = CliOptionsParser.parseClient(args);
        String debug = args[4];
        String ck = args[5];
        String ddl = "";
        String chain = "";
        if (args.length >= 8) {
            ddl = args[6];
            chain = args[7];
        }

        System.out.println("--------------" + ck + "-----------" + debug);
        Putongsql submit = new Putongsql(options, debug, ck, ddl, chain);
        submit.run();
    }

    private Putongsql(CliOptions options, String debug, String ck, String ddl, String chain) {
        this.sqlFilePath = options.getSqlFilePath();
        this.workSpace = options.getWorkingSpace();
        this.debug = debug;
        this.ck = ck;
        this.ddl = ddl;
        this.chain = chain;
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
        this.tableEnv.registerCatalog("flink_hive", hive);
        List<String> sql = Files.readAllLines(Paths.get(this.workSpace + "/" + this.sqlFilePath));
        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sql);
        Iterator var9;
        SqlCommandParser.SqlCommandCall call;
        if (this.ddl != null && this.ddl.equals("true")) {
            var9 = calls.iterator();

            while(var9.hasNext()) {
                call = (SqlCommandParser.SqlCommandCall)var9.next();
                this.tableEnv.executeSql(call.operands[0]);
            }

        } else {
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
*/
