package zcSql;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.io.*;
import java.util.ArrayList;

public class MyownSubmit {

    public static void main(String[] args) throws Exception {



/*
        sql 在文件里面，读取文件中的sql 然后进行解析，按照分号; 进行分割

*/

        String file=args[0]; // sql 文件路径（里面包含的sql 内容）
        String kind=args[1]; // 是不是数仓sql(不是的话，，
        // 那就是普通sql, 那就是整体都执行，sql 中有ddl,有insetinto。 而且是内存cata)
        // 是数仓sql 的话，是hive catalog， 而且分ddl 部分，和insert into 部分
        // 而且每个sql 还有测试debug.debug 的话（建表没有debug,只是insert into 这种有逻辑的有debug），
        // 普通sql 没有单独的建表逻辑这个分支，所以就是执行sql
        // debug的话就是要走execute方法了，但是debug的话，只是构建dag 图，不进入JM,TM
        String isddl="";
        if("putong".equals(kind) || "iceberg".equals(kind)){
            isddl=args[2]; // sql 类型，是只是ddl, 还是执行逻辑

             }




        File file1 = new File(file);
      //  long length = file1.length();

        FileInputStream fileInputStream = new FileInputStream(file1);
        int available = fileInputStream.available();


        byte[] bytes = IOUtils.readFully(fileInputStream, available);

        String lines = new String(bytes);

        if(StringUtils.isBlank(lines)){
            return;
        }
        String[] split = lines.split(";");

        ArrayList<String> executesql=new ArrayList<>();// 存的是过滤后执行的sql
        for (String s : split) {

            if(s.startsWith("--") || StringUtils.isBlank(s)){
                continue; // --开头的sql 不处理，注释调了
            }else{

                String s1 = s.toLowerCase(); // -- 把sql 都变成小写
                executesql.add(s1);
            }
        }



        execute(file,kind,isddl,executesql);

    }

    private static void execute(String file, String kind, String isddl, ArrayList<String> executesql) throws Exception {




        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        if("putong".equals(kind)){ // 普通sql 的话,所有的语句都直接执行，用的是内存catalog 吧

           // 普通sql 就是执行执行，不用hive cata
            for (String s : executesql) {
                tableEnvironment.sqlUpdate(s);
            }

            if("debug sql".equals("")){  // 传入普通sql 的debug 标识
               // tableEnvironment.execute("普通sql的debug");
                tableEnvironment.execute("Wstream_SQL_debug Job");
                System.out.println("Success");

            }else{
                tableEnvironment.execute("普通sql");

            }

        }else if("warehouse".equals(kind)){ // 数仓sql 的话，需要执行hive catalog

                  //  defaultDatabase 是当前sql 执行的环境的库，可以在这个库下建立flink 的表，但是不能建立库。
            //  hive 中不能嵌套库（也就是说库中建立个库，默认给你并列），所以即使这里写死库，
            //  在sql 中也可以写use db333;然后执行ddl建立表A, 这样A 就在db333下了
            /*
            *
            * */
            HiveCatalog zcHive = new HiveCatalog("zcHive", "flinksqlhiveMetastore", "/opt/module/hive-2.3.9-bin/conf");
            tableEnvironment.registerCatalog("zcHive",zcHive);

           // 数仓的话，用hive cata
            tableEnvironment.useCatalog("zcHive");


            if("ddl".equals(isddl)){ // 如果是数仓sql.而且只是ddl ,那就只建立表，建立在hive catalog 下
                for (String s : executesql) {
                    tableEnvironment.sqlUpdate(s);
                }
            } else{  // 执行逻辑，需要execute

                for (String s : executesql) {
                    tableEnvironment.sqlUpdate(s);
                }

                tableEnvironment.execute("数仓sql执行逻辑");
            }
                     // 数据湖建立的表，然后数据湖和数仓的数据transfer.可以吗？
        } else if("iceberg".equals(kind)){  // 表示这有数据湖的表参与

            if("ddl".equals(isddl)){  // 建立数据湖的表，建表的时候是需要指定catalog 是哪个，把表存进去(现在是hadoop catalog)
                tableEnvironment.executeSql("CREATE CATALOG dataplat_wstream_catalog WITH (\n  'type'='iceberg',\n  'catalog-type'='hadoop',\n " +
                        " 'warehouse'='hdfs://hadoop001:8020/icebergtableOnhdfs',\n  'property-version'='1')");
                tableEnvironment.useCatalog("dataplat_wstream_catalog");
                for (String s : executesql) {
                    tableEnvironment.sqlUpdate(s);  // 在hadoop catalog 下执行建表逻辑，吧表存在hadoop 下
                }

            }else{  // 那就是执行sql 任务，只是任务中有数据湖的表

                // 这样指定了hadoop catalog 的话，sql 中有这个catalog的话就可以找到对应hdfs 的地址了，
                // ，而且下面设置了当前是在hive 中，
                // 所以当你吧kfk 的表建立在hive 中，数据湖的表建立在hadoop catalog， 而且把kfk 的数据入湖, insert into 的sql 中
                // kfk的表只需要db.tb（因为设置了当前是hive catalog，默认从hive 中找），而数据湖的表，你需要写catalog.db.tb 这样3个元素，吧catalog也写出来
                tableEnvironment.executeSql("CREATE CATALOG dataplat_wstream_catalog WITH (\n  'type'='iceberg',\n  'catalog-type'='hadoop',\n " +
                        " 'warehouse'='hdfs://hadoop001:8020/icebergtableOnhdfs',\n  'property-version'='1')");


                HiveCatalog zcHive = new HiveCatalog("zcHive", "flinksqlhiveMetastore", "/opt/module/hive-2.3.9-bin/conf");
                tableEnvironment.registerCatalog("flink_hive", zcHive);
                tableEnvironment.useCatalog("flink_hive");

                for (String s : executesql) {
                    tableEnvironment.sqlUpdate(s);
                }

                tableEnvironment.execute("数仓sql执行逻辑");



            }



        }





    }


/*    private void callCommand(SqlCommandParser.SqlCommandCall cmdCall) {
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
    }*/

}
