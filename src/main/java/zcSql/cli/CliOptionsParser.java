package zcSql.cli;

import org.apache.commons.cli.*;

/**
 * @Author 赵晨
 * Date on 2021/10/14
 */
public class CliOptionsParser {
    public static final Option OPTION_WORKING_SPACE = Option.builder("w").required(true).longOpt("working_space").numberOfArgs(1).argName("working space dir").desc("The working space dir.").build();
    public static final Option OPTION_SQL_FILE = Option.builder("f").required(true).longOpt("file").numberOfArgs(1).argName("SQL file path").desc("The SQL file path.").build();
    public static final Options CLIENT_OPTIONS = getClientOptions(new Options());

    public CliOptionsParser() {
    }

    public static Options getClientOptions(Options options) {
        options.addOption(OPTION_SQL_FILE);
        options.addOption(OPTION_WORKING_SPACE);
        return options;
    }

    public static CliOptions parseClient(String[] args) {
        if (args.length < 1) {
            throw new RuntimeException("./sql-submit -w <work_space_dir> -f <sql-file>");
        } else {
            try {
                DefaultParser parser = new DefaultParser();
                CommandLine line = parser.parse(CLIENT_OPTIONS, args, true);
                return new CliOptions(line.getOptionValue(OPTION_SQL_FILE.getOpt()), line.getOptionValue(OPTION_WORKING_SPACE.getOpt()));
            } catch (ParseException var3) {
                throw new RuntimeException(var3.getMessage());
            }
        }
    }
}
