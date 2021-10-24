package zcSql.cli;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author 赵晨
 * Date on 2021/10/14
 */
public class SqlCommandParser {
    private static final Function<String[], Optional<String[]>> NO_OPERANDS = (operands) -> {
        return Optional.of(new String[0]);
    };
    private static final Function<String[], Optional<String[]>> SINGLE_OPERAND = (operands) -> {
        return Optional.of(new String[]{operands[0]});
    };
    private static final int DEFAULT_PATTERN_FLAGS = 34;

    private SqlCommandParser() {
    }

    public static List<SqlCommandCall> parse(List<String> lines) {
        List<SqlCommandParser.SqlCommandCall> calls = new ArrayList();
        StringBuilder stmt = new StringBuilder();
        Iterator var3 = lines.iterator();

        while(var3.hasNext()) {
            String line = (String)var3.next();
            if (!line.trim().isEmpty() && !line.startsWith("--")) { // 所以用户的--注释就在这里过滤了
                 // （用户在页面写的任何sql 片段都保留了下来，然后再这里提交的时候才判断）
                stmt.append("\n").append(line);
                if (line.trim().endsWith(";")) {  // 以；结尾的话，也是在这里，不以;结尾的话，就不会提交了
                    // 在下面的parse 方法中存在解析的sql的类型，比如create table, create function等
                    Optional<SqlCommandParser.SqlCommandCall> optionalCall = parse(stmt.toString());
                    if (!optionalCall.isPresent()) {
                        throw new RuntimeException("Unsupported command '" + stmt.toString() + "'");
                    }

                    calls.add(optionalCall.get());
                    stmt.setLength(0);
                }
            }
        }

        return calls;
    }

    public static Optional<SqlCommandParser.SqlCommandCall> parse(String stmt) {
        stmt = stmt.trim();
        if (stmt.endsWith(";")) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }

        SqlCommandParser.SqlCommand[] var1 = SqlCommandParser.SqlCommand.values();
        int var2 = var1.length;

        for(int var3 = 0; var3 < var2; ++var3) {
            SqlCommandParser.SqlCommand cmd = var1[var3];
            Matcher matcher = cmd.pattern.matcher(stmt);
            if (matcher.matches()) {
                String[] groups = new String[matcher.groupCount()];

                for(int i = 0; i < groups.length; ++i) {
                    groups[i] = matcher.group(i + 1);
                }

                return ((Optional)cmd.operandConverter.apply(groups)).map((operands) -> {
                    return new SqlCommandParser.SqlCommandCall(cmd, (String[]) operands);
                });
            }
        }

        return Optional.empty();
    }

    public static class SqlCommandCall {
        public final SqlCommandParser.SqlCommand command;
        public final String[] operands;

        public SqlCommandCall(SqlCommandParser.SqlCommand command, String[] operands) {
            this.command = command;
            this.operands = operands;
        }

        public SqlCommandCall(SqlCommandParser.SqlCommand command) {
            this(command, new String[0]);
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o != null && this.getClass() == o.getClass()) {
                SqlCommandParser.SqlCommandCall that = (SqlCommandParser.SqlCommandCall)o;
                return this.command == that.command && Arrays.equals(this.operands, that.operands);
            } else {
                return false;
            }
        }

        public int hashCode() {
            int result = Objects.hash(new Object[]{this.command});
            result = 31 * result + Arrays.hashCode(this.operands);
            return result;
        }

        public String toString() {
            return this.command + "(" + Arrays.toString(this.operands) + ")";
        }
    }

    public static enum SqlCommand {
        USE("(USE\\s+.*)", SqlCommandParser.SINGLE_OPERAND),
        INSERT_INTO("(INSERT\\s+INTO.*)", SqlCommandParser.SINGLE_OPERAND),
        CREATE_TABLE("(CREATE\\s+TABLE.*)", SqlCommandParser.SINGLE_OPERAND),
        CREATE_FUNCTION("(CREATE\\s+FUNCTION.*)", SqlCommandParser.SINGLE_OPERAND),
        CREATE_TEMPORARY_FUNCTION("(CREATE\\s+TEMPORARY.*)", SqlCommandParser.SINGLE_OPERAND),
        CREATE_VIEW("(CREATE\\s+VIEW.*)", SqlCommandParser.SINGLE_OPERAND),
        CREATE_CATALOG("(CREATE\\s+CATALOG.*)", SqlCommandParser.SINGLE_OPERAND),
        CREATE_DATABASE("(CREATE\\s+DATABASE.*)", SqlCommandParser.SINGLE_OPERAND),
        SET("SET(\\s+(\\S+)\\s*=(.*))?", (operands) -> {
            if (operands.length < 3) {
                return Optional.empty();
            } else {
                return operands[0] == null ? Optional.of(new String[0]) : Optional.of(new String[]{operands[1], operands[2]});
            }
        });

        public final Pattern pattern;
        public final Function<String[], Optional<String[]>> operandConverter;

        private SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
            this.pattern = Pattern.compile(matchingRegex, 34);
            this.operandConverter = operandConverter;
        }

        public String toString() {
            return super.toString().replace('_', ' ');
        }

        public boolean hasOperands() {
            return this.operandConverter != SqlCommandParser.NO_OPERANDS;
        }
    }
}
