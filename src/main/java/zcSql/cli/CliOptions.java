package zcSql.cli;

/**
 * @Author 赵晨
 * Date on 2021/10/14
 */
public class CliOptions {
    private final String sqlFilePath;
    private final String workingSpace;

    public CliOptions(String sqlFilePath, String workingSpace) {
        this.sqlFilePath = sqlFilePath;
        this.workingSpace = workingSpace;
    }

    public String getSqlFilePath() {
        return this.sqlFilePath;
    }

    public String getWorkingSpace() {
        return this.workingSpace;
    }
}
