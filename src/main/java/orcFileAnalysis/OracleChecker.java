/*
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;

import java.sql.*;

public class OracleChecker {

    private static final Logger logger = Logger.getLogger(OracleChecker.class);

    private static final String QUERY_TYPE_CGNAT = "'CGNAT'";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        String queryId = "5387";
        String taskId = "5387_2";
        String ipType = "ipv4";
        String natExpression = "natExpression_3";
        String sasnExpression = "sasnExpression_3";
        String sparkExpression = "sparkExpression_3";

        updateQueryExpression(natExpression, sasnExpression, sparkExpression, queryId);
        //updateQueryExpression_1(natExpression, sasnExpression, sparkExpression, queryId);
        //updateTaskExpression(natExpression, sasnExpression, queryId, taskId, ipType);
    }

    public static void updateQueryExpression_1(String natExpression, String sasnExpression, String sparkExpression, String queryId) throws SQLException {

        String natExpr = natExpression.toString().length() < 300 ? natExpression.toString() : natExpression.toString().substring(0, 300);
        String sasnExpr = sasnExpression.toString().length() < 300 ? sasnExpression.toString() : sasnExpression.toString().substring(0, 300);

        String query = "UPDATE cgnat_query_table SET " +
                "nat_expression = '" + natExpr + "', " +
                "sasn_expression = '" + sasnExpr + "', " +
                "final_expression = '" + sparkExpression + "' " +
                "WHERE external_id = '" + queryId + "' " +
                "AND QUERY_TYPE = " + QUERY_TYPE_CGNAT;

        runUpdate(query);
    }

    public static int runUpdate(String sql) throws SQLException {
        int tempCount = 2;
        while (tempCount-- > 0) {
            try {
                BasicDataSource dataSource = DataBaseConnection.getDataSource();
                try (Connection connection = dataSource.getConnection();
                     Statement statement = connection.createStatement()) {
                    int rowsAffected = statement.executeUpdate(sql);
                    System.out.println("Rows Affected : " + rowsAffected);
                    return rowsAffected;
                } catch (Throwable e) {
                    logger.error("Exception while executing query : " + sql, e);
                    throw e;
                }
            } catch (Throwable th) {
                if (tempCount == 0) {
                    throw th;
                }
            }
        }
        throw new RuntimeException("Unknown Exception in executing : " + sql);
    }

    public static void updateQueryExpression(String natExpression, String sasnExpression, String sparkExpression, String queryId) throws SQLException {

        String query = "UPDATE cgnat_query_table SET nat_expression = ?, sasn_expression = ?, final_expression = ? WHERE external_id = '" + queryId + "' AND query_type = " + QUERY_TYPE_CGNAT;

        String natExpr = natExpression.toString().length() < 300 ? natExpression.toString() : natExpression.toString().substring(0, 300);
        String sasnExpr = sasnExpression.toString().length() < 300 ? sasnExpression.toString() : sasnExpression.toString().substring(0, 300);

        executeUpdateQuery(query, new String[]{natExpr, sasnExpr, sparkExpression});
    }

    public static void updateTaskExpression(String natExpression, String sasnExpression, String queryId, String taskId, String ipType) throws SQLException {

        String natExpr = natExpression.toString().length() < 300 ? natExpression.toString() : natExpression.toString().substring(0, 300);
        String sasnExpr = sasnExpression.toString().length() < 300 ? sasnExpression.toString() : sasnExpression.toString().substring(0, 300);

        String query = "UPDATE cgnat_task_table SET " +
                "nat_expression = ? , " +
                "sasn_expression = ? " +
                "WHERE query_id = ? AND " +
                "(" +
                "CASE " +
                "WHEN task_id IS NULL AND ip_type IS NULL THEN 1 " +
                "WHEN task_id = ? AND ip_type = ? THEN 1 " +
                "ELSE 0 " +
                "END" +
                ") = 1";

        executeUpdateQuery(query, new String[]{natExpr, sasnExpr, queryId, taskId, ipType});
    }

    public static void executeUpdateQuery(String query, String[] values) throws SQLException {

        if (countNumOfPlaceholders(query) != values.length) {
            System.out.println("Mismatch between placeholders and values for query : " + query);
            return;
        }

        */
/*String db_url = QueryEngineProperties.CGNAT_QUERYENGINE_DBURL.getValue();
        String db_user = QueryEngineProperties.CGNAT_QUERYENGINE_DBUSERNAME.getValue();
        String db_password = QueryEngineProperties.CGNAT_QUERYENGINE_DBPASSWORD.getValue();
        String db_driver = QueryEngineProperties.CGNAT_QUERYENGINE_DRIVER.getValue();
        Class.forName(db_driver);

        Connection connection = DriverManager.getConnection(db_url, db_user, db_password);

        if (connection != null) {
            System.out.println("connected");
        } else {
            System.out.println("not connected");
        }*//*


        int tempCount = 2;
        while (tempCount-- > 0) {
            try {
                BasicDataSource dataSource = DataBaseConnection.getDataSource();
                try (Connection connection = dataSource.getConnection()) {
                    try (PreparedStatement pst = connection.prepareStatement(query)) {

                        for (int i = 0; i < values.length; i++) {
                            pst.setString(i + 1, values[i]);
                        }

                        int rowsAffected = pst.executeUpdate();
                        System.out.println("Rows Affected : " + rowsAffected);
                        return;
                    }
                } catch (Throwable e) {
                    logger.error("Error while running : " + query, e);
                    throw e;
                }
            } catch (Throwable th) {
                if (tempCount == 0) {
                    throw th;
                }
            }
        }
        //throw new RuntimeException("Unknown Exception in executing : " + query);
    }

    public static long countNumOfPlaceholders(String input) {
        return input.chars().filter(ch -> ch == '?').count();
    }
}

class DataBaseConnection {

    private static final Logger logger = Logger.getLogger(DataBaseConnection.class);

    private static volatile BasicDataSource dataSource;

    public static BasicDataSource getDataSource() {

        if (dataSource == null) {
            synchronized (DataBaseConnection.class) {
                if (dataSource == null) {
                    String db_url = QueryEngineProperties.CGNAT_QUERYENGINE_DBURL.getValue();
                    String db_user = QueryEngineProperties.CGNAT_QUERYENGINE_DBUSERNAME.getValue();
                    String db_password = QueryEngineProperties.CGNAT_QUERYENGINE_DBPASSWORD.getValue();
                    String db_driver = QueryEngineProperties.CGNAT_QUERYENGINE_DRIVER.getValue();

                    logger.info("Getting Database Connection");
                    logger.info("DB_URL:" + db_url + ", USER:" + db_user);
                    BasicDataSource ds = new BasicDataSource();

                    ds.setDriverClassName(db_driver);
                    ds.setUrl(db_url);
                    ds.setUsername(db_user);
                    ds.setPassword(db_password);

                    ds.setMinIdle(30);
                    ds.setMaxIdle(50);
                    ds.setMaxOpenPreparedStatements(100);
                    ds.setTestOnBorrow(true);
                    dataSource = ds;
                }
            }
        }
        return dataSource;
    }
}
*/
