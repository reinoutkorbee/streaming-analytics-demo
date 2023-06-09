package com.snowice23;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

// https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure
public class TestJdbc {

    /*
     * There's an incompatibility issue with Apache Arrow and the Java SDK > 1.8:
     * https://arrow.apache.org/docs/java/install.html
     *
     * If you get the error:
     *
     * Caused by: java.lang.reflect.InaccessibleObjectException: Unable to make field long java.nio.Buffer.address accessible: module java.base does not "opens java.nio" to unnamed module
     *
     *
     * Then modify the run configuration in IntelliJ and add the following to the environment variables:
     *
     * _JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED"
     *
     *
     */
    public static void main(String[] args)
            throws Exception {
        Connection conn = ConnectionHelper.getConnection();
        Statement stat = conn.createStatement();
        ResultSet res = stat.executeQuery("select 1");
        res.next();
        System.out.println(res.getString(1));
        conn.close();
    }
}