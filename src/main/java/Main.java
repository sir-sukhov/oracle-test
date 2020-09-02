import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

public class Main {

    final static PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
    final static String DB_USER = "testus";
    final static String DB_PASSWORD = "test";

    public static void main(String args[]) throws InterruptedException, SQLException {

        pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        pds.setONSConfiguration("nodes=192.168.56.78:6200,192.168.56.79:6200");
        pds.setFastConnectionFailoverEnabled(true);
        pds.setURL("jdbc:oracle:thin:@(DESCRIPTION=" +
                "(LOAD_BALANCE=on)" +
                "(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.56.78)(PORT=1521))" +
                "(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.56.79)(PORT=1521))" +
                "(CONNECT_DATA=(service_name=orclpdb)))");
        pds.setUser(DB_USER);
        pds.setPassword(DB_PASSWORD);
        pds.setInitialPoolSize(2);
        pds.setMinPoolSize(2);
        pds.setMaxPoolSize(10);

        while (true) {
            System.out.print("[" + Calendar.getInstance().getTime() + "]: ");
            try (Connection connection = pds.getConnection()) {
                connection.setAutoCommit(true);
                System.out.print("Host " + getConnectionHost(connection));
                putNewMessage(connection);
                System.out.println(": - " + getLastMessage(connection));
                Thread.sleep(1000);
            } catch (NoSuchFieldException | IllegalAccessException | SQLException e) {
                System.out.print(e.getClass().getSimpleName());
                System.out.println(": - Давай досвидания!");
                Thread.sleep(1000);
            }
        }
    }

    public static void putNewMessage(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO MESSAGES VALUES (CURRENT_TIMESTAMP, 'Ты кто такой?')");
        }
    }

    public static String getLastMessage(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement
                    .executeQuery("SELECT * FROM MESSAGES ORDER BY TIMEDATE DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY")) {
                while (resultSet.next())
                    return resultSet.getString(1) + " | " + resultSet.getString(2);
            }
        }
        return null;
    }

    public static String getConnectionHost(Connection connection) throws NoSuchFieldException, IllegalAccessException {
        Class<? extends Object> connectionClass = connection.getClass();
        Field delegateField = connectionClass.getDeclaredField("delegate");
        delegateField.setAccessible(true);
        Object delegate = delegateField.get(connection);

        Class<? extends Object> delegateClass = delegate.getClass();
        Field netField = delegateClass.getDeclaredField("net");
        netField.setAccessible(true);
        Object net = netField.get(delegate);

        Class<? extends Object> netClass = net.getClass().getSuperclass();
        Field sAttsField = netClass.getDeclaredField("sAtts");
        sAttsField.setAccessible(true);
        Object sAtts = sAttsField.get(net);

        Class<? extends Object> sAttsClass = sAtts.getClass();
        Field ntField = sAttsClass.getDeclaredField("nt");
        ntField.setAccessible(true);
        Object nt = ntField.get(sAtts);

        Class<? extends Object> ntClass = nt.getClass();
        Field hostField = ntClass.getDeclaredField("host");
        hostField.setAccessible(true);
        Object host = hostField.get(nt);
        return host.toString();
    }
}