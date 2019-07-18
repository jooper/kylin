package org.apache.kylin.sdk.datasource.adaptor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class OracleAdaptor extends DefaultAdaptor {
    public OracleAdaptor(AdaptorConfig config) throws Exception {
        super(config);
    }


    public static void main(String[] args) {

        Connection con = null;// 创建一个数据库连接
        PreparedStatement pre = null;// 创建预编译语句对象，一般都是用这个而不用Statement
        ResultSet result = null;// 创建一个结果集对象
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");// 加载Oracle驱动程序
            System.out.println("开始尝试连接数据库！");
            String url = "jdbc:oracle:thin:@//192.168.12.202:1521/XTHIS";// 127.0.0.1是本机地址，XE是精简版Oracle的默认数据库名
            String user = "emrhis_sc";// 用户名,系统默认的账户名
            String password = "123456";// 你安装时选设置的密码
            con = DriverManager.getConnection(url, user, password);// 获取连接
            System.out.println("连接成功！");

            pre.setInt(1, 40);// 设置参数，前面的1表示上面预编译语句中的?参数的索引，
            pre.setString(2, "NEW%");


        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }
}
