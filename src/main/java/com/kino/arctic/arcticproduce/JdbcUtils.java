package com.kino.arctic.arcticproduce;

import com.kino.arctic.arcticproduce.modulers.DwdDimItem;
import com.kino.arctic.arcticproduce.modulers.DwdDimStoreItem;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtils<T> {

    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://192.168.1.80:12360/flink?useSSL=false";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";

    public static Connection connection = null;
    public static PreparedStatement preparedStatement = null;
    public static ResultSet resultSet = null;

    public static void main(String[] args) throws SQLException {
        Connection conn = JdbcUtils.getConnections();
        PreparedStatement pre = null;
        ResultSet res = null;
        String sql = "select * from dwd_dim_store_item where tenant_id in (?,?)";
        try {
            pre = conn.prepareStatement(sql);
            pre.setInt(1,108000);
            pre.setInt(2,108001);
            res = pre.executeQuery();
            //调用将结果集转换成实体对象方法
            List list = JdbcUtils.Populate(res, DwdDimStoreItem.class);
            //循环遍历结果
            for(int i=0;i<list.size();i++){
                DwdDimStoreItem dwdDimStoreItem = (DwdDimStoreItem) list.get(i);
                System.out.println(dwdDimStoreItem);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public T getOne(Connection conn, Long id, String table, String tableType) throws SQLException {
        PreparedStatement pre = null;
        ResultSet res = null;
        //String sql = "select * from "+table+" where tenant_id = ?";
        StringBuffer sql = new StringBuffer();
        sql.append("SELECT");
        sql.append(" t1.* ");
        sql.append(" FROM ");
        sql.append(table + " AS t1");
        sql.append(" JOIN ( SELECT ROUND( RAND() * ( SELECT MAX( tenant_id ) FROM " + table + " )) AS tenant_id ) AS t2 ");
        sql.append(" WHERE ");
        sql.append(" t1.tenant_id >= t2.tenant_id ");
        sql.append(" ORDER BY t1.tenant_id ASC LIMIT 1;");
        try {
            pre = conn.prepareStatement(sql.toString());
            //pre.setLong(1,id);
            res = pre.executeQuery();
            //调用将结果集转换成实体对象方法
            List list = null;
            if (tableType.equals("DwdDimStoreItem")) {
                list = JdbcUtils.Populate(res, DwdDimStoreItem.class);
            } else {
                list = JdbcUtils.Populate(res, DwdDimItem.class);
            }
            //循环遍历结果
            if(list.size() >= 1) {
                T t = (T) list.get(0);
                return t;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Connection getConnections() throws SQLException {
        try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void executor(String sql) {
        try {
            //Thread.sleep(2000);
            if (sql != null && !sql.equals("")) {
                connection = getConnections();
                Statement statement = connection.createStatement();
                statement.execute(sql);
                close();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static List Populate(ResultSet rs, Class cc) throws SQLException, InstantiationException, IllegalAccessException{
        //结果集 中列的名称和类型的信息
        ResultSetMetaData rsm = rs.getMetaData();
        int colNumber = rsm.getColumnCount();
        List list = new ArrayList();
        Field[] fields = cc.getDeclaredFields();

        //遍历每条记录
        while(rs.next()){
            //实例化对象
            Object obj = cc.newInstance();
            //取出每一个字段进行赋值
            for(int i=1;i<=colNumber;i++){
                Object value = rs.getObject(i);
                //匹配实体类中对应的属性
                for(int j = 0;j<fields.length;j++){
                    Field f = fields[j];
                    if(f.getName().equals(Utils.underlineToHump(rsm.getColumnName(i)))){
                        boolean flag = f.isAccessible();
                        f.setAccessible(true);
                        if (value instanceof Timestamp || value instanceof Date) {
                            f.set(obj, value.toString());
                        } else {
                            f.set(obj, value);
                        }
                        f.setAccessible(flag);
                        break;
                    }
                }

            }
            list.add(obj);
        }
        return list;
    }

    public static void close(){
        try{
            if(connection != null){
                connection.close();
                connection = null;
            }
            if(preparedStatement != null){
                preparedStatement.close();
                preparedStatement = null;
            }
            if(resultSet!= null){
                resultSet.close();
                resultSet = null;
            }
        }catch(SQLException e){
            e.printStackTrace();
        }

    }

}
