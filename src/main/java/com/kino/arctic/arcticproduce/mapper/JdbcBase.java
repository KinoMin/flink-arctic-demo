package com.kino.arctic.arcticproduce.mapper;

import com.kino.arctic.arcticproduce.JdbcUtils;
import com.kino.arctic.arcticproduce.modulers.DwdDimItem;
import com.kino.arctic.arcticproduce.modulers.DwdDimStoreItem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public abstract class JdbcBase<T> {

    // 获取 insert sql
    public abstract String genInsertSql(T t);

    // 获取 update sql
    public abstract String genUpdateSql();

    // 获取 delete sql
    public abstract String genDeleteSql();

    /**
     * 随机查询一条记录
     **/
    public T query(Connection conn, String table) {
        PreparedStatement pre = null;
        ResultSet res = null;
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
            res = pre.executeQuery();
            //调用将结果集转换成实体对象方法
            List list = null;
            if (table.equals("dwd_dim_store_item")) {
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

    // 执行 sql
    public abstract void executor(String sql);
}
