package com.kino.arctic.arcticproduce.mapper.impl;

import com.kino.arctic.arcticproduce.mapper.JdbcBase;
import com.kino.arctic.arcticproduce.modulers.DwdDimItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Service
public class DwdDimItemJdbcImpl extends JdbcBase<DwdDimItem> {

    @Autowired
    private DataSource datasource;

    @Override
    public String genInsertSql(DwdDimItem dwdDimItem) {
        StringBuffer sql = new StringBuffer();
        sql.append("insert into dwd_dim_item values ( ");
        sql.append("0");
        sql.append(",  " + dwdDimItem.getTenantId());
        sql.append(", '" + dwdDimItem.getItemId()   + "'");
        sql.append(", '" + dwdDimItem.getItemCode() + "'");
        sql.append(", '" + dwdDimItem.getBarcode()  + "'");
        sql.append(", '" + dwdDimItem.getItemName() + "'");
        sql.append(", '" + dwdDimItem.getItemClass() + "'");
        sql.append(",  " + dwdDimItem.getCategoryIdLevel1() );
        sql.append(",  " + dwdDimItem.getCategoryIdLevel2() );
        sql.append(",  " + dwdDimItem.getCategoryIdLevel3() );
        sql.append(", '" + dwdDimItem.getSupplier()+"'");
        sql.append(", '" + dwdDimItem.getBrand()+"'");
        sql.append(", '" + dwdDimItem.getSpecification()+"'");
        sql.append(", '" + dwdDimItem.getUnitName()+"'");
        sql.append(", '" + dwdDimItem.getStatus()+"'");
        sql.append(", '" + dwdDimItem.getFreshType()+"'");
        sql.append(", '" + dwdDimItem.getSalesFlag()+"'");
        sql.append(", '" + dwdDimItem.getShelfDate()+"'");
        sql.append(", '" + dwdDimItem.getModifyTime()+"'");
        sql.append(", '" + dwdDimItem.getEnableDate()+"'");
        sql.append(", '" + dwdDimItem.getOutDate()+"'");
        sql.append(", '" + dwdDimItem.getInDate()+"'");
        sql.append(", '" + dwdDimItem.getEnableDate()+"'");
        sql.append(");");
        return sql.toString();
    }

    @Override
    public String genUpdateSql() {
        Connection connection = null;
        try {
            connection = datasource.getConnection();
            DwdDimItem dwdDimItem = super.query(connection, "dwd_dim_item");
            StringBuffer sql = new StringBuffer();
            sql.append("update dwd_dim_item set ");
            sql.append("  item_id = '" + dwdDimItem.getItemId() + "'");
            sql.append(", item_code = '" + dwdDimItem.getItemCode() + "'");
            sql.append(", barcode =   '" + dwdDimItem.getBarcode()  + "'");
            sql.append(", item_name = '" + dwdDimItem.getItemName() + "'");
            sql.append(", item_class = '" + dwdDimItem.getItemClass() + "'");
            sql.append(", category_id_level1 = " + dwdDimItem.getCategoryIdLevel1() );
            sql.append(", category_id_level2 = " + dwdDimItem.getCategoryIdLevel2() );
            sql.append(", category_id_level3 = " + dwdDimItem.getCategoryIdLevel3() );
            sql.append(", supplier = '" + dwdDimItem.getSupplier()+"'");
            sql.append(", brand = '"+dwdDimItem.getBrand()+"'");
            sql.append(", specification = '"+dwdDimItem.getSpecification()+"'");
            sql.append(", unit_name = '"+dwdDimItem.getUnitName()+"'");
            sql.append(", status = '"+dwdDimItem.getStatus()+"'");
            sql.append(", fresh_type = '"+dwdDimItem.getFreshType()+"'");
            sql.append(", sales_flag = '"+ dwdDimItem.getSalesFlag()+"'");
            sql.append(", shelf_date = '"+ dwdDimItem.getShelfDate()+"'");
            sql.append(", modify_time = '"+ dwdDimItem.getModifyTime()+"'");
            sql.append(", enable_date = '"+dwdDimItem.getEnableDate()+"'");
            sql.append(", out_date = '"+dwdDimItem.getOutDate()+"'");
            sql.append(", in_date = '"+dwdDimItem.getInDate()+"'");
            sql.append(", etl_time = '"+dwdDimItem.getEnableDate()+"'");
            sql.append(", tenant_id = " + dwdDimItem.getTenantId());
            sql.append(" where id = " + dwdDimItem.getId());
            return sql.toString();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public String genDeleteSql() {
        Connection connection = null;
        try {
            connection = datasource.getConnection();
            DwdDimItem dwdDimItem = super.query(connection, "dwd_dim_item");
            StringBuffer sql = new StringBuffer();
            sql.append("delete from dwd_dim_item ");
            sql.append(" where id = " + dwdDimItem.getId());
            return sql.toString();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public void executor(String sql) {
        Connection connection = null;
        try {
            connection = datasource.getConnection();
            connection.prepareStatement(sql).execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
