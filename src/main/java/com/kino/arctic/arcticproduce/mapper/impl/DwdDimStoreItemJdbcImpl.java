package com.kino.arctic.arcticproduce.mapper.impl;

import com.kino.arctic.arcticproduce.mapper.JdbcBase;
import com.kino.arctic.arcticproduce.modulers.DwdDimStoreItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Service
public class DwdDimStoreItemJdbcImpl extends JdbcBase<DwdDimStoreItem> {

    @Autowired
    private DataSource datasource;

    @Override
    public String genInsertSql(DwdDimStoreItem dwdDimStoreItem) {
        StringBuffer sql = new StringBuffer();
        sql.append("insert into dwd_dim_store_item values ( ");
        sql.append(" 0 ");
        sql.append(",  " + dwdDimStoreItem.getTenantId());
        sql.append(", '" + dwdDimStoreItem.getStoreId() + "'");
        sql.append(", '" + dwdDimStoreItem.getShopId() +"'");
        sql.append(", '" + dwdDimStoreItem.getItemId() + "'");
        sql.append(", '" + dwdDimStoreItem.getItemCode() +"'");
        sql.append(", '" + dwdDimStoreItem.getItemName() +"'");
        sql.append(", '" + dwdDimStoreItem.getItemClass() +"'");
        sql.append(",  " + dwdDimStoreItem.getCategoryIdLevel1());
        sql.append(",  " + dwdDimStoreItem.getCategoryIdLevel2());
        sql.append(",  " + dwdDimStoreItem.getCategoryIdLevel3());
        sql.append(",  " + dwdDimStoreItem.getNormalPrice());
        sql.append(",  " + dwdDimStoreItem.getPrice());
        sql.append(",  " + dwdDimStoreItem.getLogistics());
        sql.append(",  " + dwdDimStoreItem.getFlag());
        sql.append(",  " + dwdDimStoreItem.getPoolGoodsType());
        sql.append(", '" + dwdDimStoreItem.getPromotionType() + "'");
        sql.append(", '" + dwdDimStoreItem.getOutDate()+"'");
        sql.append(", '" + dwdDimStoreItem.getGoodDate()+"'");
        sql.append(", '" + dwdDimStoreItem.getStockType()+"'");
        sql.append(", '" + dwdDimStoreItem.getSalesFlag()+"'");
        sql.append(", '" + dwdDimStoreItem.getDcShopId()+"'");
        sql.append(", '" +dwdDimStoreItem.getModifyTime()+"'");
        sql.append(", '" + dwdDimStoreItem.getEtlTime()+"'");
        sql.append(");");
        return sql.toString();
    }

    @Override
    public String genUpdateSql() {
        Connection connection = null;
        try {
            connection = datasource.getConnection();
            DwdDimStoreItem dwdDimStoreItem = super.query(connection, "dwd_dim_store_item");
            StringBuffer sql = new StringBuffer();
            sql.append("update dwd_dim_store_item set ");
            sql.append("  item_code =  '" + dwdDimStoreItem.getItemCode() + "'");
            sql.append(", item_name =  '" + dwdDimStoreItem.getItemName() + "'");
            sql.append(", item_class = '" + dwdDimStoreItem.getItemClass() + "'");
            sql.append(", category_id_level1 =  " + dwdDimStoreItem.getCategoryIdLevel1());
            sql.append(", category_id_level2 =  " + dwdDimStoreItem.getCategoryIdLevel2());
            sql.append(", category_id_level3 =  " + dwdDimStoreItem.getCategoryIdLevel3());
            sql.append(", normal_price = " + dwdDimStoreItem.getNormalPrice());
            sql.append(", price = " + dwdDimStoreItem.getPrice());
            sql.append(", logistics = " + dwdDimStoreItem.getLogistics());
            sql.append(", flag = " + dwdDimStoreItem.getFlag());
            sql.append(", pool_goods_type = " + dwdDimStoreItem.getPoolGoodsType());
            sql.append(", promotion_type = '" + dwdDimStoreItem.getPromotionType() + "'");
            sql.append(", out_date = '" + dwdDimStoreItem.getOutDate() + "'");
            sql.append(", good_date = '" + dwdDimStoreItem.getGoodDate() + "'");
            sql.append(", stock_type = '" + dwdDimStoreItem.getStockType() + "'");
            sql.append(", sales_flag = '" + dwdDimStoreItem.getSalesFlag() + "'");
            sql.append(", dc_shop_id = '" + dwdDimStoreItem.getDcShopId() + "'");
            sql.append(", modify_time = '" + dwdDimStoreItem.getModifyTime() + "'");
            sql.append(", etl_time = '" + dwdDimStoreItem.getEtlTime() + "'");
            sql.append(", tenant_id = " + dwdDimStoreItem.getTenantId());
            sql.append(", store_id = '" + dwdDimStoreItem.getStoreId() + "'");
            sql.append(", shop_id = '" + dwdDimStoreItem.getShopId() + "'");
            sql.append(", item_id = '" + dwdDimStoreItem.getItemId() + "'");
            sql.append(" where id = " + dwdDimStoreItem.getId());
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
            DwdDimStoreItem dwdDimStoreItem = super.query(connection, "dwd_dim_store_item");
            StringBuffer sql = new StringBuffer();
            sql.append("delete from dwd_dim_store_item ");
            sql.append(" where id = " + dwdDimStoreItem.getId());
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
