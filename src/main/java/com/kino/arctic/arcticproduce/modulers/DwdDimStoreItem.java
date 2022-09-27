package com.kino.arctic.arcticproduce.modulers;

import com.kino.arctic.arcticproduce.JdbcUtils;
import com.kino.arctic.arcticproduce.Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DwdDimStoreItem {

    private int id;
    private Long tenantId;
    private String storeId;
    private String shopId;
    private String itemId;
    private String itemCode;
    private String itemName;
    private String itemClass;
    private Long categoryIdLevel1;
    private Long categoryIdLevel2;
    private Long categoryIdLevel3;
    private Double normalPrice;
    private Double price;
    private Double logistics;
    private Double flag;
    private Integer poolGoodsType;
    private String promotionType;
    private String outDate;
    private String goodDate;
    private String stockType;
    private String salesFlag;
    private String dcShopId;
    private String modifyTime;
    private String etlTime;

    public DwdDimStoreItem(int id, Long tenantId, String storeId, String shopId, String itemId, String itemCode, String itemName, String itemClass, Long categoryIdLevel1, Long categoryIdLevel2, Long categoryIdLevel3, Double normalPrice, Double price, Double logistics, Double flag, Integer poolGoodsType, String promotionType, String outDate, String goodDate, String stockType, String salesFlag, String dcShopId, String modifyTime, String etlTime) {
        this.id = id;
        this.tenantId = tenantId;
        this.storeId = storeId;
        this.shopId = shopId;
        this.itemId = itemId;
        this.itemCode = itemCode;
        this.itemName = itemName;
        this.itemClass = itemClass;
        this.categoryIdLevel1 = categoryIdLevel1;
        this.categoryIdLevel2 = categoryIdLevel2;
        this.categoryIdLevel3 = categoryIdLevel3;
        this.normalPrice = normalPrice;
        this.price = price;
        this.logistics = logistics;
        this.flag = flag;
        this.poolGoodsType = poolGoodsType;
        this.promotionType = promotionType;
        this.outDate = outDate;
        this.goodDate = goodDate;
        this.stockType = stockType;
        this.salesFlag = salesFlag;
        this.dcShopId = dcShopId;
        this.modifyTime = modifyTime;
        this.etlTime = etlTime;
    }

    public DwdDimStoreItem() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public String getStoreId() {
        return storeId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public String getShopId() {
        return shopId;
    }

    public void setShopId(String shopId) {
        this.shopId = shopId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getItemCode() {
        return itemCode;
    }

    public void setItemCode(String itemCode) {
        this.itemCode = itemCode;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public String getItemClass() {
        return itemClass;
    }

    public void setItemClass(String itemClass) {
        this.itemClass = itemClass;
    }

    public Long getCategoryIdLevel1() {
        return categoryIdLevel1;
    }

    public void setCategoryIdLevel1(Long categoryIdLevel1) {
        this.categoryIdLevel1 = categoryIdLevel1;
    }

    public Long getCategoryIdLevel2() {
        return categoryIdLevel2;
    }

    public void setCategoryIdLevel2(Long categoryIdLevel2) {
        this.categoryIdLevel2 = categoryIdLevel2;
    }

    public Long getCategoryIdLevel3() {
        return categoryIdLevel3;
    }

    public void setCategoryIdLevel3(Long categoryIdLevel3) {
        this.categoryIdLevel3 = categoryIdLevel3;
    }

    public Double getNormalPrice() {
        return normalPrice;
    }

    public void setNormalPrice(Double normalPrice) {
        this.normalPrice = normalPrice;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Double getLogistics() {
        return logistics;
    }

    public void setLogistics(Double logistics) {
        this.logistics = logistics;
    }

    public Double getFlag() {
        return flag;
    }

    public void setFlag(Double flag) {
        this.flag = flag;
    }

    public Integer getPoolGoodsType() {
        return poolGoodsType;
    }

    public void setPoolGoodsType(Integer poolGoodsType) {
        this.poolGoodsType = poolGoodsType;
    }

    public String getPromotionType() {
        return promotionType;
    }

    public void setPromotionType(String promotionType) {
        this.promotionType = promotionType;
    }

    public String getOutDate() {
        return outDate;
    }

    public void setOutDate(String outDate) {
        this.outDate = outDate;
    }

    public String getGoodDate() {
        return goodDate;
    }

    public void setGoodDate(String goodDate) {
        this.goodDate = goodDate;
    }

    public String getStockType() {
        return stockType;
    }

    public void setStockType(String stockType) {
        this.stockType = stockType;
    }

    public String getSalesFlag() {
        return salesFlag;
    }

    public void setSalesFlag(String salesFlag) {
        this.salesFlag = salesFlag;
    }

    public String getDcShopId() {
        return dcShopId;
    }

    public void setDcShopId(String dcShopId) {
        this.dcShopId = dcShopId;
    }

    public String getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(String modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getEtlTime() {
        return etlTime;
    }

    public void setEtlTime(String etlTime) {
        this.etlTime = etlTime;
    }

    @Override
    public String toString() {
        return "DwdDimStoreItem{" +
                "id=" + id +
                ", tenantId=" + tenantId +
                ", storeId='" + storeId + '\'' +
                ", shopId='" + shopId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", itemCode='" + itemCode + '\'' +
                ", itemName='" + itemName + '\'' +
                ", itemClass='" + itemClass + '\'' +
                ", categoryIdLevel1=" + categoryIdLevel1 +
                ", categoryIdLevel2=" + categoryIdLevel2 +
                ", categoryIdLevel3=" + categoryIdLevel3 +
                ", normalPrice=" + normalPrice +
                ", price=" + price +
                ", logistics=" + logistics +
                ", flag=" + flag +
                ", poolGoodsType=" + poolGoodsType +
                ", promotionType='" + promotionType + '\'' +
                ", outDate='" + outDate + '\'' +
                ", goodDate='" + goodDate + '\'' +
                ", stockType='" + stockType + '\'' +
                ", salesFlag='" + salesFlag + '\'' +
                ", dcShopId='" + dcShopId + '\'' +
                ", modifyTime='" + modifyTime + '\'' +
                ", etlTime='" + etlTime + '\'' +
                '}';
    }

    public static DwdDimStoreItem getInstance(int time){
        DwdDimStoreItem dwdDimStoreItem = new DwdDimStoreItem();
        dwdDimStoreItem.setId(0);
        dwdDimStoreItem.setTenantId(time + 0L);
        dwdDimStoreItem.setStoreId("store_id_" + time);
        dwdDimStoreItem.setShopId("shop_id_" + time);
        dwdDimStoreItem.setItemId("item_id_" + time);
        dwdDimStoreItem.setItemCode("item_code_" + time);
        dwdDimStoreItem.setItemName("item_name_" + time);
        dwdDimStoreItem.setItemClass("item_class_" + time);
        dwdDimStoreItem.setCategoryIdLevel1(time + 0L);
        dwdDimStoreItem.setCategoryIdLevel2(time + 0L);
        dwdDimStoreItem.setCategoryIdLevel3(time + 0L);
        dwdDimStoreItem.setNormalPrice(Utils.getRandomDouble());
        dwdDimStoreItem.setPrice(Utils.getRandomDouble());
        dwdDimStoreItem.setLogistics(Utils.getRandomDouble());
        dwdDimStoreItem.setFlag(Utils.getRandomDouble());
        dwdDimStoreItem.setPoolGoodsType(Utils.getRandomTime());
        dwdDimStoreItem.setPromotionType("promotion_type_" + time);
        dwdDimStoreItem.setOutDate(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
        dwdDimStoreItem.setGoodDate(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
        dwdDimStoreItem.setStockType("stock_type_" + time);
        dwdDimStoreItem.setSalesFlag("sales_flag_" + time);
        dwdDimStoreItem.setDcShopId("dc_shop_id_" + time);
        dwdDimStoreItem.setModifyTime(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
        dwdDimStoreItem.setEtlTime(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
        return dwdDimStoreItem;
    }

    /**
     * type
     * 1: insert
     * 2: update
     * 3: delete
     *
     * @param type
     * @return
     */
    public static String getSql(int type, DwdDimStoreItem dwdDimStoreItem, Connection connection) throws SQLException {
        String sql = null;
        switch (type) {
            case 1:
                sql = genInsertSql(dwdDimStoreItem);
                break;
            case 2:
                sql = genUpdateSql(dwdDimStoreItem);
                break;
            case 3:
                sql = genDeleteSql(dwdDimStoreItem, connection);
                break;
            case 4:
                sql = genSelectSql(dwdDimStoreItem, connection);
                break;
            default:
                System.out.println("break.....");
        }
        return sql;
    }

    private static String genInsertSql(DwdDimStoreItem dwdDimStoreItem) {
        StringBuffer sql = new StringBuffer();
        sql.append("insert into dwd_dim_store_item values ( ");
        sql.append(" 0 ");
        sql.append(",  " + dwdDimStoreItem.getTenantId());
        sql.append(", '" + dwdDimStoreItem.getStoreId() + "'");
        sql.append(", '" + dwdDimStoreItem.getShopId() +"'");
        sql.append(", '" + dwdDimStoreItem.getItemId() + "'");
        sql.append(", '" + dwdDimStoreItem.itemCode +"'");
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
    private static String genUpdateSql(DwdDimStoreItem dwdDimStoreItem) {
        StringBuffer sql = new StringBuffer();
        sql.append("update dwd_dim_store_item set ");
        sql.append("  item_code =  '"+ dwdDimStoreItem.itemCode +"'");
        sql.append(", item_name =  '"+ dwdDimStoreItem.getItemName() +"'");
        sql.append(", item_class = '"+ dwdDimStoreItem.getItemClass() +"'");
        sql.append(", category_id_level1 =  " + dwdDimStoreItem.getCategoryIdLevel1());
        sql.append(", category_id_level2 =  " + dwdDimStoreItem.getCategoryIdLevel2());
        sql.append(", category_id_level3 =  " + dwdDimStoreItem.getCategoryIdLevel3());
        sql.append(", normal_price = " + dwdDimStoreItem.getNormalPrice());
        sql.append(", price = " + dwdDimStoreItem.getPrice());
        sql.append(", logistics = " + dwdDimStoreItem.getLogistics());
        sql.append(", flag = " + dwdDimStoreItem.getFlag());
        sql.append(", pool_goods_type = " + dwdDimStoreItem.getPoolGoodsType());
        sql.append(", promotion_type = '" + dwdDimStoreItem.getPromotionType() + "'");
        sql.append(", out_date = '" + dwdDimStoreItem.getOutDate()+"'");
        sql.append(", good_date = '" + dwdDimStoreItem.getGoodDate()+"'");
        sql.append(", stock_type = '" + dwdDimStoreItem.getStockType()+"'");
        sql.append(", sales_flag = '" + dwdDimStoreItem.getSalesFlag()+"'");
        sql.append(", dc_shop_id = '" + dwdDimStoreItem.getDcShopId()+"'");
        sql.append(", modify_time = '" +dwdDimStoreItem.getModifyTime()+"'");
        sql.append(", etl_time = '" + dwdDimStoreItem.getEtlTime()+"'");
        sql.append(", tenant_id = " + dwdDimStoreItem.getTenantId());
        sql.append(", store_id = '" + dwdDimStoreItem.getStoreId() + "'");
        sql.append(", shop_id = '" + dwdDimStoreItem.getShopId() + "'");
        sql.append(", item_id = '" + dwdDimStoreItem.getItemId() + "'");
        sql.append(" where id = " + dwdDimStoreItem.getId());
        return sql.toString();
    }
    private static String genDeleteSql(DwdDimStoreItem dwdDimStoreItem, Connection connection) throws SQLException {
        JdbcUtils<DwdDimStoreItem> jdbcUtils = new JdbcUtils();
        DwdDimStoreItem one = (DwdDimStoreItem) jdbcUtils.getOne(connection, null, "dwd_dim_store_item", "DwdDimStoreItem");
        StringBuffer sql = new StringBuffer();
        sql.append("delete from dwd_dim_store_item ");
        sql.append(" where id = " + one.getId());
        System.out.println("--> delete sql: "+sql.toString());
        return sql.toString();
    }

    private static String genSelectSql(DwdDimStoreItem dwdDimStoreItem, Connection connection) throws SQLException {
        Long tenantId = dwdDimStoreItem.getTenantId();
        JdbcUtils<DwdDimStoreItem> jdbcUtils = new JdbcUtils();
        DwdDimStoreItem one = (DwdDimStoreItem) jdbcUtils.getOne(connection, tenantId, "dwd_dim_store_item", "DwdDimStoreItem");
        if(null != one){
            one.setItemName("iten_name_DwdDimStoreItem_" + DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
            return getSql(2, one, connection);
        } else {
            return getSql(3, one, connection);
        }
    }
}
