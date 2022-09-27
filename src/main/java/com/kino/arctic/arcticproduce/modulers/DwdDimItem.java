package com.kino.arctic.arcticproduce.modulers;

import com.kino.arctic.arcticproduce.JdbcUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class DwdDimItem {
    private int id;
    private Long tenantId;
    private String itemId;
    private String itemCode;
    private String barcode;
    private String itemName;
    private String itemClass;
    private Long categoryIdLevel1;
    private Long categoryIdLevel2;
    private Long categoryIdLevel3;
    private String supplier;
    private String brand;
    private String specification;
    private String unitName;
    private String status;
    private Integer freshType;
    private String salesFlag;
    private String shelfDate;
    private String modifyTime;
    private String enableDate;
    private String outDate;
    private String inDate;
    private String etlTime;

    public DwdDimItem() {
    }

    public DwdDimItem(int id, Long tenantId, String itemId, String itemCode, String barcode, String itemName, String itemClass, Long categoryIdLevel1, Long categoryIdLevel2, Long categoryIdLevel3, String supplier, String brand, String specification, String unitName, String status, Integer freshType, String salesFlag, String shelfDate, String modifyTime, String enableDate, String outDate, String inDate, String etlTime) {
        this.id = id;
        this.tenantId = tenantId;
        this.itemId = itemId;
        this.itemCode = itemCode;
        this.barcode = barcode;
        this.itemName = itemName;
        this.itemClass = itemClass;
        this.categoryIdLevel1 = categoryIdLevel1;
        this.categoryIdLevel2 = categoryIdLevel2;
        this.categoryIdLevel3 = categoryIdLevel3;
        this.supplier = supplier;
        this.brand = brand;
        this.specification = specification;
        this.unitName = unitName;
        this.status = status;
        this.freshType = freshType;
        this.salesFlag = salesFlag;
        this.shelfDate = shelfDate;
        this.modifyTime = modifyTime;
        this.enableDate = enableDate;
        this.outDate = outDate;
        this.inDate = inDate;
        this.etlTime = etlTime;
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

    public String getBarcode() {
        return barcode;
    }

    public void setBarcode(String barcode) {
        this.barcode = barcode;
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

    public String getSupplier() {
        return supplier;
    }

    public void setSupplier(String supplier) {
        this.supplier = supplier;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getSpecification() {
        return specification;
    }

    public void setSpecification(String specification) {
        this.specification = specification;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getFreshType() {
        return freshType;
    }

    public void setFreshType(Integer freshType) {
        this.freshType = freshType;
    }

    public String getSalesFlag() {
        return salesFlag;
    }

    public void setSalesFlag(String salesFlag) {
        this.salesFlag = salesFlag;
    }

    public String getShelfDate() {
        return shelfDate;
    }

    public void setShelfDate(String shelfDate) {
        this.shelfDate = shelfDate;
    }

    public String getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(String modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getEnableDate() {
        return enableDate;
    }

    public void setEnableDate(String enableDate) {
        this.enableDate = enableDate;
    }

    public String getOutDate() {
        return outDate;
    }

    public void setOutDate(String outDate) {
        this.outDate = outDate;
    }

    public String getInDate() {
        return inDate;
    }

    public void setInDate(String inDate) {
        this.inDate = inDate;
    }

    public String getEtlTime() {
        return etlTime;
    }

    public void setEtlTime(String etlTime) {
        this.etlTime = etlTime;
    }

    @Override
    public String toString() {
        return "DwdDimItem{" +
                "id=" + id +
                ", tenantId=" + tenantId +
                ", itemId='" + itemId + '\'' +
                ", itemCode='" + itemCode + '\'' +
                ", barcode='" + barcode + '\'' +
                ", itemName='" + itemName + '\'' +
                ", itemClass='" + itemClass + '\'' +
                ", categoryIdLevel1=" + categoryIdLevel1 +
                ", categoryIdLevel2=" + categoryIdLevel2 +
                ", categoryIdLevel3=" + categoryIdLevel3 +
                ", supplier='" + supplier + '\'' +
                ", brand='" + brand + '\'' +
                ", specification='" + specification + '\'' +
                ", unitName='" + unitName + '\'' +
                ", status='" + status + '\'' +
                ", freshType=" + freshType +
                ", salesFlag='" + salesFlag + '\'' +
                ", shelfDate='" + shelfDate + '\'' +
                ", modifyTime='" + modifyTime + '\'' +
                ", enableDate='" + enableDate + '\'' +
                ", outDate='" + outDate + '\'' +
                ", inDate='" + inDate + '\'' +
                ", etlTime='" + etlTime + '\'' +
                '}';
    }

    public static DwdDimItem getInstance(int time) {
        DwdDimItem dwdDimItem = new DwdDimItem();
        dwdDimItem.setId(0);
        dwdDimItem.setTenantId(time + 0L);
        dwdDimItem.setItemId("item_id_" + time);
        dwdDimItem.setItemCode("item_code_" + time);
        dwdDimItem.setBarcode("barcode_" + time);
        dwdDimItem.setItemName("item_name_" + time);
        dwdDimItem.setItemClass("item_class_" + time);
        dwdDimItem.setCategoryIdLevel1(time + 0L);
        dwdDimItem.setCategoryIdLevel2(time + 0L);
        dwdDimItem.setCategoryIdLevel3(time + 0L);
        dwdDimItem.setSupplier("supplier_" + time);
        dwdDimItem.setBrand("brand_" + time);
        dwdDimItem.setSpecification("specification_" + time);
        dwdDimItem.setUnitName("unit_name_" + time);
        dwdDimItem.setStatus("status_" + time);
        dwdDimItem.setFreshType(time);
        dwdDimItem.setSalesFlag("sales_flag_" + time);
        dwdDimItem.setShelfDate(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
        dwdDimItem.setModifyTime(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
        dwdDimItem.setEnableDate(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
        dwdDimItem.setOutDate(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
        dwdDimItem.setInDate(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
        dwdDimItem.setEtlTime(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
        return dwdDimItem;
    }

    /**
     * type
     * 1: insert
     * 2: update
     * 3: delete
     * 4: select
     * @param type
     * @return
     */
    public static String getSql(int type, DwdDimItem dwdDimItem, Connection connection) throws SQLException {
        String sql = null;
        switch (type) {
            case 1:
                sql = genInsertSql(dwdDimItem, connection);
                break;
            case 2:
                sql = genUpdateSql(dwdDimItem, connection);
                break;
            case 3:
                sql = genDeleteSql(dwdDimItem, connection);
                break;
            case 4:
                sql = genSelectSql(dwdDimItem, connection);
                break;
            default:
                System.out.println("break.....");
        }
        return sql;
    }

    private static String genInsertSql(DwdDimItem dwdDimItem, Connection connection) {
        StringBuffer sql = new StringBuffer();
        sql.append("insert into dwd_dim_item values ( ");
        sql.append("0");
        sql.append(",  "+dwdDimItem.tenantId);
        sql.append(", '" + dwdDimItem.getItemId()   + "'");
        sql.append(", '" + dwdDimItem.getItemCode() + "'");
        sql.append(", '" + dwdDimItem.getBarcode()  + "'");
        sql.append(", '" + dwdDimItem.getItemName() + "'");
        sql.append(", '" + dwdDimItem.getItemClass() + "'");
        sql.append(",  " + dwdDimItem.getCategoryIdLevel1() );
        sql.append(",  " + dwdDimItem.getCategoryIdLevel2() );
        sql.append(",  " + dwdDimItem.getCategoryIdLevel3() );
        sql.append(", '" + dwdDimItem.getSupplier()+"'");
        sql.append(", '"+dwdDimItem.getBrand()+"'");
        sql.append(", '"+dwdDimItem.getSpecification()+"'");
        sql.append(", '"+dwdDimItem.getUnitName()+"'");
        sql.append(", '"+dwdDimItem.getStatus()+"'");
        sql.append(", '"+dwdDimItem.getFreshType()+"'");
        sql.append(", '"+ dwdDimItem.getSalesFlag()+"'");
        sql.append(", '"+ dwdDimItem.getShelfDate()+"'");
        sql.append(", '"+ dwdDimItem.getModifyTime()+"'");
        sql.append(", '"+dwdDimItem.getEnableDate()+"'");
        sql.append(", '"+dwdDimItem.outDate+"'");
        sql.append(", '"+dwdDimItem.getInDate()+"'");
        sql.append(", '"+dwdDimItem.getEnableDate()+"'");
        sql.append(");");
        return sql.toString();
    }

    private static String genUpdateSql(DwdDimItem dwdDimItem, Connection connection) {
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
        sql.append(", out_date = '"+dwdDimItem.outDate+"'");
        sql.append(", in_date = '"+dwdDimItem.getInDate()+"'");
        sql.append(", etl_time = '"+dwdDimItem.getEnableDate()+"'");
        sql.append(", tenant_id = " + dwdDimItem.getTenantId());
        sql.append(" where id = " + dwdDimItem.getId());
        return sql.toString();
    }

    private static String genDeleteSql(DwdDimItem dwdDimItem, Connection connection) throws SQLException {
        JdbcUtils<DwdDimItem> jdbcUtils = new JdbcUtils();
        DwdDimItem one = (DwdDimItem) jdbcUtils.getOne(connection, null, "dwd_dim_item", "DwdDimItem");
        StringBuffer sql = new StringBuffer();
        sql.append("delete from dwd_dim_item ");
        sql.append(" where id = " + one.getId());
        System.out.println("==> delete sql: "+sql.toString());
        return sql.toString();
    }

    private static String genSelectSql(DwdDimItem dwdDimItem, Connection connection) throws SQLException {
        Long tenantId = dwdDimItem.getTenantId();
        JdbcUtils<DwdDimItem> jdbcUtils = new JdbcUtils();
        DwdDimItem one = (DwdDimItem) jdbcUtils.getOne(connection, tenantId, "dwd_dim_item", "DwdDimItem");
        if(null != one){
            one.setItemName("iten_name_DwdDimItem_" + DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format(LocalDateTime.now()));
            return getSql(2, one, connection);
        } else {
            return getSql(3, one, connection);
        }
    }

}
