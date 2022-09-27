package com.kino.arctic.arcticproduce.controller;

import com.alibaba.druid.pool.DruidDataSource;
import com.kino.arctic.arcticproduce.JdbcUtils;
import com.kino.arctic.arcticproduce.Utils;
import com.kino.arctic.arcticproduce.mapper.impl.DwdDimItemJdbcImpl;
import com.kino.arctic.arcticproduce.mapper.impl.DwdDimStoreItemJdbcImpl;
import com.kino.arctic.arcticproduce.modulers.DwdDimItem;
import com.kino.arctic.arcticproduce.modulers.DwdDimStoreItem;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@EnableAsync
@RestController
@RequestMapping("/kino")
@Slf4j
public class TimingWriterController {
    public static Logger LOG = LoggerFactory.getLogger(TimingWriterController.class);
    private static final long SLEEPTIME = 10000L;

    @Autowired
    private DataSource datasource;
    @Autowired
    private DwdDimItemJdbcImpl dwdDimItemJdbcImpl;
    @Autowired
    private DwdDimStoreItemJdbcImpl dwdDimStoreItemJdbcImpl;


    @GetMapping("/test")
    public void test(@RequestParam("time") Long time) throws InterruptedException {
        while (true) {
            for (int i = 0; i < 50; i++) {
                // insert
                DwdDimStoreItem dwdDimStoreItem = DwdDimStoreItem.getInstance(Utils.getRandomTime());
                String storeInsertSql = dwdDimStoreItemJdbcImpl.genInsertSql(dwdDimStoreItem);

                DwdDimItem dwdDimItem = DwdDimItem.getInstance(Utils.getRandomTime());
                int randomTime = Utils.getRandomTime();
                if (randomTime % 2 == 0){
                    dwdDimItem.setItemId(dwdDimStoreItem.getItemId());
                    dwdDimItem.setOutDate(dwdDimStoreItem.getOutDate());
                } else {
                    dwdDimItem.setItemId("-9999");
                }
                String itemInsertSql = dwdDimItemJdbcImpl.genInsertSql(dwdDimItem);
                dwdDimStoreItemJdbcImpl.executor(storeInsertSql);
                dwdDimItemJdbcImpl.executor(itemInsertSql);
//                System.out.println("storeInsertSql: " + storeInsertSql);
//                System.out.println("itemInsertSql: " + itemInsertSql);
            }
            Thread.sleep(time);
            for (int i = 0; i < 10; i++) {
                // update
                String itemUpdateSql = dwdDimItemJdbcImpl.genUpdateSql();
                String storeUpdateSql = dwdDimStoreItemJdbcImpl.genUpdateSql();
                dwdDimStoreItemJdbcImpl.executor(itemUpdateSql);
                dwdDimItemJdbcImpl.executor(storeUpdateSql);
            }
            Thread.sleep(time);
            for (int i = 0; i < 5; i++) {
                // delete
                String itemDeleteSql = dwdDimItemJdbcImpl.genDeleteSql();
                String storeDeleteSql = dwdDimStoreItemJdbcImpl.genDeleteSql();
                dwdDimStoreItemJdbcImpl.executor(itemDeleteSql);
                dwdDimItemJdbcImpl.executor(storeDeleteSql);
            }
        }
    }

    @GetMapping("/write")
    public void write(@RequestParam("time") Long time) throws SQLException, InterruptedException {
        Connection connection = datasource.getConnection();
        int count = 0;

        Boolean flag = true;
        while (flag) {
            String sql = null;
            boolean fg = true;
            if(fg) {
                for (int i = 0; i < 50; i++) {
                    DwdDimStoreItem dwdDimStoreItem = DwdDimStoreItem.getInstance(Utils.getRandomTime());
                    sql = dwdDimStoreItem.getSql(1, dwdDimStoreItem, connection);
                    connection.prepareStatement(sql).execute();

                    DwdDimItem dwdDimItem = DwdDimItem.getInstance(Utils.getRandomTime());
                    int randomTime = Utils.getRandomTime();
                    if (randomTime % 2 ==0){
                        dwdDimItem.setItemId(dwdDimStoreItem.getItemId());
                        dwdDimItem.setOutDate(dwdDimStoreItem.getOutDate());
                    } else {
                        dwdDimItem.setItemId("-9999");
                    }
                    sql = dwdDimItem.getSql(1, dwdDimItem, connection);
                    connection.prepareStatement(sql).execute();

                    count = count + 2;
//                    System.out.println("累积操作: "+count + " 次");
                }
                Thread.sleep(time);
                for (int i = 0; i < 10; i++) {
                    DwdDimStoreItem dwdDimStoreItem = DwdDimStoreItem.getInstance(Utils.getRandomTime());
                    dwdDimStoreItem = new DwdDimStoreItem();
                    dwdDimStoreItem.setTenantId(Utils.getRandomTime() + 0L);
                    sql = dwdDimStoreItem.getSql(4, dwdDimStoreItem, connection);
                    connection.prepareStatement(sql).execute();

                    DwdDimItem dwdDimItem = DwdDimItem.getInstance(Utils.getRandomTime());
                    dwdDimItem = new DwdDimItem();
                    dwdDimItem.setTenantId(Utils.getRandomTime() + 0L);
                    sql = dwdDimItem.getSql(4, dwdDimItem, connection);
                    connection.prepareStatement(sql).execute();

                    count = count + 2;
//                    System.out.println("累积操作: "+count + " 次");
                }
                Thread.sleep(time);
                for (int i = 0; i < 10; i++) {
                    DwdDimStoreItem dwdDimStoreItem = DwdDimStoreItem.getInstance(Utils.getRandomTime());
                    sql = dwdDimStoreItem.getSql(3, dwdDimStoreItem, connection);
                    connection.prepareStatement(sql).execute();

                    DwdDimItem dwdDimItem = DwdDimItem.getInstance(Utils.getRandomTime());
                    sql = dwdDimItem.getSql(3, dwdDimItem, connection);
                    connection.prepareStatement(sql).execute();

                    count = count + 2;
//                    System.out.println("累积操作: "+count + " 次");
                }
            }
        }
    }

}
