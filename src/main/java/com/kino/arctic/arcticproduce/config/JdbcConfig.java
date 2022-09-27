package com.kino.arctic.arcticproduce.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;

@Configuration
@PropertySource("classpath:jdbc.properties")
public class JdbcConfig {
    @Value("${jdbc.url}")
    private String url;
    @Value("${jdbc.driver}")
    private String driver;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String passWord;

    @Bean
    public DataSource getDataSource(){
        DruidDataSource dataSource = new DruidDataSource();
        try {
            dataSource.setDriverClassName(driver);
            dataSource.setUrl(url);
            dataSource.setUsername(userName);
            dataSource.setPassword(passWord);
            dataSource.setInitialSize(1);
            dataSource.setMinIdle(1);
            dataSource.setMaxActive(100);
            dataSource.setMaxWait(30000);
            dataSource.setTimeBetweenEvictionRunsMillis(60000);
            dataSource.setMinEvictableIdleTimeMillis(300000);
            dataSource.setTestWhileIdle(true);
            dataSource.setTestOnBorrow(false);
            dataSource.setTestOnReturn(false);
            dataSource.setKeepAlive(true);
            dataSource.setPoolPreparedStatements(false);
            dataSource.setConnectionInitSqls(Collections.singletonList("set names 'utf8'"));
            dataSource.setRemoveAbandoned(false);
            dataSource.setLogAbandoned(true);
            dataSource.setTimeBetweenConnectErrorMillis(60000);
            dataSource.setConnectionErrorRetryAttempts(3);
            dataSource.init();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return dataSource;
    }
}
