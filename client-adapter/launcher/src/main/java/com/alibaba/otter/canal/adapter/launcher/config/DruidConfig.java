package com.alibaba.otter.canal.adapter.launcher.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.alibaba.druid.support.http.StatViewServlet;

@Configuration
public class DruidConfig {

    /**
     * 配置Druid统计视图Servlet的Bean
     * 此方法用于注册StatViewServlet，以监控Druid数据源的信息
     * @return 返回配置好的ServletRegistrationBean对象
     */
    @Bean
    public ServletRegistrationBean<StatViewServlet> statViewServlet(){
        // 创建并初始化ServletRegistrationBean，映射StatViewServlet到/druid/*路径
        ServletRegistrationBean<StatViewServlet> bean = new ServletRegistrationBean<>( new StatViewServlet(),"/druid/*");

        // 设置初始化参数，允许所有IP访问统计视图
        Map<String,String> initParams = new HashMap<>();
        initParams.put("allow","");
        bean.setInitParameters(initParams);

        // 返回配置好的Bean
        return  bean;
    }
}
