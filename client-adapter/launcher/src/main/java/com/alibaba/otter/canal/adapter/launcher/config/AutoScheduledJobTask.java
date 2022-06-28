package com.alibaba.otter.canal.adapter.launcher.config;

import com.alibaba.otter.canal.adapter.launcher.common.EtlLock;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.ExtensionLoader;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ScheduledFuture;

/**
 * @author ：xcf
 * @date ：2022/6/28 0028
 * ApplicationRunner 项目启动的时候加载  ThreadPoolTaskScheduler 重写spring任务执行器
 */

@Component
public class AutoScheduledJobTask extends ThreadPoolTaskScheduler implements ApplicationRunner {
    private static Logger log = LoggerFactory.getLogger(AutoScheduledJobTask.class);
    private static final String ETL_LOCK_ZK_NODE = "/sync-etl/";
    private ExtensionLoader<OuterAdapter> loader;
    @Resource
    private EtlLock etlLock;

    @Resource
    private AdapterCanalConfig adapterCanalConfig;

    @PostConstruct
    public void init() {
        loader = ExtensionLoader.getExtensionLoader(OuterAdapter.class);
    }

    static {
        log.info("=================>初始化任务");
    }

    public AutoScheduledJobTask() {
        this.setPoolSize(50);
        this.setThreadNamePrefix("AutoScheduledJobTask-");
        this.initialize();
    }

    /**
     * 自定义cron表达式
     **/
    public ScheduledFuture<?> schedule(Runnable task, String expression) {
        return super.schedule(task, new CronTrigger(expression));
    }

    /**
     * 自定义任务开始时间
     **/
    public ScheduledFuture<?> schedule(Runnable task, Date date) {
        return super.schedule(task, date);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        //循环加载launcher项目application.yml配置文件信息(canalAdapters),配置了需要把数据存储在什么地方
        List<String> keys = new ArrayList<>();//如果同时运行多个配置，所以采用list把信息装起来
        List<String> names = new ArrayList<>();
        String key = "";
        String name = "";
        List<CanalClientConfig.CanalAdapter> canalAdapters = adapterCanalConfig.getCanalAdapters();
        for (CanalClientConfig.CanalAdapter canalAdapter : canalAdapters) {
            List<CanalClientConfig.Group> groups = canalAdapter.getGroups();
            for (CanalClientConfig.Group group : groups) {
                List<OuterAdapterConfig> outerAdapterConfigs = group.getOuterAdapters();
                for (OuterAdapterConfig outerAdapterConfig : outerAdapterConfigs) {
                    keys.add(outerAdapterConfig.getKey());
                    names.add(outerAdapterConfig.getName());
                }
            }
        }
        if (names.size() <= 0 && keys.size() <= 0) {
            throw new UnsupportedOperationException("未配置launcher项目application.yml文件canalAdapters信息");
        } else {
            for (int i = 0; i <= keys.size()-1; i++) {
                key = keys.get(i);
                name = names.get(i);
                //初始化外部适配器接口
                OuterAdapter adapter = loader.getExtension(name, key);
                //获取es采集配置文件
                List<String> tasks= adapter.getEsConfigList();
                for (String task:tasks){
                    //获取目标信息
                    String destination = adapter.getDestination(task);
                    String lockKey = destination == null ? task : destination;
                    //获取采集yam的时间表达式
                    String corn=adapter.getCorn(task);
                    String finalName = name;
                    this.schedule(()->{
                        boolean locked = etlLock.tryLock(ETL_LOCK_ZK_NODE + finalName + "-" + lockKey);
                        if (!locked) {
                            throw new UnsupportedOperationException(finalName + " 有其他进程正在导入中, 请稍后再试");
                        }
                        try {
                            try {
                                adapter.etl(task, null);
                            } finally {

                            }
                        } finally {
                            etlLock.unlock(ETL_LOCK_ZK_NODE + finalName + "-" + lockKey);
                        }
                    },corn);
                }

            }
        }
    }
}
