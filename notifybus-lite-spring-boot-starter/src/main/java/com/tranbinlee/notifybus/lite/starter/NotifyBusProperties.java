package com.tranbinlee.notifybus.lite.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * {@code notifybus.lite.*} 配置项。
 */
@ConfigurationProperties(prefix = "notifybus.lite")
public class NotifyBusProperties {

    /**
     * 是否启用 NotifyBus Lite 自动装配。默认 {@code false}——引入 starter 依赖本身不会让
     * {@code NotifyBusTransport}/{@code NotifyBusPublisher}/{@code NotifyBusConsumer} 生效，
     * 必须显式配置 {@code notifybus.lite.enabled=true} 才会装配。
     */
    private boolean enabled = false;

    /**
     * 订阅隔离用的根路径/前缀，默认 {@code /notifybus}。ZooKeeper transport 直接用作 znode 根路径；
     * Nacos transport 用作 dataId/group 前缀（非法字符会被替换成 {@code _}，见
     * {@code NacosPaths#sanitize}）。和 {@link Nacos#namespace} 不是一回事——那个是 Nacos 自身的租户
     * 隔离概念（{@code PropertyKeyConst.NAMESPACE}），这里的 {@code namespace} 是 NotifyBus 自己的
     * 命名空间隔离，两者可以同时使用、互不影响。
     */
    private String namespace = "/notifybus";

    /**
     * 具体使用哪个 transport 实现，默认 {@link TransportType#ZOOKEEPER}。当前仓库还没有实现
     * {@link TransportType#DB}，配置成它会在启动时直接失败，而不是静默退化成 zookeeper。
     */
    private TransportType type = TransportType.ZOOKEEPER;

    private final Zk zk = new Zk();
    private final Nacos nacos = new Nacos();
    private final Dispatch dispatch = new Dispatch();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public TransportType getType() {
        return type;
    }

    public void setType(TransportType type) {
        this.type = type;
    }

    public Zk getZk() {
        return zk;
    }

    public Nacos getNacos() {
        return nacos;
    }

    public Dispatch getDispatch() {
        return dispatch;
    }

    /**
     * {@code notifybus.lite.type} 支持的取值。{@code DB} 目前只是占位——对应的
     * {@code notifybus-lite-transport-db} 模块还没有实现，配置成这个值会在装配时报错。
     */
    public enum TransportType {
        ZOOKEEPER, NACOS, DB
    }

    /** {@code notifybus.lite.zk.*} 子配置：仅在没有提供自定义 {@code CuratorFramework} bean 时使用。 */
    public static class Zk {

        private String connectString;
        private Duration sessionTimeout = Duration.ofSeconds(60);
        private Duration connectionTimeout = Duration.ofSeconds(15);
        private final Retry retry = new Retry();

        public String getConnectString() {
            return connectString;
        }

        public void setConnectString(String connectString) {
            this.connectString = connectString;
        }

        public Duration getSessionTimeout() {
            return sessionTimeout;
        }

        public void setSessionTimeout(Duration sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
        }

        public Duration getConnectionTimeout() {
            return connectionTimeout;
        }

        public void setConnectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
        }

        public Retry getRetry() {
            return retry;
        }

        /** {@code notifybus.lite.zk.retry.*} 子配置。 */
        public static class Retry {

            private int baseSleepMs = 1000;
            private int maxRetries = 3;

            public int getBaseSleepMs() {
                return baseSleepMs;
            }

            public void setBaseSleepMs(int baseSleepMs) {
                this.baseSleepMs = baseSleepMs;
            }

            public int getMaxRetries() {
                return maxRetries;
            }

            public void setMaxRetries(int maxRetries) {
                this.maxRetries = maxRetries;
            }
        }
    }

    /** {@code notifybus.lite.nacos.*} 子配置：仅在没有提供自定义 {@code ConfigService} bean 时使用。 */
    public static class Nacos {

        private String serverAddr;

        /**
         * Nacos 自身的租户隔离概念（{@code PropertyKeyConst.NAMESPACE}），和顶层的
         * {@link NotifyBusProperties#namespace} 是两个不同的概念，不要混淆。可以不填，用 Nacos 的
         * public 命名空间。
         */
        private String namespace;

        private String username;
        private String password;

        public String getServerAddr() {
            return serverAddr;
        }

        public void setServerAddr(String serverAddr) {
            this.serverAddr = serverAddr;
        }

        public String getNamespace() {
            return namespace;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    /** {@code notifybus.lite.dispatch.*} 子配置：业务 handler 执行所在的调度线程池。 */
    public static class Dispatch {

        private String threadNamePrefix = "notifybus-lite-dispatch";
        private int corePoolSize = 2;
        private int maxPoolSize = 8;
        private int queueCapacity = 1000;

        public String getThreadNamePrefix() {
            return threadNamePrefix;
        }

        public void setThreadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
        }

        public int getCorePoolSize() {
            return corePoolSize;
        }

        public void setCorePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
        }

        public int getMaxPoolSize() {
            return maxPoolSize;
        }

        public void setMaxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }
    }
}
