package com.tranbinlee.notifybus.lite.transport.nacos;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * dataId/group 编码规则的纯逻辑校验，不需要连接真实 Nacos server。
 */
class NacosPathsTest {

    @Test
    void resourceGroupJoinsSanitizedRootAndTopic() {
        assertThat(NacosPaths.resourceGroup("/notifybus", "orders")).isEqualTo("_notifybus.orders");
    }

    @Test
    void resourceDataIdIsSanitizedResourceKey() {
        assertThat(NacosPaths.resourceDataId("order/1")).isEqualTo("order_1");
    }

    @Test
    void broadcastGroupAppendsFixedSuffixAndIsDistinctFromResourceGroup() {
        String broadcastGroup = NacosPaths.broadcastGroup("/notifybus", "orders");
        assertThat(broadcastGroup).isEqualTo("_notifybus.orders.broadcast");
        assertThat(broadcastGroup).isNotEqualTo(NacosPaths.resourceGroup("/notifybus", "orders"));
    }

    @Test
    void broadcastDataIdIsFixedConstant() {
        assertThat(NacosPaths.broadcastDataId()).isEqualTo("topic");
    }

    @Test
    void sanitizeReplacesIllegalCharactersWithUnderscore() {
        assertThat(NacosPaths.sanitize("a/b c#d")).isEqualTo("a_b_c_d");
    }

    @Test
    void sanitizeKeepsAllowedCharactersUnchanged() {
        assertThat(NacosPaths.sanitize("Order-1_v2.final")).isEqualTo("Order-1_v2.final");
    }

    @Test
    void sanitizeReturnsUnderscoreForBlankInput() {
        assertThat(NacosPaths.sanitize("")).isEqualTo("_");
        assertThat(NacosPaths.sanitize(null)).isEqualTo("_");
    }
}
