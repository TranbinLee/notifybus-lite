package com.tranbinlee.notifybus.lite.transport.zk;

/**
 * {@code {root}/{topic}/{resourceKey}} 路径拼接。
 */
final class ZkPaths {

    private ZkPaths() {
    }

    static String normalizeRoot(String rootPath) {
        if (rootPath == null || rootPath.isEmpty() || "/".equals(rootPath)) {
            return "";
        }
        String trimmed = rootPath.endsWith("/") ? rootPath.substring(0, rootPath.length() - 1) : rootPath;
        return trimmed.startsWith("/") ? trimmed : "/" + trimmed;
    }

    static String topicPath(String rootPath, String topic) {
        return rootPath + "/" + topic;
    }

    static String resourcePath(String rootPath, String topic, String resourceKey) {
        return topicPath(rootPath, topic) + "/" + resourceKey;
    }

    static String lastSegment(String path) {
        int idx = path.lastIndexOf('/');
        return idx < 0 ? path : path.substring(idx + 1);
    }
}
