package com.tranbinlee.notifybus.lite.example.springboot.publisher.web;

import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.example.springboot.publisher.catalog.CatalogChangePublisher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 示例 REST 接口，方便用 curl 手动触发一次广播：
 * <pre>{@code
 * curl -X POST "http://localhost:8081/catalog/sku-1001/changes?changeType=UPDATED"
 * curl http://localhost:8081/catalog/changes
 * }</pre>
 */
@RestController
public class CatalogChangeController {

    private final CatalogChangePublisher publisher;

    public CatalogChangeController(CatalogChangePublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping("/catalog/{sku}/changes")
    public String publish(@PathVariable String sku, @RequestParam ChangeType changeType) {
        publisher.publishChange(sku, changeType);
        return "published: sku=" + sku + ", changeType=" + changeType;
    }

    @GetMapping("/catalog/changes")
    public List<CatalogChangePublisher.PublishedChange> recent() {
        return publisher.recentHistory();
    }
}
