package corgi.hub.core.zk.listener;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Terry LIANG on 2016/12/30.
 */
public class HubNodeTreeCacheListener implements TreeCacheListener {
    private Logger logger = LoggerFactory.getLogger(HubNodeTreeCacheListener.class);
    @Override
    public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
        switch (treeCacheEvent.getType()) {
            case NODE_ADDED:
                logger.info("NODE_ADD = ");
                break;
            case NODE_UPDATED:
                logger.info("NODE_UPDATED = {}", new String(treeCacheEvent.getData().getData()));
                break;
            case NODE_REMOVED:
                logger.info("NODE_REMOVED = ");
                break;
            case CONNECTION_LOST:
                logger.info("CONNECTION_LOST = ");
                break;
            case CONNECTION_RECONNECTED:
                logger.info("CONNECTION_RECONNECTED = ");
                break;
            case CONNECTION_SUSPENDED:
                logger.info("CONNECTION_SUSPENDED = ");
                break;
            case INITIALIZED:
                logger.info("INITIALIZED = ");
                break;
        }
    }
}
