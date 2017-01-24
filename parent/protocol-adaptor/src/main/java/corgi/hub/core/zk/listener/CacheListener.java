package corgi.hub.core.zk.listener;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

/**
 * Created by Terry LIANG on 2016/12/30.
 */
public class CacheListener implements PathChildrenCacheListener {
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
        switch (event.getType()) {
            case CHILD_UPDATED: {
                String key = ZKPaths.getNodeFromPath(event.getData().getPath());
                String value = new String(event.getData().getData());
                System.out.println(String.format("////////////////////////// %s = %s", key, value));
                break;
            }
            case CHILD_REMOVED: {
                String key = ZKPaths.getNodeFromPath(event.getData().getPath());
                System.out.println(String.format("////////////////////////// %s = %s", key, "hhh"));
                break;
            }
            case CHILD_ADDED: {
                String key = ZKPaths.getNodeFromPath(event.getData().getPath());
                String value = new String(event.getData().getData());
                System.out.println(String.format("////////////////////////// %s = %s", key, value));
                break;
            }
            default:
                break;
        }
    }

}
