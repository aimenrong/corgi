package corgi.hub.core.zk;

/**
 * Created by Terry LIANG on 2016/12/26.
 */
import corgi.hub.core.zk.listener.CacheListener;
import corgi.hub.core.zk.listener.HubNodeTreeCacheListener;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

/**
 * Created by Terry LIANG on 12/26/2016.
 */
public class ZkClient {
    private CuratorFramework client;
    private PathChildrenCache cache;
    private TreeCache treeCache;

    public void init() throws Exception {
        try {
            client = createSimple(getConnectString());
            client.start();
            if (client.checkExists().forPath("/corgi") == null) {
                client.create().forPath("/corgi");
                client.setData().forPath("/corgi", "{list:[terry, corgi]}".getBytes());
            }
            if (client.checkExists().forPath("/corgi/fullpicture") == null) {
                client.create().forPath("/corgi/fullpicture");
                client.setData().forPath("/corgi/fullpicture", "{list:[terry, corgi]}".getBytes());
            }

            System.out.println("get data =--" + client.getData().forPath("/corgi/fullpicture"));

            System.out.println("get data =--" +client.getData().forPath("/corgi/fullpicture"));

            cache = createCache("/corgi");

            PathChildrenCacheListener listener = new CacheListener();
            cache.getListenable().addListener(listener);

            treeCache = new TreeCache(client, "/corgi/app1");
            TreeCacheListener listener2 = new HubNodeTreeCacheListener();
            treeCache.getListenable().addListener(listener2);
            TreeCacheListener listener3 = new HubNodeTreeCacheListener();
            treeCache.getListenable().addListener(listener3);
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            treeCache.start();

            final NodeCache nodeCache = new NodeCache(client, "/corgi/app1");
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    String message = new String(nodeCache.getCurrentData().getData());
                    System.out.println("--------------------got message : " + message);
                }
            });
            System.out.println("cache set listener");
            nodeCache.start(true);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
        }
        while (true) {
            TimeUnit.SECONDS.sleep(1000);
        }
    }

    protected PathChildrenCache createCache(String path) {
        return new PathChildrenCache(client, path, true);
    }

    public String getConnectString() {
        return "localhost:2181";
    }

    public CuratorFramework createSimple(String connectionString) {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    }

    public CuratorFramework createWithOptions(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs) {
        // using the CuratorFrameworkFactory.builder() gives fine grained control
        // over creation options. See the CuratorFrameworkFactory.Builder javadoc details
        return CuratorFrameworkFactory.builder().connectString(connectionString)
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                // etc. etc.
                .build();
    }


    public static void main(String[] args) throws Exception {
        new ZkClient().init();
        while (true) {
            TimeUnit.SECONDS.sleep(1000);
        }
    }
}


