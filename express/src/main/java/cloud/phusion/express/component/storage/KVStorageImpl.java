package cloud.phusion.express.component.storage;

import cloud.phusion.Context;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.PhusionException;
import cloud.phusion.storage.KVStorage;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class KVStorageImpl implements KVStorage {
    private static final String _position = KVStorageImpl.class.getName();

    private static JedisPool pool = null;

    private static boolean isFake = false;
    private static Map<String, Object> storage = new ConcurrentHashMap<String, Object>(); // For fake storage
    private static Map<String, Date> timer = new ConcurrentHashMap<String, Date>(); // For fake storage

    public static void init(Properties props) throws Exception {
        if (pool!=null || isFake || props==null) return;

        String host = props.getProperty(EngineFactory.Redis_Host);
        if (host==null || host.length()==0) {
            isFake = true;
            return;
        }

        JedisPoolConfig config = new JedisPoolConfig();

        config.setMaxIdle(128);
        config.setMaxTotal(1024);
        config.setTestOnBorrow(true);
        config.setTestWhileIdle(true);
        config.setTestOnReturn(false);
        config.setNumTestsPerEvictionRun(1024);

        int timeout = 3000;
        int port = Integer.parseInt(props.getProperty(EngineFactory.Redis_Port));
        int database = Integer.parseInt(props.getProperty(EngineFactory.Redis_Database));
        String password = props.getProperty(EngineFactory.Redis_Auth);

        pool = new JedisPool(config, host, port, timeout, password, database);
    }

    private String namespace = null;
    private Context baseCtx = null;

    /**
     * @param namespace A{applicationId}、I{integrationId}、C{clientId}
     */
    public KVStorageImpl(String namespace, Context ctx) {
        super();

        this.namespace = namespace + "_";
        this.baseCtx = ctx==null ? EngineFactory.createContext() : ctx;
    }

    public KVStorageImpl(String namespace) {
        this(namespace, null);
    }

    @Override
    public void put(String key, Object value) throws Exception {
        put(key, value, baseCtx);
    }

    @Override
    public void put(String key, Object value, Context ctx) throws Exception {
        String v = (value instanceof String) ? (String)value : value.toString();

        if (isFake) {
            storage.put(namespace+key, v);
            Date expireTime = new Date((new Date()).getTime()+(24*60*60*1000l));
            timer.put(namespace+key, expireTime);
            return;
        }

        try (Jedis redis = pool.getResource()) {
            redis.set(namespace+key, v);
        } catch (Exception ex) {
            throw new PhusionException("KV_OP", "Failed to put without expire time", String.format("namespace=%s, key=%s, value=%s",
                    namespace, key, v), ctx, ex);
        }
    }

    @Override
    public void put(String key, Object value, long millisecondsToLive) throws Exception {
        put(key, value, millisecondsToLive, baseCtx);
    }

    @Override
    public void put(String key, Object value, long millisecondsToLive, Context ctx) throws Exception {
        String v = (value instanceof String) ? (String)value : value.toString();

        if (isFake) {
            storage.put(namespace+key, v);
            Date expireTime = new Date((new Date()).getTime()+millisecondsToLive);
            timer.put(namespace+key, expireTime);
            return;
        }

        try (Jedis redis = pool.getResource()) {
            redis.psetex(namespace+key, millisecondsToLive, v);
        } catch (Exception ex) {
            throw new PhusionException("KV_OP", "Failed to put with expire time", String.format("namespace=%s, key=%s, value=%s",
                    namespace, key, v), ctx, ex);
        }
    }

    @Override
    public Object get(String key) throws Exception {
        return get(key, baseCtx);
    }

    @Override
    public Object get(String key, Context ctx) throws Exception {
        if (isFake) {
            if (doesExist(key)) return storage.get(namespace+key);
            else return null;
        }

        try (Jedis redis = pool.getResource()) {
            return redis.get(namespace+key);
        } catch (Exception ex) {
            throw new PhusionException("KV_OP", "Failed to retrieve", String.format("namespace=%s, key=%s",
                    namespace, key), ctx, ex);
        }
    }

    @Override
    public boolean doesExist(String key) throws Exception {
        return doesExist(key, baseCtx);
    }

    @Override
    public boolean doesExist(String key, Context ctx) throws Exception {
        if (isFake) {
            if (! storage.containsKey(namespace+key)) return false;

            Date t = timer.get(namespace+key);
            if ((new Date()).getTime() > t.getTime()) return false;
            else return true;
        }

        try (Jedis redis = pool.getResource()) {
            return redis.exists(namespace+key);
        } catch (Exception ex) {
            throw new PhusionException("KV_OP", "Failed to check existence", String.format("namespace=%s, key=%s",
                    namespace, key), ctx, ex);
        }
    }

    @Override
    public void remove(String key) throws Exception {
        remove(key, baseCtx);
    }

    @Override
    public void remove(String key, Context ctx) throws Exception {
        if (isFake) {
            storage.remove(namespace+key);
            timer.remove(namespace+key);
            return;
        }

        try (Jedis redis = pool.getResource()) {
            redis.del(namespace+key);
        } catch (Exception ex) {
            throw new PhusionException("KV_OP", "Failed to remove", String.format("namespace=%s, key=%s",
                    namespace, key), ctx, ex);
        }
    }

    @Override
    public boolean lock(String key) throws Exception {
        return lock(key, baseCtx);
    }

    @Override
    public boolean lock(String key, Context ctx) throws Exception {
        // It is a naive implementation of distributed lock. To be optimized.

        if (isFake) {
            if (doesExist(key)) return false;
            else {
                put(key, "");
                return true;
            }
        }

        try (Jedis redis = pool.getResource()) {
            boolean locked = redis.setnx(namespace+key, "lock") == 1;
            return locked;
        } catch (Exception ex) {
            throw new PhusionException("KV_OP", "Failed to lock without expire time", String.format("namespace=%s, key=%s",
                    namespace, key), ctx, ex);
        }
    }

    @Override
    public boolean lock(String key, long millisecondsToLive) throws Exception {
        return lock(key, millisecondsToLive, baseCtx);
    }

    @Override
    public boolean lock(String key, long millisecondsToLive, Context ctx) throws Exception {
        // It is a naive implementation of distributed lock. To be optimized.

        if (isFake) {
            if (doesExist(key)) return false;
            else {
                put(key, "", millisecondsToLive);
                return true;
            }
        }

        try (Jedis redis = pool.getResource()) {
            String theKey = namespace + key;
            boolean locked = redis.setnx(theKey, "lock") == 1;
            if (locked) {
                try {
                    locked = redis.pexpire(theKey, millisecondsToLive) == 1;
                }
                catch (Exception ex) {
                    redis.del(theKey); // If failed to set the expire time, the lock should be released
                    throw ex;
                }
                if (! locked) redis.del(theKey);
            }
            return locked;
        } catch (Exception ex) {
            throw new PhusionException("KV_OP", "Failed to lock with expire time", String.format("namespace=%s, key=%s",
                    namespace, key), ctx, ex);
        }
    }

    @Override
    public void unlock(String key) throws Exception {
        unlock(key, baseCtx);
    }

    @Override
    public void unlock(String key, Context ctx) throws Exception {
        // It is a naive implementation of distributed lock. To be optimized.

        if (isFake) {
            if (doesExist(key)) remove(key);
            return;
        }

        try (Jedis redis = pool.getResource()) {
            redis.del(namespace+key);
        } catch (Exception ex) {
            throw new PhusionException("KV_OP", "Failed to unlock", String.format("namespace=%s, key=%s",
                    namespace, key), ctx, ex);
        }
    }

    /*
     Simple message sub/pub implementation based on Redis.
     The subscription is not persistent, i.e. if some client was shutdown, when it comes back,
     the messages published during its downtime will not be republished to it.
     */

    private Map<String,JedisPubSub> subscribers = new ConcurrentHashMap<>();

    public void subscribe(String channel, MessageListener listener) throws Exception {
        subscribe(channel, listener, baseCtx);
    }

    public void subscribe(String channel, MessageListener listener, Context ctx) throws Exception {
        String data = String.format("engineId=%s, channel=%s", ctx.getEngine().getId(), namespace+channel);

        JedisPubSub subscriber = new MessageSubscriber(listener);

        new Thread(() -> {
            try (Jedis redis = pool.getResource()) {
                redis.subscribe(subscriber, namespace+channel);
            } catch (Exception ex) {
                ctx.logError(_position, "Failed to subscribe message", data, ex);
            }
        }).start();

        // Wait a while for the subscription is done. If other messaging operations coming in soon, it will fail.
        Thread.sleep(1000);

        subscribers.put(namespace+channel,subscriber);
        ctx.logInfo(_position, "Message subscribed", data);
    }

    public void publish(String channel, String msg) throws Exception {
        publish(channel, msg, baseCtx);
    }

    public void publish(String channel, String msg, Context ctx) throws Exception {
        channel = namespace+channel;
        String engineId = ctx.getEngine().getId();
        String data = String.format("engineId=%s, channel=%s, msg=%s", engineId, channel, msg);

        try (Jedis redis = pool.getResource()) {
            redis.publish(channel, msg);
            ctx.logInfo(_position, "Message published", data);
        } catch (Exception ex) {
            throw new PhusionException("KV_OP", "Failed to publish message", data, ctx, ex);
        }
    }

    public void unsubscribe(String channel) throws Exception {
        unsubscribe(channel, baseCtx);
    }

    public void unsubscribe(String channel, Context ctx) throws Exception {
        channel = namespace+channel;
        JedisPubSub subscriber = subscribers.get(channel);
        if (subscriber == null) return;

        subscriber.unsubscribe(channel);
        subscribers.remove(channel);

        String data = String.format("engineId=%s, channel=%s", ctx.getEngine().getId(), channel);
        ctx.logInfo(_position, "Message unsubscribed", data);
    }

    private static void _testMessaging(String[] args) throws Exception {
        // mvn exec:exec -Dexec.executable="java" -Dexec.args="-classpath %classpath cloud.phusion.express.component.storage.KVStorageImpl <WORKER_ID>"
        // Press q+<enter> to exit

        Properties props = new Properties();
        props.setProperty(EngineFactory.Cluster_Worker, args.length==0?"1":args[0]);
        props.setProperty(EngineFactory.Redis_Host, "192.168.30.158");
        props.setProperty(EngineFactory.Redis_Port, "6379");
        props.setProperty(EngineFactory.Redis_Database, "0");
        props.setProperty(EngineFactory.Redis_Auth, "qZatalyNpnTZoeDv");

        Engine engine = EngineFactory.createEngine(props);
        Context ctx = EngineFactory.createContext(engine);
        KVStorageImpl queue = (KVStorageImpl) engine.getKVStorageForIntegration("Simple");

        queue.subscribe(
                "NOTHING",
                (channel, msg) ->
                        System.out.println(String.format("Received message: channel=%s, msg=%s",channel,msg)),
                ctx);

        queue.subscribe(
                "MSG",
                (channel, msg) ->
                        System.out.println(String.format("Received message: channel=%s, msg=%s",channel,msg)),
                ctx);

        while (true) {
            byte[] buf = new byte[100];
            int len = System.in.read(buf);
            String input = new String(buf, 0, len);
            input = input.trim();
            if (input.equals("q")) {
                queue.unsubscribe("MSG", ctx);
                queue.unsubscribe("NOTHING", ctx);
                break;
            }
            else queue.publish("MSG", "start application "+input, ctx);
        }
    }

}
