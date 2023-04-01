package cloud.phusion.express.component.storage;

/**
 * Adapter to Redis message subscription
 */
public interface MessageListener {

    void onMessage(String channel, String msg);

}
