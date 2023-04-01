package cloud.phusion.express.component.storage;

import redis.clients.jedis.JedisPubSub;

public class MessageSubscriber extends JedisPubSub {
    private MessageListener listener;

    MessageSubscriber(MessageListener listener) {
        super();
        this.listener = listener;
    }

    @Override
    public void onMessage(String channel, String message) {
        listener.onMessage( channel, message );
    }

}
