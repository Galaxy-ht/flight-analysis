import com.Producer;
import com.Consumer;

import java.net.UnknownHostException;

public class StreamingDataServer {
    public static void main(String []args) throws UnknownHostException {
        Thread consumer = new Thread(new Consumer());
        Thread producer = new Thread(new Producer());
        consumer.start();
        producer.start();
    }

}
