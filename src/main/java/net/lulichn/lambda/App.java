package net.lulichn.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;

public class App implements RequestHandler<S3Event, Object> {
    @Override
    public Object handleRequest(S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();

        logger.log("S3Event: " + event.toJson());
        if (event.getRecords() == null || event.getRecords().isEmpty()) {
            return "Empty Event";
        }

        for (S3EventNotification.S3EventNotificationRecord record: event.getRecords()){
            String key = record.getS3().getObject().getKey();
            logger.log("Key: " + key);
            String message = new EmbulkWork().embulk(key);
            logger.log(message);
        }

        return String.valueOf("Done");
    }

    public static void main(String[] args) {
        // For Local test
        System.out.println("Main");
        String message = new EmbulkWork().embulk(null);
        System.out.println(message);
    }
}
