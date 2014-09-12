package org.oceanobservatories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

public class Main {
    static String templateMessageString = "{\"type\":\"DRIVER_ASYNC_EVENT_SAMPLE\",\"value\":\"{\\\"quality_flag\\\": \\\"ok\\\", \\\"preferred_timestamp\\\": \\\"port_timestamp\\\", \\\"values\\\": [{\\\"value_id\\\": \\\"serial_number\\\", \\\"value\\\": \\\"4278190306\\\"}, {\\\"value_id\\\": \\\"elapsed_time\\\", \\\"value\\\": 4165.05}, {\\\"value_id\\\": \\\"par\\\", \\\"value\\\": 2157006272}, {\\\"value_id\\\": \\\"checksum\\\", \\\"value\\\": 70}], \\\"stream_name\\\": \\\"parad_sa_sample\\\", \\\"port_timestamp\\\": 3618862452.335417, \\\"driver_timestamp\\\": 3618862453.114037, \\\"pkt_format_id\\\": \\\"JSON_Data\\\", \\\"pkt_version\\\": 1}\",\"time\":%f}";
    private final static Logger log = LoggerFactory.getLogger(Main.class);
    private static Session session;
    private static MessageProducer messageProducer;

    public static void main(String args[]) throws Exception {
        long msDelay = 1000;    // 1 Hz default rate
        try {
            msDelay = Long.parseLong(System.getenv("MS_BETWEEN_JMS_MESSAGES"));
        } catch (Exception ex) {
            log.warn("Unable to retrieve/parse the MS_BETWEEN_JMS_MESSAGES environment variable. Defaulting to 1000 (1 Hz).", ex);
        }
        log.info("Creating producer...");
        createProducer();
        log.info("Publishing...");
        while (true) {
            publishJMS(String.format(templateMessageString, ((double)System.currentTimeMillis())/1000));
            Thread.sleep(msDelay);
        }
    }

    private static void createProducer() throws Exception {
        Properties properties = new Properties();
        try {
            properties.load(Main.class.getResourceAsStream("/jmsPublisher.properties"));
        } catch (Exception ex) {
            log.error("A fatal error occurred retrieving /jmsPublisher.properties", ex);
            throw ex;
        }
        Context context = new InitialContext(properties);
        ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionfactory");
        Connection connection = connectionFactory.createConnection();
        connection.start();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = (Destination) context.lookup("topicExchange");

        messageProducer = session.createProducer(destination);
    }

    public static void publishJMS(String event) throws Exception {
        if (messageProducer == null)
            createProducer();
        log.info("Publish to JMS: {}", event);
        messageProducer.send(session.createTextMessage(event));
    }
}



