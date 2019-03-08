package com.kafka.test.ctlr;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.test.util.ConsumerCreator;

@RestController
@RequestMapping("/consume")
public class ConsumerController {

	private String TOPIC = "topicjan142";

	@Autowired
	private KafkaTemplate<String, Object> template;
	
	@GetMapping("/consumetest/{input}")
	public String publishTest(@PathVariable String input) {
		return "Data published"+input;
	}
	
	@GetMapping("/read")
	public String consumeMessage() {
		
		//KafkaConsumer<Long, User> consumer = new KafkaConsumer<>(props);
		Consumer<String, Object> consumer = ConsumerCreator.createConsumer();
        List<String> topics = Arrays.asList("topicjan142");
        consumer.subscribe(topics);
        System.out.println("Subscribed to topics " + topics);
        long count = 0;
        long start = System.nanoTime();
        
        ConsumerRecords<String, Object> poll = consumer.poll(5000);
        for (final ConsumerRecord<String, Object> record : poll) {
        	System.out.println("Records are in topics:: "+record.value());
        	count += poll.count();
        }
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        long rate = (long) ((double) count / elapsed * 1000);
        consumer.close();
        System.out.printf("Total count: %,d in %,dms. Average rate: %,d records/s %n", count, elapsed, rate);
		
		return "Data consumed";
	}
	
	/*@GetMapping("/publish/{input}")
	public String publishMessage(@PathVariable String input) {
		template.send(TOPIC, "Hi  " + input + " welcome to Java Techie");
		//templateProducer.send(record)(TOPIC, "Hi  " + input + " welcome to Java Techie");
		return "Data published";
	}

	@GetMapping("/publishObject")
	public String publishJsonMessage() {
		template.send(TOPIC, new User(1234, "Bimal Panigrahy",
				new String[] { "Bangalore", "Marathali", "SGR Dental college", "house no : 143", "560037" }));
		return "Json Data published";
	}*/

}

