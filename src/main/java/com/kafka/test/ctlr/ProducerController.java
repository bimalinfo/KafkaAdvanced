package com.kafka.test.ctlr;


import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


import com.kafka.test.util.IKafkaConstants;
import com.kafka.test.util.ProducerCreator;
import com.kafka.test.vo.User;


@RestController
@RequestMapping("/publish")
public class ProducerController {

	private String TOPIC = "topicjan142";
	
	@GetMapping("/test/{input}")
	public String publishTest(@PathVariable String input) {
		return "Data published"+input;
	}
	
	@GetMapping("/write/{input}")
	public String publishMessage(@PathVariable String input) {
		Producer<String, Object> producer = ProducerCreator.createProducer();

		ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(IKafkaConstants.TOPIC_NAME,
				 input);
		try {
			RecordMetadata metadata = producer.send(record).get();
			System.out.println("Record sent with key " + input + " to partition " + metadata.partition()
					+ " with offset " + metadata.offset());
		} catch (ExecutionException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		} catch (InterruptedException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		}
	            
		
		return "Data published";
	}
	
	@GetMapping("/write")
	public String publishJsonMessage() {
		
		User user=new User(1234, "Bimal Panigrahy",
				new String[] { "Bangalore", "K Narayanapura", "LIG 21", "KHB Colony ", "560077" });
		
		Producer<String, Object> producer = ProducerCreator.createProducer();

		ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(IKafkaConstants.TOPIC_NAME,
				user);
		try {
			RecordMetadata metadata = producer.send(record).get();
			System.out.println("User Record sent " + metadata.partition()
					+ " with offset " + metadata.offset());
		} catch (ExecutionException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		} catch (InterruptedException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		}
		return "Json Data published";
	}
}