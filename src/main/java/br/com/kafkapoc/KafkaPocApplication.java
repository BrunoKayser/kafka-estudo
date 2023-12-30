package br.com.kafkapoc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka 	//Para utilizar o Kafka com spring precisa habilitar a anotação, para os testes nas pastas diferente te kafkaComSpring,
				// necessário desabilitar essa anotação
public class KafkaPocApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaPocApplication.class, args);
	}

}
