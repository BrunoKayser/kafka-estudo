package br.com.kafkapoc.kafkaComSpringETratamentoDeExcecao_e_DLQ;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(
            topics = "ecommerce.cliente2",
            groupId = "ecommerce-groupId-1",
            errorHandler = "myCustomHandler", // Para utilizar erro personalizado
            containerFactory = "kafkaFactory") // Forçar o consumidor a usar um determinado containerFactory configurado no KafkaConfig
    //@Retryable(maxAttempts = 2, backoff = @Backoff(delay = 1000)) -> Essa também é uma maneira de tratar a retentativa, sem a necessadade de criar uma nova classe
    public void consumer(String message) {
        System.out.println("consumidor ecommerce-groupId-1");
        System.out.println(message);
        throw new RuntimeException("Erro para teste"); //Colocado para sempre lançar exception para caír no Handler
    }

//    @KafkaListener(topics = "ecommerce.cliente2", groupId = "ecommerce-groupId-2")
//    public void consumer2(String message) {
//        System.out.println("consumidor ecommerce-groupId-2");
//        System.out.println(message);
//    }

    /** Configuração do listener de DLQ*/
    @KafkaListener(
            topics = "ecommerce.cliente2-DLQ",
            groupId = "ecommerce-groupId-1",
            errorHandler = "myCustomHandler", // Para utilizar erro personalizado
            containerFactory = "kafkaFactory") // Forçar o consumidor a usar um determinado containerFactory configurado no KafkaConfig
    public void consumerDLQ(String message) {
        System.out.println("Consumindo da DLQ");
        System.out.println(message);
    }

}
