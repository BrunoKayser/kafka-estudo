package br.com.kafkapoc.kafkaComSpringETratamentoDeExcecao_e_DLQ;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

@EnableKafka
@Configuration
public class KafkaConfig {

    private static final long ONE_SECOND = 1000L;

    //Configuração de como o Kafka irá consumir uma mensagem
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaFactory() {
        var kafkaFactory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        kafkaFactory.setConsumerFactory(consumer());
        kafkaFactory.setCommonErrorHandler(defaultErrorandler()); //Configurar as retentativas para o tratamento de exceção
        return kafkaFactory;
    }

    private DefaultErrorHandler defaultErrorandler() {
        var recover = new DeadLetterPublishingRecoverer(new KafkaTemplate<>(producer()), topicDLQ); //Configurando a DLQ

        // O comportamento default do kafka é 1 tentativa e 9 retentativas, então a configuração abaxo sobrescreve esta padrão
        return new DefaultErrorHandler(recover, new FixedBackOff(ONE_SECOND, 3)); //Configurar de quanto em quanto tempo será consumido(em milesegundos) a mensagem novamente
    }

    private final BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> topicDLQ = (consumerRecord, e) -> {
        return new TopicPartition("ecommerce.cliente2-DLQ", consumerRecord.partition()); //O segundo argumento 'cr.partition' indica em qual partição do tópico DLQ será postado,
                                                                                    // então é necessáro que a DLQ e o tópco que gerou a excetion tenham a mesma quantidade
                                                                                    // de partições, caso desejado específicar em qual partição da DLQ será publicada
                                                                                    // basta substituír o consumerRecord.partition pelo número da partição, ou colocar -1 para ser em qualquer partição que tiver
    };

    private ConsumerFactory<String, String> consumer() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    //Configuração de como o Kafka irá postar uma mensagem
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producer());
    }

    private ProducerFactory<String, String> producer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    private ProducerFactory<String, String> producerDLQ() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

}
