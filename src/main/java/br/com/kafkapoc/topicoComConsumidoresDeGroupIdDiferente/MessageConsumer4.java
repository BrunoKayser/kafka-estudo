package br.com.kafkapoc.topicoComConsumidoresDeGroupIdDiferente;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MessageConsumer4 {

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());

        consumer.subscribe(Collections.singletonList("ecommerce.groupid.teste"));

        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));

            for (var record: records) {
                System.out.println("Compra nova: ");
                System.out.println(record.key());
                System.out.println(record.value());
                System.out.println(record.offset());
                System.out.println(record.partition());
            }
        }
    }

    private static Properties properties() {

        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //Endereço do kafka
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //Descerializar  da chave: Pode ser colocado um schema ou outro tipo de classe
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// Pegar as mensagens desde o começo, ou seja, quando não tem ofset configurado ou ele esta indisponível, o consumidor atribuirá o ofset como o primeiro, no caso o ofset mais antigo
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "eccomerce-teste"); //Precisa sempre informar o grupo sempre que consumir algo

        return properties;
    }

}
