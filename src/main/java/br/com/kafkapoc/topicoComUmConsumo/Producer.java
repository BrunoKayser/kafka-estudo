package br.com.kafkapoc.topicoComUmConsumo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var producer = new KafkaProducer<String, String>(properties());

        var record = new ProducerRecord<>("compras.do.cliente", "cliente-1","compras: 50reais");

        Callback callback = (data, error) -> {
            if(error != null) {
                error.printStackTrace();
                return;
            }
            System.out.println("Mensagem enviada com sucesso");
            System.out.println(data.partition());
            System.out.println(data.offset());
        };

        producer.send(record, callback).get();


    }

    private static Properties properties() {

        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //Endereço do kafka
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //Serializador da chave: Pode ser colocado um schema ou outro tipo de classe
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//Serializador do valor

        return properties;
    }

}
