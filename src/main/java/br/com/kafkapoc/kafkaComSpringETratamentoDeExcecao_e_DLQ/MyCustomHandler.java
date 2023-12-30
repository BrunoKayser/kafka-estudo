package br.com.kafkapoc.kafkaComSpringETratamentoDeExcecao_e_DLQ;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MyCustomHandler implements KafkaListenerErrorHandler {

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException e) {
        log.error("*** Entrou no handler");
        log.error("Payload: {}", message.getPayload());
        log.error("Exception: ", e);
        throw e;
    }

    //Este tratamento abaxo é para fazer algum tratamento ao Consumer, como por eexmplo resetar o ofset
   // @Override
   // public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
   //     return KafkaListenerErrorHandler.super.handleError(message, exception, consumer);
   // }
}
