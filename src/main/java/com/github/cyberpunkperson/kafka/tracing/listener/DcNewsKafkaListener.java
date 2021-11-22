package com.github.cyberpunkperson.kafka.tracing.listener;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import src.main.java.com.github.cyberpunkperson.tracing.marvel.kafka.event.Marvel.Event;
import src.main.java.com.github.cyberpunkperson.tracing.marvel.kafka.event.Marvel.Event.News;
import src.main.java.com.github.cyberpunkperson.tracing.target.kafka.event.News.NewsEvent;
import src.main.java.com.github.cyberpunkperson.tracing.target.kafka.event.News.NewsEvent.Action;

import static com.github.cyberpunkperson.kafka.tracing.utils.ProtobufUtils.byteStringToUUIDString;

@Slf4j
@Component
@RequiredArgsConstructor
class DcNewsKafkaListener {

    @Value("${kafka.topic.news-target}")
    private String targetTopic;

    private final KafkaTemplate kafkaTemplate;


    @KafkaListener(id = "DcNewsKafkaListener",
            topics = "${kafka.topic.dc}",
            containerFactory = "dcContainer",
            concurrency = "${kafka.topic.dc.concurrency:1}",
            clientIdPrefix = "#{T(java.util.UUID).randomUUID().toString()}",
            idIsGroup = false)
    public void handleDcNewsEvent(byte[] message) throws InvalidProtocolBufferException {
        Event dcEvent = Event.parseFrom(message);
        News news = dcEvent.getNews();
        Action action = switch (news.getAction()) {
            case VICTORY -> Action.FEAT;
            case FUCK_UP -> Action.FUCK_UP;
            default -> throw new IllegalArgumentException("Unsupported action received");
        };

        log.info("Handling DC event with id: '%s'".formatted(dcEvent.getId()));

        NewsEvent targetEvent = NewsEvent.newBuilder()
                .setId(dcEvent.getId())
                .setHeroId(news.getHeroId())
                .setHeroName(news.getHeroName())
                .setDescription(news.getDescription())
                .setAction(action)
                .setEntityIdempotencyKey(byteStringToUUIDString(news.getHeroId()))
                .build();

        kafkaTemplate.send(targetTopic, news.getHeroId().toByteArray(), targetEvent.toByteArray());
    }
}
