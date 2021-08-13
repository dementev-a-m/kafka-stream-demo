package inno.tech.demo.config;


import inno.tech.demo.pojo.Account;
import inno.tech.demo.pojo.Client;
import inno.tech.demo.pojo.ClientAccount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;
import redis.clients.jedis.Jedis;

import java.time.Duration;

@Configuration
@EnableKafkaStreams
@Slf4j
@RequiredArgsConstructor
public class StreamConfig {
    private final Jedis jedis;
//    @Bean
//    public KStream<Long, ClientAccount> accountKTable(StreamsBuilder streamsBuilder) {
//        KTable<Long, Client> clientKTable = streamsBuilder.stream("client", Consumed.with(Serdes.String(),new JsonSerde<>(Client.class)))
//                .selectKey((key, value) -> value.getId()).groupByKey().reduce(((oldValue, newValue) -> newValue));
//        KStream<Long, ClientAccount> accountKStream = streamsBuilder.stream("account", Consumed.with(Serdes.String(),new JsonSerde<>(Account.class)))
//                .peek((key, value) -> log.info(value.toString()))
//                .selectKey((key, value) -> value.getClientId())
//                .join(clientKTable, (account, client) -> ClientAccount.builder().account(account).client(client).build());
//
//        accountKStream.peek((key, value) -> log.info(value.toString()))
//                .to("output");
//        return accountKStream;
//    }


//    @Bean
//    public KStream<String, Account> accountKStream(StreamsBuilder streamsBuilder) {
//        KStream<String, Account> accountKStream = streamsBuilder.stream("account", Consumed.with(Serdes.String(), new JsonSerde<>(Account.class)));
//        accountKStream.peek((key, value) -> log.info(value.toString()))
//                .selectKey((key, value) -> "" + value.getClientId())
//                .groupByKey()
//                .windowedBy(TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(10)))
//                .reduce((value1, value2) -> value2)
//                .toStream()
//                .selectKey((key, value) -> "" + value.getClientId())
//                .to("windows_account");
//        return accountKStream;
//    }

    @Bean
    public KStream<String, Account> accountKStream(StreamsBuilder streamsBuilder){
        KStream<String, Account> accountKStream = streamsBuilder.stream("account", Consumed.with(Serdes.String(), new JsonSerde<>(Account.class)));

        return accountKStream;
    }
}
