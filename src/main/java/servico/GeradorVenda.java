package servico;

import model.Venda;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import serializer.VendaSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.Random;

public class GeradorVenda {

    private static final Random random = new Random();
    private static BigDecimal valorIngresso = BigDecimal.valueOf(500);
    private static long operacao = 0;

    public static void main(String[] args) {

        final Properties _properties = new Properties();
        _properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        _properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        _properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VendaSerializer.class.getName());

        try (KafkaProducer<String, Venda> _producer = new KafkaProducer<>(_properties)) {

            while (true) {
                Venda _venda = gerarVenda();
                ProducerRecord<String, Venda> _record = new ProducerRecord<>("venda-ingresso", _venda);
                _producer.send(_record);
                Thread.sleep(500);
            }

        } catch (Exception e) {
            System.out.println("Deu ruim " + e +"\n"+e.getLocalizedMessage());
            e.printStackTrace();
        }


    }

    private static Venda gerarVenda() {
        long cliente = random.nextLong();
        int qtdIngressos = random.nextInt(10);
        return new Venda(
                operacao++,
                cliente,
                qtdIngressos,
                valorIngresso.multiply(BigDecimal.valueOf(qtdIngressos))

        );
    }
}
