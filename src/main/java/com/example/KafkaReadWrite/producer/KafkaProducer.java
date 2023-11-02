package com.example.KafkaReadWrite.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.logging.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@SpringBootApplication
public class KafkaProducer {
    private static final Logger LOG = (Logger) LoggerFactory.getLogger(KafkaProducer.class);
    private CommandLine cmd;
    private CommandLineParser parser;
    private Options options;
    private HelpFormatter formatter;
    private static final String OPT_KAFKA_BOOTSTRAP_SERVER = "kafkaBootstrapServers";
    private static final String OPT_DEFAULT_KAFKA_BOOTSTRAP_SERVER = "";
    private static final String OPT_TOPIC_NAME = "topicName";
    private static final String OPT_DEFAULT_TOPIC_NAME = "";
    private static final String OPT_PARTITION = "partition";
    private static final String OPT_DEFAULT_PARTITION = "0";
    private static final String OPT_NUMBER_OF_MESSAGES = "numberOfMessages";
    private static final String OPT_DEFAULT_NUMBER_OF_MESSAGES = "125826";
    private static final String OPT_OUTPUT_FILENAME = "outputFilename";
    private static final String OPT_DEFAULT_OUTPUT_FILENAME = "";
    private static final String JSON_SCHEMA_NAME_KEY = "JSON_SCHEMA_NAME";
    private static final String JSON_SCHEMA_VERSION_KEY = "JSON_SCHEMA_VALUE";
    private static final String CORRELATION_ID = "CORRELATION_ID";

    public static void main(final String[] args) {
        LOG.info("SEO URL Mapping Maintenance Tool - Kafka Publish Data Collector");
        LOG.info("- initializing application...");

        final SpringApplication app = new SpringApplication(KafkaProducer.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.run(args);
        LOG.info("- Finished publish data collection...");
        System.exit(1);
    }

    @PostConstruct
    public void initialize() {
        this.setupCommandLineOptions();
    }

    private void setupCommandLineOptions() {
        this.options = new Options();
        this.parser = new DefaultParser();
        this.formatter = new HelpFormatter();
        this.formatter.setWidth(120);

        final OptionGroup commandGroup = new OptionGroup();
        final Option helpOption = Option.builder("help")
                .hasArg(false)
                .required(false)
                .desc("print help")
                .build();
        commandGroup.addOption(helpOption);
        this.options.addOptionGroup(commandGroup);

        final Option kafkaBootstrapServerOption = Option.builder(OPT_KAFKA_BOOTSTRAP_SERVER)
                .hasArg()
                .required()
                .desc("Defines kafka boostrap server (host:port)")
                .build();
        this.options.addOption(kafkaBootstrapServerOption);

        final Option topicName = Option.builder(OPT_TOPIC_NAME)
                .hasArg()
                .required(false)
                .desc("Defines topic name (default: 1")
                .build();
        this.options.addOption(topicName);

        final Option numberOfMessages = Option.builder(OPT_NUMBER_OF_MESSAGES)
                .hasArg()
                .required(false)
                .desc("Defines number Of messages (default: 125 826)")
                .build();
        this.options.addOption(numberOfMessages);

        final Option outputFilename = Option.builder(OPT_OUTPUT_FILENAME)
                .hasArg()
                .required(false)
                .desc("Defines output file name (default: outputFile)")
                .build();
        this.options.addOption(outputFilename);

        final List<Option> optionsList = Arrays.asList(helpOption, kafkaBootstrapServerOption, topicName, numberOfMessages, outputFilename);
        this.formatter.setOptionComparator(Comparator.comparing((Option o) -> optionsList.indexOf(o)));
    }

    private void showConfiguration() {
        LOG.info("- Using Kafka Bootstrap Servers: {}", getKafkaBootstrapServer());
        LOG.info("- Using topic name: {}", getTopicName());
        LOG.info("- Using partition: {}", getPartition());
        LOG.info("- Using numberOfMessages: {}", getNumberOfMessages());
        LOG.info("- Using outputFilename: {}", getOutputFileName());
    }

    public void run(final String... args) throws Exception {
        try {
            this.cmd = this.parser.parse(this.options, args);
        } catch (final UnrecognizedOptionException | MissingArgumentException e) {
            this.formatter.printHelp("SEO URL Mapping Maintenance Tool - Kafka Publish Data Collector", this.options);
            LOG.warn("could not parse command options", e);
            System.exit(1);
        }
        this.showConfiguration();
        try (final KafkaProducer<String, String> producer = producer();
             // Read tar.gz from file by default /tmp/messages-count-125826.tar.gz
             final InputStream fo = Files.newInputStream(Paths.get(this.getOutputFileName()));
             final InputStream gzo = new GzipCompressorInputStream(fo);
             final ArchiveInputStream o = new TarArchiveInputStream(gzo)) {
            publishDataSetCollector(producer, o);
            producer.flush();
        } catch (final IOException e) {
            throw e;
        } catch (final Exception e) {
            LOG.error("Error during operation: {}", e.getLocalizedMessage(), e);
        }
    }

    void publishDataSetCollector(final KafkaProducer<String, String> producer, final ArchiveInputStream archiveInputStream) throws IOException {
        uncompressTarGz(producer, archiveInputStream);
    }

    private KafkaProducer<String, String> producer() {
        final Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBootstrapServer());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(config);
    }

    private String getKafkaBootstrapServer() {
        return this.cmd.getOptionValue(OPT_KAFKA_BOOTSTRAP_SERVER, OPT_DEFAULT_KAFKA_BOOTSTRAP_SERVER);
    }

    private String getTopicName() {
        return this.cmd.getOptionValue(OPT_TOPIC_NAME, OPT_DEFAULT_TOPIC_NAME);
    }

    private long getPartition() {
        return Long.parseLong(this.cmd.getOptionValue(OPT_PARTITION, OPT_DEFAULT_PARTITION));
    }

    private long getNumberOfMessages() {
        return Long.parseLong(this.cmd.getOptionValue(OPT_NUMBER_OF_MESSAGES, OPT_DEFAULT_NUMBER_OF_MESSAGES));
    }

    private String getOutputFileName() {
        return this.cmd.getOptionValue(OPT_OUTPUT_FILENAME, OPT_DEFAULT_OUTPUT_FILENAME);
    }

    private void uncompressTarGz(final KafkaProducer<String, String> producer, final ArchiveInputStream inputStream) {
        final long start = System.currentTimeMillis();
        int count = 0;
        try {
            TarArchiveEntry entry = (TarArchiveEntry) inputStream.getNextEntry();
            do {
                final int size = (int) entry.getSize();
                final byte[] buf = new byte[size];
                if (inputStream.canReadEntryData(entry)) {
                    inputStream.read(buf, 0, size);
                    publishProducerRecordToKafkaStorage(producer, saveObjectMapper(new String(buf)));
                    count++;
                    if (count % 1000 == 0) {
                        // Write every 1000 records a message to the output
                        LOG.info("Publish #{} messages ", count);
                    }
                }
                entry = (TarArchiveEntry) inputStream.getNextEntry();
            } while (entry != null);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final long end = System.currentTimeMillis();
        LOG.info("Publish {} records in {} ms", count, end - start);
    }

    private void publishProducerRecordToKafkaStorage(final KafkaProducer<String, String> producer, final CollectDataSetModel model) {
        this.sendHeaderTopicKeyValueToProducer(producer, model);
    }

    private void sendHeaderTopicKeyValueToProducer(final KafkaProducer<String, String> producer, final CollectDataSetModel model) {
        producer.send(this.addHeaderTopicKeyValue(model));
    }

    private ProducerRecord<String, String> addHeaderTopicKeyValue(final CollectDataSetModel model) {
        return this.addHeaders(new ProducerRecord<>(this.getTopicName(), model.recordKey(), model.recordValue()), model);
    }

    private ProducerRecord<String, String> addHeaders(final ProducerRecord<String, String> producerRecord, final CollectDataSetModel model) {
        final Headers headers = producerRecord.headers();
        headers.add(JSON_SCHEMA_NAME_KEY, model.schemaName().getBytes(StandardCharsets.UTF_8));
        headers.add(JSON_SCHEMA_VERSION_KEY, model.schemaVersion().getBytes(StandardCharsets.UTF_8));
        headers.add(CORRELATION_ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        return producerRecord;
    }

    private CollectDataSetModel saveObjectMapper(final String json) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readerFor(CollectDataSetModel.class).readValue(json);
    }
}
