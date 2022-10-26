package org.examples.nats;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class NatsEphemeralConsumerTests {

  private final String subject = "my-subject";
  private Connection nc;
  private JetStream js;
  private long startTime;
  private long receivedMessageCount = 0;

  @BeforeEach
  @SneakyThrows
  void connect() {
    nc = Nats.connect("nats://localhost:4222");
    JetStreamManagement jsm = nc.jetStreamManagement();

    // Build the configuration
    StreamConfiguration streamConfig = StreamConfiguration.builder()
        .name("my-stream")
        .subjects(subject)
        .storageType(StorageType.Memory)
        .build();
    jsm.addStream(streamConfig);
    jsm.purgeStream("my-stream");
    js = nc.jetStream();
  }

  @AfterEach
  void close() throws InterruptedException {
    nc.close();
  }

  @ParameterizedTest(name = "{index} processing delay: {0}")
  @CsvSource({ "2000", "3000" })
  @SneakyThrows
  @DisplayName("Ephemeral consumer should be able to consume all messages in pull mode")
  void shouldConsumeAllMessagesWithBatchPull(long processingDelayInMillis) {

    log("Testing numberOfMessages: %d batchSize:%d, processingDelayInMillis:%d",
        7, 3, processingDelayInMillis);

    publishMessages(7);

    JetStreamSubscription subscription = subscribe();

    subscription.pull(3);
    processAndAcknowledgeNextMessage(subscription, processingDelayInMillis);
    processAndAcknowledgeNextMessage(subscription, processingDelayInMillis);
    processAndAcknowledgeNextMessage(subscription, processingDelayInMillis);

    subscription.pull(3); //  pull with batch size 3
    processAndAcknowledgeNextMessage(subscription, processingDelayInMillis);
    processAndAcknowledgeNextMessage(subscription, processingDelayInMillis);
    processAndAcknowledgeNextMessage(subscription, processingDelayInMillis);

    subscription.pull(3); // cannot get any messages anymore
    processAndAcknowledgeNextMessage(subscription, processingDelayInMillis);
  }

  @SneakyThrows
  private void processAndAcknowledgeNextMessage(final JetStreamSubscription sub,
      final long processingDelayInMillis) {
    var message = sub.nextMessage(Duration.ofSeconds(1));
    assertNotNull(message, () -> "Next message is null, consumer info: " + getConsumerInfo(sub));
    message.inProgress();
    ++receivedMessageCount;
    message.ackSync(Duration.ofMillis(100));
    log("Received message #%d content: %s\nmessage metadata: %s", receivedMessageCount,
        new String(message.getData()), message.metaData());
    Thread.sleep(processingDelayInMillis);
  }

  private String getConsumerInfo(final JetStreamSubscription sub) {
    try {
      return Objects.toString(sub.getConsumerInfo());
    } catch (IOException | JetStreamApiException e) {
      e.printStackTrace();
    }
    return "ERROR";
  }

  @ParameterizedTest(name = "{index} published messages:{0}  batch size: {1} delay: {2}")
  @CsvSource({ "1,1,3000", "9,1,3000", "9,2,3000", "9,3,3000" })
  @SneakyThrows
  public void shouldConsumeAllMessages(int numberOfMessages, int batchSize,
      int processingDelayInMillis) {
    consumeAllMessages(numberOfMessages, batchSize, processingDelayInMillis);
  }

  @SneakyThrows
  public void consumeAllMessages(int numberOfMessages, int batchSize, int processingDelayInMillis) {
    log(" numberOfMessages: %d batchSize:%d, processingDelayInMillis:%d", numberOfMessages,
        batchSize, processingDelayInMillis);
    // pull a bit more than required
    int maxPullAttempts = (numberOfMessages / batchSize) + 3;
    // publish messages first
    publishMessages(numberOfMessages);

    JetStreamSubscription sub = subscribe();

    while (--maxPullAttempts >= 0) {
      sub.pull(batchSize);
      boolean newMessageReceived = false;
      var message = sub.nextMessage(Duration.ofSeconds(1));
      while (message != null) {
        if (message.isStatusMessage()) {
          log("Status message received%", message.getStatus());
        }
        if (!message.isJetStream()) {
          continue;
        }
        newMessageReceived = true;
        receivedMessageCount++;
        message.inProgress();
        printMessageInfo(message);
        message.ackSync(Duration.ofSeconds(1)); // moved ack prior to sleep and used sync,
        Thread.sleep(processingDelayInMillis);
        message = sub.nextMessage(Duration.ofMillis(10));
      }

      if (!newMessageReceived) {
        log("No message is returned from pull request. Remaining pull attempt %s, received "
                + "message count: %d subscription is active: %s,\nconsumer info: %s",
            maxPullAttempts,
            receivedMessageCount, sub.isActive(), sub.getConsumerInfo());
      }
    }

    assertEquals(numberOfMessages, receivedMessageCount, this::getStreamInfo);

  }

  @ParameterizedTest(name = "{index} published messages:{0}  batch size: {1} delay: {2}")
  @CsvSource({ "57,100,1000", "57,100,500" })
  @SneakyThrows
  public void testDuplicatesMessagesWithLargeBatchSize(int numberOfMessages, int batchSize,
      int processingDelayInMillis) {
    consumeAllMessages(numberOfMessages, batchSize, processingDelayInMillis);
  }

  private String getStreamInfo() {
    try {
      return nc.jetStreamManagement().getStreamInfo("my-stream").toString();
    } catch (Exception e) {
      return "NA";
    }
  }

  @SneakyThrows
  private void publishMessages(int numberOfMessages) {
    for (int i = 1; i <= numberOfMessages; i++) {
      js.publish(subject, ("" + i).getBytes());
    }
  }

  @SneakyThrows
  private JetStreamSubscription subscribe() {
    // use default options.
    PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
        .durable(null) //  ephemeral consumer
        .configuration(
            ConsumerConfiguration.builder().inactiveThreshold(600_000).ackPolicy(AckPolicy.Explicit)
                //            .ackWait(Duration.ofSeconds(100))
                .build())
        .build();

    startTime = System.currentTimeMillis();
    JetStreamSubscription sub = js.subscribe(subject, pullOptions);
    var consumerInfo = sub.getConsumerInfo();
    var consumerConfiguration = sub.getConsumerInfo().getConsumerConfiguration();

    log("Initial consumer info:\n\tinactive threshold: %sms\n\tack wait %sms\n\t"
            + "num pending:%s\n\tother options: %s",
        consumerConfiguration.getInactiveThreshold().toMillis(),
        consumerConfiguration.getAckWait().toMillis(), consumerInfo.getNumPending(),
        sub.getConsumerInfo());
    return sub;
  }

  public void log(String format, Object... args) {
    long time = (System.currentTimeMillis() - startTime);
    String timeStr = String.format("%3ds %3dms : ", time / 1000, time % 1000);
    System.out.printf(timeStr + format + "\n", args);
  }

  @SneakyThrows
  private void printMessageInfo(final Message message) {
    log("Received message #%d content: %s\tmessage metadata: %s", receivedMessageCount,
        new String(message.getData()), message.metaData());
  }

}
