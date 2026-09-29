#include <WiFi.h>
#include <PubSubClient.h>
#include <DHTesp.h>
#include <esp_system.h>
#include <math.h>
#include <sys/time.h>
#include <time.h>

namespace {

constexpr char WIFI_SSID[] = "Wokwi-GUEST";
constexpr char WIFI_PASSWORD[] = "";

// The Wokwi Private IoT Gateway exposes services on the host through this name.
constexpr char MQTT_SERVER[] = "host.wokwi.internal";
constexpr uint16_t MQTT_PORT = 1884;
constexpr char MQTT_TOPIC[] = "iot/lab2/esp32-01/telemetry";
constexpr char DEVICE_ID[] = "esp32-01";

constexpr char NTP_SERVER_PRIMARY[] = "pool.ntp.org";
constexpr char NTP_SERVER_SECONDARY[] = "time.google.com";
constexpr time_t MIN_VALID_EPOCH = 1704067200;  // 2024-01-01T00:00:00Z

constexpr uint8_t DHT_PIN = 15;
constexpr uint8_t TRIG_PIN = 5;
constexpr uint8_t ECHO_PIN = 18;
constexpr uint8_t LED_PIN = 2;

constexpr uint32_t SEND_INTERVAL_MS = 5000;
constexpr uint32_t INITIAL_RETRY_MS = 1000;
constexpr uint32_t MAX_RETRY_MS = 30000;
constexpr uint16_t MQTT_PACKET_BUFFER_SIZE = 512;

constexpr float MIN_TEMPERATURE_C = -40.0f;
constexpr float MAX_TEMPERATURE_C = 80.0f;
constexpr float MIN_HUMIDITY_PCT = 0.0f;
constexpr float MAX_HUMIDITY_PCT = 100.0f;
constexpr float MIN_DISTANCE_CM = 2.0f;
constexpr float MAX_DISTANCE_CM = 400.0f;

WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);
DHTesp dht;

char bootId[17] = {};
char mqttClientId[32] = {};
uint32_t sequenceNo = 0;
uint32_t lastSendMs = 0;
uint32_t nextWifiAttemptMs = 0;
uint32_t nextMqttAttemptMs = 0;
uint32_t wifiRetryDelayMs = INITIAL_RETRY_MS;
uint32_t mqttRetryDelayMs = INITIAL_RETRY_MS;
bool wifiWasConnected = false;
bool ntpConfigured = false;
bool ledState = false;

bool deadlineReached(uint32_t now, uint32_t deadline) {
  return static_cast<int32_t>(now - deadline) >= 0;
}

uint32_t nextRetryDelay(uint32_t currentDelay) {
  if (currentDelay >= MAX_RETRY_MS / 2) {
    return MAX_RETRY_MS;
  }
  return currentDelay * 2;
}

void initializeIdentifiers() {
  const uint64_t chipId = ESP.getEfuseMac();
  const uint32_t chipSuffix = static_cast<uint32_t>(chipId);
  const uint32_t bootNonce = esp_random();

  snprintf(bootId, sizeof(bootId), "%08lX%08lX",
           static_cast<unsigned long>(chipSuffix),
           static_cast<unsigned long>(bootNonce));
  snprintf(mqttClientId, sizeof(mqttClientId), "%s-%08lX", DEVICE_ID,
           static_cast<unsigned long>(chipSuffix));
}

void startNtpSynchronization() {
  // Offset values are zero so the Unix timestamp is always UTC.
  configTime(0, 0, NTP_SERVER_PRIMARY, NTP_SERVER_SECONDARY);
  ntpConfigured = true;
  Serial.println("NTP synchronization requested (UTC)");
}

bool getUtcEpochMs(uint64_t& epochMs) {
  timeval now{};
  if (gettimeofday(&now, nullptr) != 0 || now.tv_sec < MIN_VALID_EPOCH) {
    return false;
  }

  epochMs = static_cast<uint64_t>(now.tv_sec) * 1000ULL
          + static_cast<uint64_t>(now.tv_usec) / 1000ULL;
  return true;
}

void maintainWiFi(uint32_t nowMs) {
  if (WiFi.status() == WL_CONNECTED) {
    if (!wifiWasConnected) {
      wifiWasConnected = true;
      wifiRetryDelayMs = INITIAL_RETRY_MS;
      Serial.printf("WiFi connected, IP=%s, RSSI=%d dBm\n",
                    WiFi.localIP().toString().c_str(), WiFi.RSSI());
    }
    if (!ntpConfigured) {
      startNtpSynchronization();
    }
    return;
  }

  if (wifiWasConnected) {
    wifiWasConnected = false;
    ntpConfigured = false;
    mqttClient.disconnect();
    nextWifiAttemptMs = nowMs;
    Serial.println("WiFi disconnected");
  }

  if (!deadlineReached(nowMs, nextWifiAttemptMs)) {
    return;
  }

  Serial.printf("Connecting WiFi to %s (next retry in %lu ms)\n",
                WIFI_SSID, static_cast<unsigned long>(wifiRetryDelayMs));
  WiFi.disconnect();
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, 6);
  nextWifiAttemptMs = nowMs + wifiRetryDelayMs;
  wifiRetryDelayMs = nextRetryDelay(wifiRetryDelayMs);
}

void maintainMqtt(uint32_t nowMs) {
  if (WiFi.status() != WL_CONNECTED) {
    return;
  }

  if (mqttClient.connected()) {
    mqttClient.loop();
    return;
  }

  if (!deadlineReached(nowMs, nextMqttAttemptMs)) {
    return;
  }

  Serial.printf("Connecting MQTT to %s:%u...", MQTT_SERVER, MQTT_PORT);
  if (mqttClient.connect(mqttClientId)) {
    mqttRetryDelayMs = INITIAL_RETRY_MS;
    Serial.println(" connected");
    return;
  }

  Serial.printf(" failed (state=%d), next retry in %lu ms\n",
                mqttClient.state(),
                static_cast<unsigned long>(mqttRetryDelayMs));
  // mqttClient.connect() may block until its socket timeout; schedule from the
  // end of that attempt so the backoff is still honored when the broker is down.
  nextMqttAttemptMs = millis() + mqttRetryDelayMs;
  mqttRetryDelayMs = nextRetryDelay(mqttRetryDelayMs);
}

float readDistanceCm() {
  digitalWrite(TRIG_PIN, LOW);
  delayMicroseconds(2);
  digitalWrite(TRIG_PIN, HIGH);
  delayMicroseconds(10);
  digitalWrite(TRIG_PIN, LOW);

  const unsigned long durationUs = pulseIn(ECHO_PIN, HIGH, 30000UL);
  if (durationUs == 0) {
    return NAN;
  }
  return static_cast<float>(durationUs) * 0.0343f / 2.0f;
}

bool sensorValuesAreValid(const TempAndHumidity& dhtData, float distanceCm) {
  if (!isfinite(dhtData.temperature) ||
      dhtData.temperature < MIN_TEMPERATURE_C ||
      dhtData.temperature > MAX_TEMPERATURE_C) {
    Serial.printf("Invalid temperature: %.2f C\n", dhtData.temperature);
    return false;
  }

  if (!isfinite(dhtData.humidity) ||
      dhtData.humidity < MIN_HUMIDITY_PCT ||
      dhtData.humidity > MAX_HUMIDITY_PCT) {
    Serial.printf("Invalid humidity: %.2f %%\n", dhtData.humidity);
    return false;
  }

  if (!isfinite(distanceCm) ||
      distanceCm < MIN_DISTANCE_CM ||
      distanceCm > MAX_DISTANCE_CM) {
    Serial.printf("Invalid distance: %.2f cm\n", distanceCm);
    return false;
  }

  return true;
}

void publishTelemetry(uint32_t nowMs) {
  if (!mqttClient.connected()) {
    Serial.println("Telemetry skipped: MQTT is not connected");
    return;
  }

  uint64_t sentAtMs = 0;
  if (!getUtcEpochMs(sentAtMs)) {
    Serial.println("Telemetry skipped: waiting for a valid UTC NTP time");
    return;
  }

  const TempAndHumidity dhtData = dht.getTempAndHumidity();
  const float distanceCm = readDistanceCm();
  if (!sensorValuesAreValid(dhtData, distanceCm)) {
    Serial.println("Telemetry skipped: sensor validation failed");
    return;
  }

  const uint32_t messageSequence = ++sequenceNo;
  char payload[MQTT_PACKET_BUFFER_SIZE];
  const int payloadLength = snprintf(
      payload, sizeof(payload),
      "{\"schema_version\":1,\"device_id\":\"%s\",\"boot_id\":\"%s\","
      "\"sequence\":%lu,\"sent_at_ms\":%llu,\"temperature_c\":%.2f,"
      "\"humidity_pct\":%.2f,\"distance_cm\":%.2f,\"rssi_dbm\":%d,"
      "\"uptime_s\":%lu,\"led_on\":%s}",
      DEVICE_ID, bootId, static_cast<unsigned long>(messageSequence),
      static_cast<unsigned long long>(sentAtMs), dhtData.temperature,
      dhtData.humidity, distanceCm, WiFi.RSSI(),
      static_cast<unsigned long>(nowMs / 1000UL),
      ledState ? "true" : "false");

  if (payloadLength < 0 || static_cast<size_t>(payloadLength) >= sizeof(payload)) {
    Serial.printf("Telemetry not published: JSON buffer too small (required=%d)\n",
                  payloadLength);
    return;
  }

  const bool published = mqttClient.publish(MQTT_TOPIC, payload);
  Serial.printf("topic=%s publish=%s payload=%s\n", MQTT_TOPIC,
                published ? "OK" : "FAILED", payload);
}

}  // namespace

void setup() {
  Serial.begin(115200);

  pinMode(TRIG_PIN, OUTPUT);
  pinMode(ECHO_PIN, INPUT);
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(TRIG_PIN, LOW);
  digitalWrite(LED_PIN, LOW);

  dht.setup(DHT_PIN, DHTesp::DHT22);
  initializeIdentifiers();

  WiFi.mode(WIFI_STA);
  mqttClient.setServer(MQTT_SERVER, MQTT_PORT);
  if (!mqttClient.setBufferSize(MQTT_PACKET_BUFFER_SIZE)) {
    Serial.println("Warning: could not allocate the requested MQTT packet buffer");
  }

  Serial.printf("Lab 2 device ready: device_id=%s boot_id=%s\n", DEVICE_ID,
                bootId);
}

void loop() {
  const uint32_t nowMs = millis();
  maintainWiFi(nowMs);
  maintainMqtt(nowMs);

  if (nowMs - lastSendMs >= SEND_INTERVAL_MS) {
    lastSendMs = nowMs;
    publishTelemetry(nowMs);
  }
}
