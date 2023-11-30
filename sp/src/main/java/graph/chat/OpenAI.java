package graph.chat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.Log;

import com.fasterxml.jackson.databind.ObjectMapper;

import graph.chat.Chat.Message;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class OpenAI {

  public static final String CHAT_ENDPOINT = "https://api.openai.com/v1/chat/completions";

  public static final String OPENAI_API_KEY = System.getenv("OPENAI_API_KEY");
  public static final MediaType JSON_MIME = MediaType.parse("application/json");

  public static record Choice(String finish_reason, int index, Message message) {
  }

  public static record ChatCompletion(String id, Choice[] choices,
      int created, String model, String system_fingerprint,
      String object, Usage usage) {
    public ChatCompletion {
      if (!"chat.completion".equals(object))
        throw new IllegalArgumentException("object == " + object + " ?!");
    }
  }

  public static record Usage(int completion_tokens, int prompt_tokens,
      int total_tokens) {
  }

  public static ChatCompletion completeChat(
      String model, List<Message> msgs, Log log) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();

    OkHttpClient client = new OkHttpClient.Builder()
        .writeTimeout(1, TimeUnit.MINUTES)
        .readTimeout(5, TimeUnit.MINUTES)
        .build();

    String payload = objectMapper.writeValueAsString(Map.of(
        "model", model, "messages", msgs));
    log.debug("Calling [" + CHAT_ENDPOINT + "] with:\n" + payload);
    Request request = new Request.Builder()
        .url(CHAT_ENDPOINT)
        .addHeader("Authorization", "Bearer " + OPENAI_API_KEY)
        .addHeader("Content-Type", JSON_MIME.toString())
        .post(RequestBody.create(payload, JSON_MIME))
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        log.error("HTTP error from [" + CHAT_ENDPOINT + "] - "
            + response.code() + ": " + response.message()
            + "\n" + response.body().string());
        return null;
      }
      return objectMapper.readValue(response.body().charStream(),
          ChatCompletion.class);
    }
  }
}
