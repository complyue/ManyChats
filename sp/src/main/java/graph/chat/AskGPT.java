package graph.chat;

import static graph.chat.Chat.DECOHERES;
import static graph.chat.Chat.Message;
import static graph.chat.Chat.setNodeProp;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import graph.chat.OpenAI.ChatCompletion;
import graph.chat.OpenAI.Message;

public class AskGPT {

  @Context
  public Transaction tx;

  @Context
  public Log log;

  @Procedure(name = "chat.ask", mode = Mode.WRITE)
  @Description("Ask GPT about a user message, with its full conversation history included.")
  public Stream<AskResult> ask(
      @Name("quest") Node questMsg,
      @Name(value = "model", defaultValue = "gpt-3.5-turbo") String model) {
    assert model != null : "use null model?!";

    log.info("Asking [" + model + "] for quest: " + questMsg.getProperty("content"));

    try {
      List<Message> messages = new ArrayList<>();
      Result histResult = tx.execute(
          "MATCH (q:Message) WHERE elementId(q) = $questId"
              + " MATCH p = (h:Message)-[:DECOHERES*]->(q)"
              + " WHERE NOT (:Message)-[:DECOHERES]->(h)"
              + " RETURN p",
          Map.of("questId", questMsg.getElementId()));
      while (histResult.hasNext()) {
        Map<String, Object> row = histResult.next();
        Path path = (Path) row.get("p");
        for (Node n : path.reverseNodes()) {
          String role = (String) n.getProperty("role");
          String content = (String) n.getProperty("content");
          messages.add(new Message(role, content));
        }
        break;
      }
      assert messages.size() >= 1 : "At least a system prompt message should precede the selected quest message";

      ChatCompletion completion = OpenAI.completeChat(model, messages, log);
      if (completion == null || completion.choices() == null)
        return Stream.empty(); // assuming error has been logged

      // use an eager loop instead of lazy stream,
      // so errors can be caught and logged by us
      AskResult[] results = new AskResult[completion.choices().length];
      for (int i = 0; i < results.length; i++) {
        final var choice = completion.choices()[i];

        Node ans = tx.createNode(Message);

        ans.setProperty("msgid", MsgID.genUUID());

        setNodeProp(ans, "role", choice.message().role());
        setNodeProp(ans, "content", choice.message().content());

        setNodeProp(ans, "created",
            ZonedDateTime.ofInstant(Instant.ofEpochSecond(completion.created()), ZoneOffset.UTC));
        setNodeProp(ans, "model", completion.model());
        setNodeProp(ans, "system_fingerprint", completion.system_fingerprint());

        setNodeProp(ans, "usage.completion", completion.usage().completion_tokens());
        setNodeProp(ans, "usage.prompt", completion.usage().prompt_tokens());
        setNodeProp(ans, "usage.total", completion.usage().total_tokens());

        setNodeProp(ans, "finish_reason", choice.finish_reason());
        setNodeProp(ans, "choice.index", choice.index());

        log.info("Got answer #" + i + ": " + choice.message().content());

        Relationship r = questMsg.createRelationshipTo(ans, DECOHERES);

        results[i] = new AskResult(ans, r);
      }
      return Arrays.stream(results);

    } catch (Exception exc) {
      log.error("Error calling OpenAI", exc);
    }
    return Stream.empty();
  }

  public static record AskResult(Node ans, Relationship r) {
  }

}
