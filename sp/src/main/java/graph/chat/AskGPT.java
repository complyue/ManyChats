package graph.chat;

import static graph.chat.Schema.setNonNullProp;

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

import graph.chat.Chat.Message;
import graph.chat.OpenAI.ChatCompletion;

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

    try {
      log.debug("Asking [" + model + "] for quest: " + questMsg.getProperty("content"));

      List<Message> messages = new ArrayList<>();
      Result histResult = tx.execute(
          "MATCH (q:Message) WHERE elementId(q) = $msgEID"
              + " MATCH p = (h:Message)-[:DECOHERES*]->(q)"
              + " WHERE NOT (:Message)-[:DECOHERES]->(h)"
              + " RETURN p",
          Map.of("msgEID", questMsg.getElementId()));
      while (histResult.hasNext()) {
        Map<String, Object> row = histResult.next();
        Path path = (Path) row.get("p");
        for (Node m : path.nodes()) {
          messages.add(Message.fromNode(m));
        }
        break;
      }
      assert messages.size() >= 1 : "at least self msg node should be there, bug?!";

      ChatCompletion completion = OpenAI.completeChat(model, messages, log);
      if (completion == null || completion.choices() == null)
        return Stream.empty(); // assuming error has been logged

      // use an eager loop instead of lazy stream,
      // so errors can be caught and logged by us
      AskResult[] results = new AskResult[completion.choices().length];
      for (int i = 0; i < results.length; i++) {
        final var choice = completion.choices()[i];

        log.debug("Got answer #" + i + ": " + choice.message().content());

        Node ans = tx.createNode(Schema.Message);
        ans.setProperty("msgid", MsgID.genUUID());
        choice.message().updateToNode(ans);

        Relationship r = questMsg.createRelationshipTo(ans, Schema.DECOHERES);
        setNonNullProp(r, "created", // cypher: datetime({epochSeconds: created})
            ZonedDateTime.ofInstant(Instant.ofEpochSecond(completion.created()), ZoneOffset.UTC));
        setNonNullProp(r, "model", completion.model());
        setNonNullProp(r, "system_fingerprint", completion.system_fingerprint());
        setNonNullProp(r, "usage.completion", completion.usage().completion_tokens());
        setNonNullProp(r, "usage.prompt", completion.usage().prompt_tokens());
        setNonNullProp(r, "usage.total", completion.usage().total_tokens());
        setNonNullProp(r, "finish_reason", choice.finish_reason());
        setNonNullProp(r, "choice.index", choice.index());

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
