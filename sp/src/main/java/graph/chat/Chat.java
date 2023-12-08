package graph.chat;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.CRC32;

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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Chat {

  public static long signature(String... inputs) {
    CRC32 crc32 = new CRC32();
    for (final var input : inputs)
      crc32.update(input.getBytes(StandardCharsets.UTF_8));
    return crc32.getValue();
  }

  public static final ObjectMapper jsonMapper = new ObjectMapper();

  @Context
  public Transaction tx;

  @Context
  public Log log;

  @Procedure(name = "chat.snapshots", mode = Mode.READ)
  @Description("Return all conversations ever snapshot for the specified tip msg.")
  public Stream<SnapshotResult> snapshots(@Name("tipMsg") Node tipMsg,
      @Name(value = "limit", defaultValue = "1") long limit) {
    return tx.execute(
        "MATCH (m:Message) WHERE elementId(m) = $msgEID"
            + " MATCH (m)-[r:SNAPSHOT]->(cnvs:Conversation)"
            + " RETURN cnvs, r"
            + " ORDER BY r.timestamp DESC"
            + " LIMIT " + limit,
        Map.of("msgEID", tipMsg.getElementId())).stream().map(
            row -> new SnapshotResult(
                (Node) row.get("cnvs"),
                (Relationship) row.get("r")));
  }

  public static record SnapshotResult(Node cnvs, Relationship r) {
  }

  @Procedure(name = "chat.snapshot", mode = Mode.WRITE)
  @Description("Snapshot the full conversation history into a `(:Conversation)` node, for the specified tip msg.")
  public Stream<SnapshotResult> snapshot(@Name("tipMsg") Node tipMsg) {
    try {
      final List<Message> messages = new ArrayList<>();
      final StringWriter cypherWriter = new StringWriter();

      final Result cnvsResult = tx.execute(
          "MATCH (m:Message) WHERE elementId(m) = $msgEID"
              + " MATCH p = (h:Message)-[:DECOHERES*]->(m)"
              + " WHERE NOT (:Message)-[:DECOHERES]->(h)"
              + " CALL {"
              + "    WITH h"
              + "    OPTIONAL MATCH (t:Topic)-[:INITIATES]->(h)"
              + "    OPTIONAL MATCH (t)-[:HAS_TAG]->(tg:Tag)"
              + "    WITH t, COLLECT(DISTINCT tg) AS tags"
              + "    RETURN t, tags"
              + " }"
              + " WITH p, COLLECT({topic: t, tags: tags}) AS topicsWithTags"
              + " RETURN p, topicsWithTags",
          Map.of("msgEID", tipMsg.getElementId()));
      if (cnvsResult.hasNext()) {
        final Map<String, Object> row = cnvsResult.next();
        final Path path = (Path) row.get("p");
        prepareSnapshot(path.nodes(), (List<?>) row.get("topicsWithTags"),
            messages, cypherWriter);
        cnvsResult.close();
      } else { // special case: given the very 1st prompt msg
        cnvsResult.close();
        final Result p0Result = tx.execute(
            "MATCH (h:Message) WHERE elementId(h) = $msgEID"
                + " CALL {"
                + "    WITH h"
                + "    OPTIONAL MATCH (t:Topic)-[:INITIATES]->(h)"
                + "    OPTIONAL MATCH (t)-[:HAS_TAG]->(tg:Tag)"
                + "    WITH t, COLLECT(DISTINCT tg) AS tags"
                + "    RETURN t, tags"
                + " }"
                + " WITH COLLECT({topic: t, tags: tags}) AS topicsWithTags"
                + " RETURN topicsWithTags",
            Map.of("msgEID", tipMsg.getElementId()));
        assert p0Result.hasNext() : "bug?!";
        final Map<String, Object> row = p0Result.next();
        prepareSnapshot(Collections.singleton(tipMsg), (List<?>) row.get("topicsWithTags"),
            messages, cypherWriter);
        p0Result.close();
      }
      assert messages.size() >= 1 : "At least a system prompt message should precede the selected quest message";

      final String json = jsonMapper.writeValueAsString(messages);
      final String cypher = cypherWriter.toString();
      final Result ssResult = tx.execute(
          "MATCH (m:Message) WHERE elementId(m) = $msgEID"
              + " MERGE (m)-[r:SNAPSHOT]->(cnvs:Conversation{ msgid: $msgid, sig: $sig }) SET"
              + "   cnvs.timestamp = datetime({epochMillis: timestamp()}),"
              + "   cnvs.cypher = $cypher,"
              + "   cnvs.json = $json"
              + " RETURN cnvs, r",
          Map.of(
              "msgEID", tipMsg.getElementId(),
              "msgid", tipMsg.getProperty("msgid"),
              "sig", signature(json, cypher),
              "json", json,
              "cypher", cypher));
      assert ssResult.hasNext() : "MERGE can fail?!";
      final var ssRecord = ssResult.next();
      final var cnvs = (Node) ssRecord.get("cnvs");
      final var r = (Relationship) ssRecord.get("r");

      return Stream.of(new SnapshotResult(cnvs, r));

    } catch (Exception exc) {
      log.error("Error snapshotting the conversation", exc);
    }
    return Stream.empty();
  }

  protected void prepareSnapshot(Iterable<Node> nodes, List<?> topicsWithTags,
      List<Message> messages, StringWriter cypherWriter) throws IOException {
    for (final Node m : nodes) {
      final Message msg = Message.fromNode(m);
      messages.add(msg);
      final int msgNo = messages.size();

      cypherWriter.write("MERGE (m" + msgNo);
      cypherWriter.write(":Message {msgid: ");
      jsonMapper.writeValue(cypherWriter, (String) m.getProperty("msgid"));
      cypherWriter.write("}) SET m" + msgNo + ".timestamp=datetime(");
      jsonMapper.writeValue(cypherWriter, m.getProperty("timestamp").toString());
      cypherWriter.write("), m" + msgNo + ".role=");
      jsonMapper.writeValue(cypherWriter, msg.role);
      if (msg.content != null) {
        cypherWriter.write(", m" + msgNo + ".content=");
        jsonMapper.writeValue(cypherWriter, msg.content);
      }
      if (msg.tool_calls != null) {
        cypherWriter.write(", m" + msgNo + ".tool_calls=");
        final String[] tcs = new String[msg.tool_calls.length];
        for (int i = 0; i < tcs.length; i++)
          tcs[i] = jsonMapper.writeValueAsString(msg.tool_calls[i]);
        jsonMapper.writeValue(cypherWriter, tcs);
      }
      if (msg.tool_call_id != null) {
        cypherWriter.write(", m" + msgNo + ".tool_call_id=");
        jsonMapper.writeValue(cypherWriter, msg.tool_call_id);
      }
      cypherWriter.write("\n");
      if (msgNo > 1) {
        cypherWriter.write("MERGE (m" + (msgNo - 1));
        cypherWriter.write(")-[:DECOHERES]->(m" + msgNo);
        cypherWriter.write(")\n");
      }

      cypherWriter.write("\n");
    }
    assert messages.size() >= 1 : "at least self msg node should be there, bug?!";

    int topicNo = 1;
    for (final var entry : topicsWithTags) {
      final Map<?, ?> tr = (Map<?, ?>) entry;
      final Node topic = (Node) tr.get("topic");

      cypherWriter.write("MERGE (t" + topicNo);
      cypherWriter.write(":Topic { title: ");
      jsonMapper.writeValue(cypherWriter, (String) topic.getProperty("title"));
      cypherWriter.write(", summary: ");
      jsonMapper.writeValue(cypherWriter, (String) topic.getProperty("summary"));
      cypherWriter.write(", description: ");
      jsonMapper.writeValue(cypherWriter, (String) topic.getProperty("description"));
      cypherWriter.write(" })-[:INITIATES]->(m1)\n");

      final List<?> tags = (List<?>) tr.get("tags");
      for (final var t : tags) {
        final Node tag = (Node) t;
        cypherWriter.write("MERGE (t" + topicNo);
        cypherWriter.write(")-[:HAS_TAG]->(:Tag { name: ");
        jsonMapper.writeValue(cypherWriter, (String) tag.getProperty("name"));
        cypherWriter.write(", description: ");
        jsonMapper.writeValue(cypherWriter, (String) tag.getProperty("description"));
        cypherWriter.write(" })\n");
      }

      topicNo++;
      cypherWriter.write("\n");
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static record Message(
      String role, String content,
      ToolCall[] tool_calls, String tool_call_id) {

    public Message(String role, String content) {
      this(role, content, null, null);
    }

    public static Message toolResult(String content, String tool_call_id) {
      return new Message("tool", content, null, tool_call_id);
    }

    public void updateToNode(final Node m) throws IOException {
      m.setProperty("role", role);
      if (content != null) {
        m.setProperty("content", content);
      }
      if (tool_calls != null) {
        final String[] tca = new String[tool_calls.length];
        for (int i = 0; i < tca.length; i++) {
          tca[i] = jsonMapper.writeValueAsString(tool_calls[i]);
        }
        m.setProperty("tool_calls", tca);
      }
      if (tool_call_id != null) {
        m.setProperty("tool_call_id", tool_call_id);
      }
    }

    public static Message fromNode(final Node m) throws IOException {
      final String role = (String) m.getProperty("role");
      final String content = (String) m.getProperty("content", null);
      ToolCall[] tool_calls = null;
      final var tca = (Object[]) m.getProperty("tool_calls", null);
      if (tca != null) {
        tool_calls = new ToolCall[tca.length];
        for (int i = 0; i < tool_calls.length; i++) {
          tool_calls[i] = jsonMapper.readValue((String) tca[i], ToolCall.class);
        }
      }
      final String tool_call_id = (String) m.getProperty("tool_call_id", null);
      return new Message(role, content, tool_calls, tool_call_id);
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static record ToolCall(String id, String type,
      Function function) {
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static record Function(String name, String arguments) {
  }

}
