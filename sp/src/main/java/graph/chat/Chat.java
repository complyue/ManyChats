package graph.chat;

import java.io.StringWriter;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Chat {

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
      ObjectMapper jsonMapper = new ObjectMapper();
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
      while (cnvsResult.hasNext()) {
        final Map<String, Object> row = cnvsResult.next();
        final Path path = (Path) row.get("p");
        for (final Node m : path.nodes()) {
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
            cypherWriter.write(", m" + msgNo + ".tool_calls=[");
            for (final var tc : msg.tool_calls) {
              cypherWriter.write("{ id: ");
              jsonMapper.writeValue(cypherWriter, tc.id);
              cypherWriter.write(", type: ");
              jsonMapper.writeValue(cypherWriter, tc.type);
              cypherWriter.write(", function: { name: ");
              jsonMapper.writeValue(cypherWriter, tc.function.get("name"));
              cypherWriter.write(", arguments: ");
              jsonMapper.writeValue(cypherWriter, tc.function.get("arguments"));
              cypherWriter.write(" } }");
            }
            cypherWriter.write("]");
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

        final var topicsWithTags = (List<?>) row.get("topicsWithTags");
        int topicNo = 1;
        for (final var entry : topicsWithTags) {
          final Map<?, ?> tr = (Map<?, ?>) entry;
          final Node topic = (Node) tr.get("topic");

          cypherWriter.write("MERGE (t" + topicNo);
          cypherWriter.write(":Topic { summary: ");
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

        break;
      }
      assert messages.size() >= 1 : "At least a system prompt message should precede the selected quest message";

      final var ts = ZonedDateTime.now(ZoneOffset.UTC);

      final Node cnvs = tx.createNode(Schema.Conversation);

      cnvs.setProperty("msgid", tipMsg.getProperty("msgid"));
      cnvs.setProperty("timestamp", ts);
      cnvs.setProperty("json", jsonMapper.writeValueAsString(messages));
      cnvs.setProperty("cypher", cypherWriter.toString());

      Relationship r = tipMsg.createRelationshipTo(cnvs, Schema.SNAPSHOT);
      r.setProperty("timestamp", ts);

      return Stream.of(new SnapshotResult(cnvs, r));

    } catch (Exception exc) {
      log.error("Error snapshotting the conversation", exc);
    }
    return Stream.empty();
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

    public void updateToNode(final Node m) {
      m.setProperty("role", role);
      if (content != null) {
        m.setProperty("content", content);
      }
      if (tool_calls != null) {
        m.setProperty("tool_calls",
            Arrays.stream(tool_calls).map(ToolCall::toMap)
                .collect(Collectors.toList()));
      }
      if (tool_call_id != null) {
        m.setProperty("tool_call_id", tool_call_id);
      }
    }

    public static Message fromNode(final Node m) {
      final String role = (String) m.getProperty("role");
      final String content = (String) m.getProperty("content", null);
      ToolCall[] tool_calls = null;
      final var tcs = (List<?>) m.getProperty("tool_calls", null);
      if (tcs != null) {
        tool_calls = tcs.stream().map(
            tcm -> ToolCall.fromMap((Map<?, ?>) tcm)).toArray(ToolCall[]::new);
      }
      final String tool_call_id = (String) m.getProperty("tool_call_id", null);
      return new Message(role, content, tool_calls, tool_call_id);

    }
  }

  public static record ToolCall(String id, String type,
      Map<String, Object> function) {

    public Map<String, Object> toMap() {
      return Map.of("id", id, "type", type, "function", function);
    }

    public static ToolCall fromMap(Map<?, ?> m) {
      final Map<?, ?> function = (Map<?, ?>) m.get("function");
      return new ToolCall(
          (String) m.get("id"),
          (String) m.get("type"),
          Map.of("name", (String) function.get("name"),
              "arguments", (String) function.get("arguments")));
    }

  }

}
