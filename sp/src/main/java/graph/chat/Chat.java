package graph.chat;

import java.io.StringWriter;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import com.fasterxml.jackson.databind.ObjectMapper;

import graph.chat.OpenAI.Message;

public class Chat {

  static final Label Message = Label.label("Message");
  static final Label Conversation = Label.label("Conversation");

  static final RelationshipType DECOHERES = RelationshipType.withName("DECOHERES");
  static final RelationshipType SNAPSHOT = RelationshipType.withName("SNAPSHOT");

  public static void setNodeProp(Node n, String p, Object v) {
    if (v == null)
      return;
    n.setProperty(p, v);
  }

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
          final String role = (String) m.getProperty("role");
          final String content = (String) m.getProperty("content");

          messages.add(new Message(role, content));
          final int msgNo = messages.size();

          cypherWriter.write("MERGE (m" + msgNo);
          cypherWriter.write(":Message {msgid: ");
          jsonMapper.writeValue(cypherWriter, (String) m.getProperty("msgid"));
          cypherWriter.write("}) SET m" + msgNo + ".role=");
          jsonMapper.writeValue(cypherWriter, role);
          cypherWriter.write(", m" + msgNo + ".content=");
          jsonMapper.writeValue(cypherWriter, content);
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

      final Node cnvs = tx.createNode(Conversation);

      cnvs.setProperty("msgid", tipMsg.getProperty("msgid"));
      cnvs.setProperty("timestamp", ts);
      cnvs.setProperty("json", jsonMapper.writeValueAsString(messages));
      cnvs.setProperty("cypher", cypherWriter.toString());

      Relationship r = tipMsg.createRelationshipTo(cnvs, SNAPSHOT);
      r.setProperty("timestamp", ts);

      return Stream.of(new SnapshotResult(cnvs, r));

    } catch (Exception exc) {
      log.error("Error snapshotting the conversation", exc);
    }
    return Stream.empty();
  }

}
