package graph.chat;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public final class Schema {

  static final Label Message = Label.label("Message");
  static final Label Conversation = Label.label("Conversation");
  static final Label Topic = Label.label("Topic");
  static final Label Tag = Label.label("Tag");

  static final RelationshipType HAS_TAG = RelationshipType.withName("HAS_TAG");
  static final RelationshipType INITIATES = RelationshipType.withName("INITIATES");
  static final RelationshipType DECOHERES = RelationshipType.withName("DECOHERES");
  static final RelationshipType SNAPSHOT = RelationshipType.withName("SNAPSHOT");

  public static void setNonNullProp(Node n, String p, Object v) {
    if (v == null)
      return;
    n.setProperty(p, v);
  }

  public static void setNonNullProp(Relationship r, String p, Object v) {
    if (v == null)
      return;
    r.setProperty(p, v);
  }

}
