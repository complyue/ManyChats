package graph.chat;

import org.neo4j.graphdb.*;

public class Chat {

  static final Label Message = Label.label("Message");
  static final RelationshipType DECOHERES = RelationshipType.withName("DECOHERES");

  public static void setNodeProp(Node n, String p, Object v) {
    if (v == null)
      return;
    n.setProperty(p, v);
  }

}
