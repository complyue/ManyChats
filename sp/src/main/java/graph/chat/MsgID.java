package graph.chat;

import java.util.UUID;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.UserFunction;

public class MsgID {

  public static String genUUID() {
    return UUID.randomUUID().toString();
  }

  @UserFunction(name = "chat.msgid")
  @Description("Generate a new UUID for a chat message.")
  public String newMsgID() {
    return genUUID();
  }

}
