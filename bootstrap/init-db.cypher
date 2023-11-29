CREATE CONSTRAINT IF NOT EXISTS
  FOR (m:Message) REQUIRE m.msgid IS UNIQUE
;

MERGE (topic0:Topic {
  summary: 'Many Chats',
  description: trim("
Chats as branches of conversational messages, naturally form a graph structure: 

After a human or ChatGPT sees a sequential conversation (consists of a sequence of messages from various roles), he/she/it could produce/complete a subsequent message, to lengthen the chat.

The randomness of ChatGPT, as well as human minds, behaves quite like quantum mechanisms at work - many possibilities co-exist before you write down the actual words (or ChatGPT decides the tokens to output), then once it's output, those possibilities collapse (i.e. decohere) into a definite reality.

With a graph database (i.e. neo4j as we are using), we can store more than 1 possibilities of those chats, though in the single classical-mechanical world we perceive.

Actually we can store as many as the capacity of underlying storage (disks as for today) allows. Let's pretend we are god seeing multiverse of chats, and mine usefulness out it.
") })
MERGE (brainstorm:Tag { name: 'brainstorm', description: 'Novel ideas born from storms in the mind' })
MERGE (topic0)-[:HAS_TAG]->(brainstorm)

MERGE (prompt0:Message {role: 'system'})
ON CREATE SET prompt0.msgid = chat.msgid(), prompt0.content = trim("
You are an expert in graph database (Neo4j). You will help the user work on db design & operating, by translating user's description into Cypher queries, as well as optimizing/revising queries from the user, to make them more performant & appropriate.
")
MERGE (topic0)-[:INITIATES]->(prompt0)

MERGE (quest0:Message {role: 'user'})
ON CREATE SET quest0.msgid = chat.msgid(), quest0.content = trim("
We have a graph of chat messages, a `(:Message)` `[:DECOHERES]` into many possible next `(:Message)`s, branching the graph toward different directions.

'Many Chats' is like the 'Many Worlds' interpretation of quantum mechanism.

How can I show my db schema to you, so you can give advices?
")
MERGE (prompt0)-[:DECOHERES]->(quest0)

;
