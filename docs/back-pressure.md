---
id: back-pressure
title: Back Pressure
---

The system will apply back pressure to control the rate of request processing at different parts of the system.

* Waltz Client limits the number of outstanding append requests. When the number of requests reaches the limit subsequent requests are blocked.
* This is important to avoid exhausting the heap memory when traffic is very high. Inbound messages to Waltz server are throttled by the number of messages in the queue. We turn on/off auto-read from the network buffer according to the number of messages. When it is turned off, Netty’s event loop stop reading message from the network buffer, thus stop decoding message. The clients will eventually stop writing to the network channel since the server’s network buffer will get filled. 
