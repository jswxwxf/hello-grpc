## Purpose
Learn GRPC by practice
## Modules
There are two modules:
<li>Server: GRPC server</li>
<li>Client: GRPC client</li>

## Naming Convention
<b>Packages</b>
<li><code>org.example.grpc.unary</code>: Request and Response unary communication API</li>
<li><code>org.example.grpc.stream</code>: Stream communication API</li>

<b>GRPC Server Class Name</b>
<li>GRPCServer : Class to start GRPC server</li>

<b>GRPC Client Class Name</b>
<li>XXXClient: Client to establish connection and communicate with GRPC server</li>

## IDL
<b>Location</b> : <code>src/main/proto</code>

## Learning Marks
### Bidirectional Stream API
1. By default, each channel being created map to one TCP connection(could be 0 or many), you can use <code>netstat</code> to verify
2. Channel can be reused by multiple stream, each stream on the same channel has different stream ID, you can use <code>wireshark</code> capturing HTTP2 package to verify
3. If channel is not shutdown, on linux system with default tcp keepalive setting, the TCP keepalive probe should be sent after 7200 seconds, you can use <code>wireshark</code> to see there is no TCP probe when no RPC call
4. Refer to doc via link <a href="https://github.com/grpc/proposal/blob/master/A8-client-side-keepalive.md">client side keepalive</a> to know why should use PING frame to keepalive
5. Both GRPC server and GRPC client has GRPC ping setting and the setting of the ping on both side should consider setting on another side. Below are the basic configuration on both side<br>
      Detailed info please ref to https://grpc.github.io/grpc-java/javadoc/<br>
      <code>keepAliveTime</code> <br>
      <code>keepAliveTimeout</code> <br>
      <code>permitKeepAliveWithoutCalls</code> <br>
      <code>permitKeepAliveTime</code> <br>
6. From GRPC best practice, client and server should use PING rather than native TPC keepalive to keep the GRPC connection alive  
7. Refer to GRPC official doc via link <a href="https://grpc.io/docs/guides/performance/">GRPC Performance</a>, get inspiration on how to improve performance
8. Consider pooling stub and channel to improve GRPC client side performance and avoid max concurrent stream issue. Ref to <a href="https://httpwg.org/specs/rfc7540.html#rfc.section.5.1.2">Stream Concurrency</a>
9. ManagedChannel has <code>getState</code> API to get its state


