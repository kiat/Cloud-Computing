// CatalogServer.java
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;

public class CatalogServer {
  public static void main(String[] args) throws Exception {
    Server server = NettyServerBuilder
        .forAddress(new InetSocketAddress("127.0.0.1", 50051))
        .addService(new Catalog()).build().start();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      server.shutdown();
    }));
    server.awaitTermination();
  }
}
