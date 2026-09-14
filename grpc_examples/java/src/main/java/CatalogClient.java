// CatalogClient.java
import edu.utexas.cs.*;

import io.grpc.*;
import java.util.concurrent.TimeUnit;
public class CatalogClient {
  public static void main(String[] args) throws Exception {
    ManagedChannel channel = ManagedChannelBuilder
        .forAddress("127.0.0.1", 50051).usePlaintext().build();
    try {
      CourseInfo course = CourseServiceGrpc.newBlockingStub(channel)
          .withDeadlineAfter(2, TimeUnit.SECONDS)
          .getCourse(CourseQuery.newBuilder().setCourseId(args.length > 0 ? args[0] : "CS101").build());
      System.out.println(course.getTitle() + " " + course.getCredits());
    } catch (StatusRuntimeException error) {
      System.err.println(error.getStatus());
    } finally {
      channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
    }
  }
}
