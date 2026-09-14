// Complete Java counterpart to the streaming client discussed on slide 16.
import edu.utexas.cs.*;
import io.grpc.*;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class CatalogStreamingClient {
  public static void main(String[] args) throws Exception {
    ManagedChannel channel = ManagedChannelBuilder
        .forAddress("127.0.0.1", 50051).usePlaintext().build();
    try {
      Iterator<CourseInfo> courses = CourseServiceGrpc.newBlockingStub(channel)
          .withDeadlineAfter(5, TimeUnit.SECONDS)
          .listCourses(CourseQuery.getDefaultInstance());
      while (courses.hasNext()) {
        System.out.println(courses.next().getTitle());
      }
    } catch (StatusRuntimeException error) {
      System.err.println(error.getStatus());
    } finally {
      channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
    }
  }
}
