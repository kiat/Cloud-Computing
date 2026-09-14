// Catalog.java
import edu.utexas.cs.*;

import io.grpc.Status;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

public class Catalog extends CourseServiceGrpc.CourseServiceImplBase {
  @Override
  public void getCourse(CourseQuery query, StreamObserver<CourseInfo> out) {
    if (!"CS101".equals(query.getCourseId())) {
      out.onError(Status.NOT_FOUND.withDescription("Unknown course")
          .asRuntimeException());
      return;
    }
    CourseInfo course = CourseInfo.newBuilder()
        .setTitle("Intro to Computing").setCredits(3).build();
    out.onNext(course);
    out.onCompleted();
  }
  // Java equivalent of the Python streaming handler on slide 16.
  @Override
  public void listCourses(CourseQuery query, StreamObserver<CourseInfo> out) {
    for (String title : new String[] {"Computing", "Networks"}) {
      if (Context.current().isCancelled()) {
        return;
      }
      out.onNext(CourseInfo.newBuilder()
          .setTitle(title).setCredits(3).build());
    }
    out.onCompleted();
  }
}
