"""Course catalog server: an import-safe entry point."""
from concurrent.futures import ThreadPoolExecutor

import grpc
import catalog_pb2 as pb
import catalog_pb2_grpc as rpc


class Catalog(rpc.CourseServiceServicer):
    def GetCourse(self, request, context):
        if request.course_id != "CS101":
            context.abort(grpc.StatusCode.NOT_FOUND, "Unknown course")
        return pb.CourseInfo(title="Intro to Computing", credits=3)

    def ListCourses(self, request, context):
        # Demo ignores the query filter, as on slide 16.
        for title in ("Computing", "Networks"):
            if not context.is_active():
                return
            yield pb.CourseInfo(title=title, credits=3)


def main():
    server = grpc.server(ThreadPoolExecutor(max_workers=4))
    rpc.add_CourseServiceServicer_to_server(Catalog(), server)
    server.add_insecure_port("127.0.0.1:50051")  # Local demo, no TLS
    server.start()
    print("Course catalog listening on 127.0.0.1:50051", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=2).wait()


if __name__ == "__main__":
    main()
