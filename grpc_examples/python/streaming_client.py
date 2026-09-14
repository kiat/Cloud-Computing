"""Complete client wrapper for the streaming example."""
import grpc
import catalog_pb2 as pb
import catalog_pb2_grpc as rpc


def main():
    with grpc.insecure_channel("127.0.0.1:50051") as channel:
        client = rpc.CourseServiceStub(channel)
        try:
            courses = client.ListCourses(pb.CourseQuery(), timeout=5.0)
            for course in courses:
                print(course.title)
        except grpc.RpcError as error:
            print(error.code().name, error.details())
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
