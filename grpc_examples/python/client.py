"""Unary client from slide 11. Optional argument: course ID (default CS101)."""
import sys

import grpc
import catalog_pb2 as pb
import catalog_pb2_grpc as rpc


def main():
    course_id = sys.argv[1] if len(sys.argv) > 1 else "CS101"
    with grpc.insecure_channel("127.0.0.1:50051") as channel:
        client = rpc.CourseServiceStub(channel)
        try:
            course = client.GetCourse(
                pb.CourseQuery(course_id=course_id), timeout=2.0)
            print(course.title, course.credits)
        except grpc.RpcError as error:
            print(error.code().name, error.details())
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
