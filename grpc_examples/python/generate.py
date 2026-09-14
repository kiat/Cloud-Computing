"""Generate Python message classes, type stubs, and gRPC bindings."""
from pathlib import Path
import importlib.util
import subprocess
import sys


def main():
    if importlib.util.find_spec("grpc_tools") is None:
        raise SystemExit(
            "Install tools first: python -m pip install -r requirements.txt")
    here = Path(__file__).resolve().parent
    root = here.parent
    subprocess.run([
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{root}", f"--python_out={here}", f"--pyi_out={here}",
        f"--grpc_python_out={here}", str(root / "catalog.proto"),
    ], check=True)
    print("Generated catalog_pb2.py, catalog_pb2.pyi, and catalog_pb2_grpc.py")


if __name__ == "__main__":
    main()
