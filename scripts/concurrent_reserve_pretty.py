#!/usr/bin/env python3
import concurrent.futures, grpc, booking_pb2, booking_pb2_grpc, time, sys

ADDR = "127.0.0.1:60051"   # must point to the current leader address
SEAT = "S9"                # seat to stress-test
NUM = 20

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

def try_reserve(i):
    ch = grpc.insecure_channel(ADDR)
    stub = booking_pb2_grpc.ClientAPIStub(ch)
    try:
        r = stub.ReserveSeat(booking_pb2.ReserveRequest(token="t", seat_id=SEAT, client_id=f"c{i}"), timeout=5)
        return i, r.code, r.msg
    except Exception as e:
        return i, -1, str(e)

if __name__ == "__main__":
    print(f"Running {NUM} concurrent attempts to reserve {SEAT} on {ADDR}")
    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM) as ex:
        results = list(ex.map(try_reserve, range(NUM)))
    took = time.time() - start
    success = [r for r in results if r[1] == 0]
    already = [r for r in results if r[1] == 3]
    errors = [r for r in results if r[1] not in (0,3)]
    for r in results:
        i, code, msg = r
        if code == 0:
            print(GREEN + f"[OK]    client-{i}: {code} {msg}" + RESET)
        elif code == 3:
            print(YELLOW + f"[REJ]   client-{i}: {code} {msg}" + RESET)
        else:
            print(RED +   f"[ERR]   client-{i}: {code} {msg}" + RESET)
    print("\nSummary:")
    print(f"  Successes: {len(success)}")
    print(f"  Rejections (already reserved): {len(already)}")
    print(f"  Other errors: {len(errors)}")
    print(f"  Total time: {took:.2f}s")
