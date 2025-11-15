import concurrent.futures, grpc, booking_pb2, booking_pb2_grpc
ADDR = "127.0.0.1:60051"
def try_reserve(i):
    ch = grpc.insecure_channel(ADDR)
    stub = booking_pb2_grpc.ClientAPIStub(ch)
    try:
        r = stub.ReserveSeat(booking_pb2.ReserveRequest(token="t", seat_id="S9", client_id=f"c{i}"), timeout=5)
        return i, r.code, r.msg
    except Exception as e:
        return i, -1, str(e)
if __name__ == "__main__":
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        for res in ex.map(try_reserve, range(20)):
            print(res)
