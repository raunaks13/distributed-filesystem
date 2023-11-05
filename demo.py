write_queue = [(f"{i}w", 0) for i in range(1, 10)]
read_queue = [(f"{i}r", 0) for i in range(1, 5)]
op_queue = ["1w", "2w", "1r", "3w", "4w", "5w", "6w", "7w", "8w", "2r", "3r", "9w", "4r"]
read_queue[3] = ("4r", -3)

while True:
    if len(write_queue) > 0 and len(read_queue) == 0:
        op_queue.pop(0)
        write_req = write_queue.pop(0)
        print(write_req)

    elif len(write_queue) == 0 and len(read_queue) > 0:
        op_queue.pop(0)
        read_req = read_queue.pop(0)
        print(read_req)

    elif len(write_queue) > 0 and len(read_queue) > 0:
        if read_queue[0][1] >= 4:
            while True:
                if len(read_queue) > 0  and read_queue[0][1] >= 4:
                    read_req = read_queue.pop(0)
                    op_queue.remove(read_req[0])
                    for i in range(len(write_queue)):
                        write_queue[i] = (write_queue[i][0], write_queue[i][1]+1)
                    print(read_req)
                else:
                    break
        elif write_queue[0][1] >= 4:
            while True:
                if len(write_queue) > 0 and write_queue[0][1] >= 4:
                    write_req = write_queue.pop(0)
                    op_queue.remove(write_req[0])
                    for i in range(len(read_queue)):
                        read_queue[i] = (read_queue[i][0], read_queue[i][1]+1)
                    print(write_req)
                else:
                    break
        else:
            op = op_queue.pop(0)
            if 'w' in op:
                write_req = write_queue.pop(0)
                for i in range(len(read_queue)):
                    read_queue[i] = (read_queue[i][0], read_queue[i][1]+1)
                print(write_req)
            else:
                read_req = read_queue.pop(0)
                for i in range(len(write_queue)):
                    write_queue[i] = (write_queue[i][0], write_queue[i][1]+1)
                print(read_req)
    else:
        continue
