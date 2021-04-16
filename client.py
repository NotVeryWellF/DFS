import rpyc
import sys
import os
import logging

logging.basicConfig(level=logging.DEBUG)
PORT = 2131
host = "localhost"


def initialize(master):
    all_minions = master.initialize()
    for key, minion in all_minions.items():
        rpyc.connect(*minion).root.initialize_minion()


def read(master, file, save_name):
    file_table = master.read(file)
    if not file_table:
        logging.info("file not found")
        return

    for block in file_table:
        for host, port in block['block_addr']:
            try:
                con = rpyc.connect(host, port=port).root
                data = con.get(block['block_id'])
                if data:
                    # sys.stdout.write(data)
                    output_file = open(save_name, "w")
                    output_file.write(data)
                    break
            except Exception as e:
                continue
        else:
            logging.error("No blocks found. Possibly a corrupt file")


def write(master, source, dest):
    size = os.path.getsize(source)
    blocks = master.write(dest, size)
    with open(source) as f:
        for block in blocks:
            data = f.read(master.block_size)
            block_id = block['block_id']
            minions = block['block_addr']

            minion = minions[0]
            minions = minions[1:]
            host, port = minion

            con = rpyc.connect(host, port=port)
            con.root.put(block_id, data, minions)


def create(master, file):
    blocks = master.create(file)
    for block in blocks:
        data = " "
        block_id = block['block_id']
        minions = block['block_addr']

        minion = minions[0]
        minions = minions[1:]
        host, port = minion

        con = rpyc.connect(host, port=port)
        con.root.put(block_id, data, minions)


def create_non_empty(master, file, data):
    blocks = master.create(file)
    for block in blocks:
        block_id = block['block_id']
        minions = block['block_addr']

        minion = minions[0]
        minions = minions[1:]
        host, port = minion

        con = rpyc.connect(host, port=port)
        con.root.put(block_id, data, minions)


def delete(master, file):
    mapping = master.delete(file)
    for entry in mapping:
        block_id = entry['block_id']
        minions = entry['block_addr']
        for minion in minions:
            rpyc.connect(*minion).root.delete(block_id, minions)


def info(master, file):
    print(master.file_info(file))


def copy(master, file, new_name):
    file_table = master.read(file)
    if not file_table:
        logging.info("file not found")

    for block in file_table:
        for host, port in block['block_addr']:
            try:
                con = rpyc.connect(host, port=port).root
                data = con.get(block['block_id'])
                if data:
                    sys.stdout.write(data)
                    create_non_empty(master, new_name, data)
                    break
                else:
                    create(master, new_name)
            except Exception as e:
                continue
    pass


def move(master, file, path):
    arr = file.split('/')
    abs_path = path + arr[len(arr)-1]
    copy(master, file, abs_path)
    delete(master, file)
    pass


def make_dir(master, path):
    master.create_dir(path)
    pass


def read_dir(master, path):
    files = master.read_directory(path)
    for file in files:
        print(file)


def open_dir(master, path):
    pass


def delete_dir(master, path):
    master.delete_directory(path)
    pass


def main(args):
    con = rpyc.connect(host, port=PORT)
    master = con.root

    if args[0] == "initialize":
        initialize(master)
    if args[0] == "create":
        create(master, args[1])
    if args[0] == "delete":
        delete(master, args[1])
    if args[0] == "read":
        read(master, args[1], args[2])
    if args[0] == "write":
        write(master, args[1], args[2])
    if args[0] == "info":
        info(master, args[1])
    if args[0] == "copy":
        copy(master, args[1], args[2])
    if args[0] == "move":
        move(master, args[1], args[2])
    if args[0] == "make_dir":
        make_dir(master, args[1])
    if args[0] == "read_dir":
        read_dir(master, args[1])
    if args[0] == "open_dir":
        open_dir(master, args[1])
    if args[0] == "delete_dir":
        delete_dir(master, args[1])


if __name__ == "__main__":
    main(sys.argv[1:])