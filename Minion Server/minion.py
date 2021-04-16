# Minion (Storage server) implementation for DFS

import rpyc
import os
import sys
import logging
from rpyc.utils.server import ThreadedServer

DATA_DIR = "/data_dir"
PORT = 8888

logging.basicConfig(level=logging.DEBUG)


def tuple_to_str(tup):
    return ":".join(map(lambda x: str(x), tup))


class Minion(rpyc.Service):

    # Initialize and clear Minion, deleteing all existing blocks:
    def exposed_initialize_minion(self):
        logging.debug("initializing")
        for root, dirs, files in os.walk(DATA_DIR):
            for f in files:
                os.unlink(os.path.join(root, f))

    def exposed_put(self, block_id, data, minions):
        logging.debug("put block: " + block_id)
        out_path = os.path.join(DATA_DIR, block_id)
        with open(out_path, 'w') as f:
            f.write(data)
        if len(minions) > 0:
            self.replicate_put(block_id, data, minions)

    def exposed_get(self, block_id):
        logging.debug("get block: " + block_id)
        block_addr = os.path.join(DATA_DIR, block_id)
        if not os.path.isfile(block_addr):
            logging.debug("block not found")
            return None
        with open(block_addr) as f:
            return f.read()

    def exposed_delete(self, block_id, minions):
        logging.debug("delete block: " + block_id)
        if os.path.exists(DATA_DIR + str(block_id)):
            os.remove(DATA_DIR + str(block_id))
        if len(minions) > 0:
            self.replicate_delete(block_id, minions)

    def replicate_put(self, block_id, data, minions):
        logging.debug("replicate_put block: " + block_id + str(minions))
        next_minion = minions[0]
        minions = minions[1:]
        host, port = next_minion
        rpyc.connect(host, port=port).root.put(block_id, data, minions)

    def replicate_delete(self, block_id, minions):
        logging.debug("replicate_delete block: " + block_id + str(minions))
        next_minion = minions[0]
        logging.debug("from minion: " + tuple_to_str(next_minion))
        minions = minions[1:]
        host, port = next_minion
        rpyc.connect(host, port=port).root.delete(block_id, minions)


if __name__ == "__main__":
    if (not os.path.isdir(DATA_DIR)):
        os.mkdir(DATA_DIR)

    logging.debug("starting minion")
    rpyc_logger = logging.getLogger('rpyc')
    rpyc_logger.setLevel(logging.WARN)
    t = ThreadedServer(Minion(), port=PORT, logger=rpyc_logger, protocol_config={
        'allow_public_attrs': True,
    })
    t.start()
