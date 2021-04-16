import rpyc
import uuid
import math
import random
import copy

from rpyc.utils.server import ThreadedServer

PORT = 2131
BLOCK_SIZE = 100
REPLICATION_FACTOR = 2
MINIONS = {"1": ("127.0.0.1", 8000),
           "2": ("127.0.0.1", 9000),}
ROOT = {'name': "", 'file_block': {}, 'parent': None, 'children': []}


class MasterService(rpyc.Service):
    """
    dir = {'name'; "name", 'file_block': {}, 'parent': parent_dir, 'children': []}
    file_block = {'file.txt': ["block1", "block2"]}
    block_minion = {"block1": [1,3]}
    minions = {"1": (127.0.0.1,8000), "3": (127.0.0.1,9000)}
    """
    dir = copy.deepcopy(ROOT)
    block_minion = {}
    minions = MINIONS

    block_size = BLOCK_SIZE
    replication_factor = REPLICATION_FACTOR

    # Initialization returns list of all minions
    def exposed_initialize(self):
        self.dir = copy.deepcopy(ROOT)
        return self.minions

    # Just for debug
    def exposed_dir_info(self):
        return self.dir

    # Get file, returns all blocks with all minions that contain such block
    def exposed_read(self, file):
        try:
            dir, file_name = self.find_dir(file)
        except FileNotFoundError:
            raise

        if file_name not in dir['file_block']:
            raise FileNotFoundError

        mapping = []
        # iterate over all of file's blocks
        for blk in dir['file_block'][file_name]:
            minion_addr = []
            # get all minions that contain that block
            for m_id in self.block_minion[blk]:
                minion_addr.append(self.minions[m_id])

            mapping.append({"block_id": blk, "block_addr": minion_addr})
        return mapping

    # Write file, allocates new blocks for the file, returns all allocated file's blocks
    def exposed_write(self, file, size):
        try:
            dir, file_name = self.find_dir(file)
        except FileNotFoundError:
            raise


        if file_name in dir['file_block']:
            raise FileExistsError

        dir['file_block'][file_name] = []
        num_blocks = int(math.ceil(float(size) / self.block_size))
        return self.alloc_blocks(file_name, num_blocks, dir)

    # Create new file, return all allocated file's blocks
    def exposed_create(self, file):
        try:
            dir, file_name = self.find_dir(file)
        except FileNotFoundError:
            raise

        if file_name in dir['file_block']:
            raise FileExistsError

        dir['file_block'][file_name] = []
        num_blocks = 1
        return self.alloc_blocks(file_name, num_blocks, dir)

    # Create directory
    def exposed_create_dir(self, path):
        path = path.split('/')
        curr_path = ""
        curr_dir = self.dir
        for p in path:
            curr_path += p + "/"
            try:
                curr_dir, n = self.find_dir(curr_path)
            except FileNotFoundError:
                curr_dir['children'].append({'name': p, 'file_block': {}, 'parent': curr_dir, 'children': []})
                curr_dir, n = self.find_dir(curr_path)

    # Delete file, returns all blocks with all minions that contain such block
    def exposed_delete(self, file):
        try:
            dir, file_name = self.find_dir(file)
        except FileNotFoundError:
            raise

        if file_name not in dir['file_block']:
            raise FileNotFoundError

        mapping = []
        # iterate over all of file's blocks
        for blk in dir['file_block'][file_name]:
            minion_addr = []
            # get all minions that contain that block
            for m_id in self.block_minion[blk]:
                minion_addr.append(self.minions[m_id])

            mapping.append({"block_id": blk, "block_addr": minion_addr})
        dir['file_block'].pop(file_name)
        return mapping

    # File info, return file's directory, and size in bytes
    def exposed_file_info(self, file):
        try:
            dir, file_name = self.find_dir(file)
        except FileNotFoundError:
            raise

        if file_name not in dir['file_block']:
            raise FileNotFoundError

        info = {'directory': dir['name'], 'size': BLOCK_SIZE*len(dir['file_block'][file_name])}
        return info

    # Read directory, returns list of all files in the directory
    def exposed_read_directory(self, dir_path):
        dir_path += "/"
        try:
            dir, f = self.find_dir(dir_path)
        except FileNotFoundError:
            raise
        return dir['file_block'].keys()

    # Is directory empty, returns True if directory is empty, False otherwise
    def exposed_is_dir_empty(self, dir_path):
        dir_path += "/"
        try:
            dir, f = self.find_dir(dir_path)
        except FileNotFoundError:
            raise

        if len(dir['file_block']) != 0 or len(dir['children']) != 0:
            return False
        return True

    # Delete directory
    def exposed_delete_directory(self, dir_path):
        dir_path += "/"
        try:
            dir, f = self.find_dir(dir_path)
        except FileNotFoundError:
            raise

        parent = dir['parent']
        parent['children'].remove(dir)

    # Allocate blocks for the file
    def alloc_blocks(self, file, num_blocks, dir):
        return_blocks = []
        for i in range(0, num_blocks):
            block_id = str(uuid.uuid1()) # generate a block
            minion_ids = random.sample(     # allocate REPLICATION_FACTOR number of minions
                list(self.minions.keys()), self.replication_factor)
            minion_addr = [self.minions[m] for m in minion_ids]
            self.block_minion[block_id] = minion_ids
            dir['file_block'][file].append(block_id)
            return_blocks.append({"block_id": block_id, "block_addr": minion_addr})
        return return_blocks

    # Find directory of the file
    def find_dir(self, file):
        path = file.split('/')
        if len(path) == 1:
            return self.dir, path[0]
        name = path[len(path) - 1]
        path = path[:len(path) - 1]
        dir = self.dir
        for p in path:
            if p == '':
                continue
            if len(dir['children']) == 0:
                raise FileNotFoundError
            for child in dir['children']:
                if child['name'] == p:
                    dir = child
                    break
                if child is dir['children'][len(dir['children']) - 1]:
                    raise FileNotFoundError
        return dir, name


if __name__ == "__main__":
    t = ThreadedServer(MasterService(), port=PORT, protocol_config={'allow_public_attrs': True,})
    t.start()