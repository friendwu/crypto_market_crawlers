import logging
import os
from watchdog.events import FileSystemEventHandler
import concurrent.futures
from loky import get_reusable_executor

log = logging.getLogger(__name__)


class DupeFilter(object):
    def __init__(self, path, debug=False):
        self.debug = debug
        self.keys = set()
        self.filepath = path
        if os.path.exists(self.filepath):
            with open(self.filepath, "r") as f:
                for l in f:
                    self.keys.add(l.strip())

        self.file = open(self.filepath, "a+")

    def duplicate(self, key):
        if key in self.keys:
            return True
        else:
            return False

    def set_duplicate(self, key):
        if self.duplicate(key):
            return
        else:
            self.keys.add(key)
            self.file.write(key + "\n")
            self.file.flush()


class FileHandler(FileSystemEventHandler):
    def __init__(self, dup_path, item_pipeline):
        super().__init__()

        self.item_pipeline = item_pipeline
        self.dup_filter = DupeFilter(dup_path)

    def __process_file(self, file_path):
        print(file_path)
        if not self.item_pipeline.is_valid_file(file_path):
            log.warn("invalid file: %s", file_path)
            return

        if self.dup_filter.duplicate(file_path):
            log.debug("duplicate file: %s", file_path)
            return

        log.info(f"Processing {file_path}")

        self.item_pipeline.extract_and_export_items(file_path)
        self.dup_filter.set_duplicate(file_path)

    def on_created(self, event):
        if event.is_directory or not self.item_pipeline.is_valid_filename(
            os.path.basename(event.src_path)
        ):
            log.warn("invalid created file %s", event.src_path)
            return

        self.__process_file(event.src_path)

    def process_existing_files(self, directory):
        for foldername, subfolders, filenames in os.walk(directory):
            # executor = get_reusable_executor(max_workers=10)
            filenames2 = []
            for filename in filenames:
                filenames2.append(os.path.join(foldername, filename))

            with concurrent.futures.ThreadPoolExecutor(20) as executor:
                executor.map(self.__process_file, filenames2)
            # results = executor.map(self.__process_file, filenames, chunksize=5)
            # print(len(set(results)))

            # executor.shutdown(wait=True)


            # for filename in filenames:
            #    self.__process_file(os.path.join(foldername, filename))

    def __process_files(self, file_paths):
        for file_path in file_paths:
            self.__process_file(file_path)


def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))
