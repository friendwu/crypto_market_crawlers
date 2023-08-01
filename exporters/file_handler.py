import logging
import os
from watchdog.events import FileSystemEventHandler

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
        if not self.item_pipeline.is_valid_file(file_path):
            log.debug("invalid file: %s", file_path)
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
            for filename in filenames:
                self.__process_file(os.path.join(foldername, filename))
