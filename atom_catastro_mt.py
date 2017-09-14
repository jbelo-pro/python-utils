
"""
    Downloader data from Catastro (Spain) Atom Service.
    main() function must be changed to download the data from the three Atom services
    PATH_ROOT must be changed indicating the folder where you want to download the data
    Python version: 3.5
    author: Javier Belo
    email: javier.belo@outlook.com
    date: 2017-09-07
"""
from feedparser import parse as fp
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
import urllib.request
import urllib.parse
import os
import csv
import threading
import time

# Information of atom catastro
ATOM_DATA = {'addresses': {'url': "http://www.catastro.minhap.es/INSPIRE/Addresses/ES.SDGC.AD.atom.xml",
                               'folder': "addresses"},
                      'parcels': {'url': "http://www.catastro.minhap.es/INSPIRE/CadastralParcels/ES.SDGC.CP.atom.xml",
                                   'folder': "parcels"},
                      'buildings': {'url': "http://www.catastro.minhap.es/INSPIRE/buildings/ES.SDGC.BU.atom.xml",
                                     'folder': "buildings"}}

# Path to folder where to download the files
PATH_ROOT = "/put/here/the/path/to/the/folder"


class WorkingException(Exception):
    """
    Exception for catching problems during the process
    """
    pass


class CSVHandler:
    """
    Class for opening and writing csv error file in  thread safe mode
    """
    # ptah to folder of the csv
    path_folder = None
    # lock for thread safety
    lock = threading.Lock()

    def __init__(self):
        pass

    def open_csv(self, path_folder: str) -> None:
        """ Create new csv file based on the parameters
        :param path_folder:
        :return: None
        :raise: IOError
        """
        self.lock.acquire()
        try:
            self.path_folder = path_folder + '/errors.csv'
            print(self.path_folder)
            csv_log = open(self.path_folder, 'w')
        finally:
            if csv_log is not None:
                csv_log.close()
            self.lock.release()

    def write_csv(self, rows: list) -> None:
        """ Update the csv with the rows passed as argument
        :param rows: array containing the rows to include in the csv
        :return: None
        """
        self.lock.acquire()
        try:
            csv_log = open(self.path_folder, 'a')
            csv_writer = csv.writer(csv_log, delimiter=';')
            for row in rows:
                csv_writer.writerow(row)
        finally:
            if csv_log is not None:
                csv_log.close()
            self.lock.release()


class HardWorker:

    def __init__(self):
        pass

    @staticmethod
    def folder_setting(path_root: str, folder: str) -> str:
        """ Set up directory, in case directory exist raise an exception. If path is created successfully then
        the string of path directory is returned
        :param path_root: directory root
        :param folder: folder name
        :return: String directory path of folder
        :raise: OSError in case directory already exist, avoid overwriting
        """
        path_folder = path_root + "/" + folder
        os.makedirs(path_folder, exist_ok=False)
        return path_folder

    @staticmethod
    def entries_list(atom_url: str) -> list:
        """ Get the list with the entries
        :param atom_url: utrl of atom service
        :return: list with entries
        """
        atom = fp(atom_url)
        entries = atom.entries
        return entries

    @staticmethod
    def level_reader(atom_url: str, path_folder: str) -> list:
        """ Get the information to enter in next level. This method only check the first enclosure
        of each entry.
        :param atom_url: url of Catastro atom service
        :param path_folder: directory where Catastro data will be downloaded
        :return: list with errors in municipality downloading. Return the name of the folder, the path to
        the folder and url
        """
        # amount of entries at first level. In
        entries_data = []
        try:
            atom = fp(atom_url)
            entries = atom.entries
            for entry in entries:
                # changing the names
                m_pos = [pos for pos, char in enumerate(entry.title) if char == " "]
                folder_entry = entry.title[m_pos[1] + 1:]
                folder_entry = folder_entry.replace(' ', '_')
                path_folder_entry = path_folder + '/' + folder_entry
                sub_atom = entry.enclosures[0].href
                entries_data.append({'folder': folder_entry, 'path': path_folder_entry, 'reference': sub_atom})
        except Exception as e:
            print("Exception exception: " + str(e))
        return entries_data


    @staticmethod
    def extractor(atom_url: str, file_type: str = 'application/atom+xml') -> list:
        """ Get the enclosure list for a specific type
        :param atom_url: url of Catastro atom service
        :param file_type: enclosure type
        :return: list with errors in municipality downloading
        :raise:
        """
        entries_file = []
        try:
            atom = fp(atom_url)
            entries = atom.entries
            for entry in entries:
                if entry.enclosures[0].type == file_type:
                    entries_file.append(entry)
                # raise FileExistsError if the directory already exist. avoid overwriting
        except Exception as e:
            raise WorkingException(str(e))
        return entries_file

    @staticmethod
    def downloader(lvl_name: str, entries: list, path_folder: str, csv_handler: CSVHandler,
                   con_type: str = 'application/zip') -> None:
        """ Download files with included in an entry as enclosure data of each municipality
        :param lvl_name: name of the level for this list. In this case Catastro province
        :param entries: list of entries containing file for downloading
        :param path_folder: directory where Catastro data will be downloaded
        :param con_type: type of file to download
        :param csv_handler: instance of CSVHandler
        :return: list with errors in municipality downloading
        """
        # multidimensional array containing the errors generated during the downloading process
        failures = []
        # atom entries at second level. In Catastro means municipalities
        entries_downloaded = 0
        total_entries = len(entries)
        file_extension = None
        if con_type == 'application/zip':
            file_extension = '.zip'
        try:
            # raise FileExistsError if the directory already exist. avoid overwriting
            os.mkdir(path_folder)
            for entry_n in entries:
                file_name = entry_n.title.rsplit(' ', 1)[0]
                file_name = file_name.replace(' ', '_')
                path_file = path_folder + '/' + file_name + file_extension
                href = entry_n.enclosures[0].href
                url_enclosure = href.replace('http://', '')
                url_enclosure = urllib.parse.quote(url_enclosure)
                try:
                    urllib.request.urlretrieve('http://'+url_enclosure, path_file)
                    entries_downloaded += 1
                # TODO handle exceptions raised by urllib.request...
                except Exception as e:
                    failures.append([lvl_name, file_name, str(e)])

            print('Province: ' + lvl_name + '  Municipalities downloaded: ' + str(entries_downloaded) + '/' +
                  str(total_entries))
            # TODO change reporting by
            csv_handler.write_csv(failures)
        except IOError as e:
            print("Exception IOError: " + str(e))
        except FileExistsError as e:
            print("Exception FileExistError: " + str(e))
        except Exception as e:
            print("Exception exception: " + str(e))


class WorkerThread(Thread):
    folder = None
    list_files = None
    path = None

    def __init__(self):
        print("Hello world")
        Thread.__init__(self)

    def set_variables(self,folder, list_files, path):
        self.folder = folder
        self.list_files = list_files
        self.path = path

    def run(self):
        print("Thread is now running")
        HardWorker.downloader(self.folder, self.list_files, self.path)


def main():
    try:
        path_to_folder = HardWorker.folder_setting(PATH_ROOT,ATOM_DATA['addresses']['folder'])
        csv_handler = CSVHandler()
        csv_handler.open_csv(path_to_folder)
        list_entries = HardWorker.level_reader(ATOM_DATA['addresses']['url'], path_to_folder)
        time_1 = time.time()
        with ThreadPoolExecutor(max_workers=50) as executor:
            for entry in list_entries:
                list_files = HardWorker.extractor(entry['reference'])
                future = executor.submit(HardWorker.downloader, lvl_name=entry['folder'], entries=list_files,
                                         path_folder=entry['path'], csv_handler=csv_handler)

        time_2 = time.time()
        print("Total time spent: {}".format(time_2-time_1))
    except IOError as e:
        rint("Exception IOError: " + str(e))
    except OSError as e:
        print("Exception OSError: " + str(e))
    except WorkingException as e:
        print("Exception WorkingException: " + str(e))


if __name__ == '__main__':
    main()

